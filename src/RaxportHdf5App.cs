using System.Collections;
using System.Diagnostics;
using System.Globalization;
using ThermoFisher.CommonCore.BackgroundSubtraction;
using ThermoFisher.CommonCore.Data;
using ThermoFisher.CommonCore.Data.Business;
using ThermoFisher.CommonCore.Data.FilterEnums;
using ThermoFisher.CommonCore.Data.Interfaces;
using ThermoFisher.CommonCore.RawFileReader;

namespace Raxport
{
    internal static class Hdf5BufferDefaults
    {
        public const long PeaksPerFlushUnit = 10_000_000;
        public const int DefaultPeakFlushUnits = 2;
        public const long DefaultMaxBufferedPeaks = DefaultPeakFlushUnits * PeaksPerFlushUnit;
    }

    public class FTwriter
    {
        private static readonly string raxportVersion = "6.0";
        private string? rawFileName;
        private string? outPath;
        private IRawDataPlus? rawFile;
        private Hdf5BufferedWriter? hdf5Writer;
        private int firstScanNumber;
        private int lastScanNumber;

        public int topNprecursor = 15;
        public double intensityTreshold = 0.99;
        private readonly int[] chargesInConsideration = { 2, 3, 4 };
        public double mzTolerancePpm = 10.0;
        public long maxBufferedPeaks = Hdf5BufferDefaults.DefaultMaxBufferedPeaks;

        public IEnumerable<int>? MS1scanNumbers;
        public List<int[]>? scanNumberRanges;

        public bool ifMergeScans = false;
        private IScanAveragePlus? averager;

        private readonly List<Scan> MSnScansChunk = new();
        private readonly List<int> MSnScanNumbersChunk = new();
        private readonly List<IScanFilter> MSnScanFiltersChunk = new();
        private readonly List<IReaction> MSnScanReactionsChunk = new();
        private readonly List<double> MSnScansRTsChunk = new();

        public FTwriter(string fileName, string path, bool createFT)
        {
            _ = createFT;
            outPath = path;
            rawFileName = fileName;
            try
            {
                rawFile = RawFileReaderAdapter.FileFactory(rawFileName);
                if (!rawFile.IsOpen)
                {
                    Console.WriteLine("Cannot read " + rawFileName);
                    MainProgram.StopNoCloseWindow();
                    Environment.Exit(0);
                }
                rawFile.SelectInstrument(Device.MS, 1);
                firstScanNumber = rawFile.RunHeaderEx.FirstSpectrum;
                lastScanNumber = rawFile.RunHeaderEx.LastSpectrum;
                MS1scanNumbers = rawFile.GetFilteredScanEnumerator("ms");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString() + "\nCannot read " + rawFileName);
                MainProgram.StopNoCloseWindow();
                Environment.Exit(0);
            }
        }

        public FTwriter()
        {
        }

        public void InitScanNumberRanges(int splitRange)
        {
            int startScanNumber = firstScanNumber, endScanNumber = lastScanNumber;
            IEnumerator g = MS1scanNumbers!.GetEnumerator();
            scanNumberRanges = new List<int[]>();
            while (g.MoveNext())
            {
                if ((int)g.Current - startScanNumber > splitRange)
                {
                    endScanNumber = (int)g.Current;
                    scanNumberRanges.Add(new int[] { startScanNumber, endScanNumber });
                    startScanNumber = (int)g.Current;
                }
            }
            if (scanNumberRanges.Count == 0)
            {
                scanNumberRanges.Add(new int[] { startScanNumber, endScanNumber });
            }
            scanNumberRanges.Last()[1] = lastScanNumber;
        }

        public List<LabelPeak> BinarySearchMzRange(LabelPeak[] peaks, double start, double end)
        {
            List<LabelPeak> peaksInRange = new();
            if (peaks.Length == 0)
            {
                return peaksInRange;
            }

            double lowerBound = start - MzToleranceDa(start);
            double upperBound = end + MzToleranceDa(end);
            int low = 0;
            int high = peaks.Length - 1;
            while (low <= high)
            {
                int mid = (low + high) / 2;
                if (peaks[mid].Mass < lowerBound)
                {
                    low = mid + 1;
                }
                else
                {
                    high = mid - 1;
                }
            }
            int i = low;
            while (i < peaks.Length)
            {
                if (peaks[i].Mass > upperBound)
                {
                    break;
                }

                peaksInRange.Add(peaks[i]);
                i++;
            }
            return peaksInRange;
        }

        public List<LabelPeak> FindPrecursorPeaks(Scan mScan, double precusorMZ, double isolationWindow, int topN, double intensityRatio)
        {
            List<LabelPeak> precurosrPeaks = new();
            double neutronMass = 1.003355;
            bool foundIsotopicPeak = false;
            double totalIntensity = 0.000000001;
            double summedIntensity = 0;
            int[] tryISO = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
            List<LabelPeak> peaksInRange = BinarySearchMzRange(mScan.CentroidScan.GetLabelPeaks(),
                precusorMZ - isolationWindow / 2, precusorMZ + isolationWindow / 2);
            peaksInRange = peaksInRange.OrderByDescending(o => o.Intensity).ToList();
            foreach (LabelPeak peak in peaksInRange)
            {
                totalIntensity += peak.Intensity;
            }
            for (int i = 0; i < peaksInRange.Count; i++)
            {
                if (summedIntensity / totalIntensity > intensityRatio)
                {
                    break;
                }
                precurosrPeaks.Add(peaksInRange[i]);
                summedIntensity += peaksInRange[i].Intensity;
                if (precurosrPeaks.Count >= topN)
                {
                    break;
                }
                foreach (int iso in tryISO)
                {
                    foundIsotopicPeak = false;
                    for (int j = i + 1; j < peaksInRange.Count; j++)
                    {
                        if (CouldBeIsotopicPeak(peaksInRange[i], peaksInRange[j], iso, neutronMass, precusorMZ))
                        {
                            foundIsotopicPeak = true;
                            summedIntensity += peaksInRange[j].Intensity;
                            peaksInRange.RemoveAt(j);
                            j--;
                        }
                    }
                    if (!foundIsotopicPeak)
                    {
                        break;
                    }
                }
            }
            return precurosrPeaks;
        }

        private bool CouldBeIsotopicPeak(LabelPeak selectedPeak, LabelPeak candidatePeak, int isotopeOffset, double neutronMass, double isolationCenterMz)
        {
            double observedMzSpacing = Math.Abs(selectedPeak.Mass - candidatePeak.Mass);
            double mzTolerance = MzToleranceDa(isolationCenterMz);
            foreach (int charge in CandidateChargesForIsotopeCheck(selectedPeak, candidatePeak))
            {
                double expectedMzSpacing = isotopeOffset * neutronMass / charge;
                if (Math.Abs(observedMzSpacing - expectedMzSpacing) <= mzTolerance)
                {
                    return true;
                }
            }
            return false;
        }

        private IEnumerable<int> CandidateChargesForIsotopeCheck(LabelPeak selectedPeak, LabelPeak candidatePeak)
        {
            int selectedCharge = (int)selectedPeak.Charge;
            int candidateCharge = (int)candidatePeak.Charge;
            if (selectedCharge > 0 && candidateCharge > 0)
            {
                if (selectedCharge == candidateCharge)
                {
                    yield return selectedCharge;
                }
                yield break;
            }
            if (selectedCharge > 0)
            {
                yield return selectedCharge;
                yield break;
            }
            if (candidateCharge > 0)
            {
                yield return candidateCharge;
                yield break;
            }
            foreach (int charge in chargesInConsideration)
            {
                yield return charge;
            }
        }

        private double MzToleranceDa(double mz)
        {
            return Math.Abs(mz) * mzTolerancePpm / 1_000_000.0;
        }

        private bool IsWithinPpmTolerance(double observed, double expected)
        {
            return Math.Abs(observed - expected) <= MzToleranceDa(expected);
        }

        public void WriteFT1Scan(int mScanNumber, Scan mScan, IScanFilter mFilter, double mRT)
        {
            hdf5Writer!.AddScan(new Hdf5ScanRecord(
                mScanNumber,
                1,
                mRT,
                mScan.ScanStatistics.TIC,
                mFilter.ToString(),
                string.Empty,
                0,
                null,
                CollectPeaks(mScan)));
        }

        public void WriteFT2Scan(int mScanNumber, int mPrecusorScanNumber, IReaction mReaction, Scan mScan,
            Scan mPrecursorScan, IScanFilter mFilter, double mRT)
        {
            int chargeStateInt = GetTrailerChargeState(mScanNumber);
            List<Hdf5PeakRecord> parentPeaks = CollectPeaks(mPrecursorScan);
            List<Hdf5PeakRecord> evidencePeaks = PrecursorSelector.GetPrecursorEvidencePeaks(
                parentPeaks,
                mReaction.PrecursorMass,
                mReaction.IsolationWidth,
                mzTolerancePpm);
            List<Hdf5PeakRecord> precursorPeaks = PrecursorSelector.FindPrecursorPeaks(
                parentPeaks,
                mReaction.PrecursorMass,
                mReaction.IsolationWidth,
                topNprecursor,
                intensityTreshold,
                mzTolerancePpm);

            Hdf5ReactionRecord reaction = new(
                mReaction.PrecursorMass,
                mReaction.IsolationWidth,
                chargeStateInt,
                mReaction.CollisionEnergy,
                mReaction.CollisionEnergyValid,
                mReaction.ActivationType.ToString(),
                mReaction.MultipleActivation,
                mReaction.PrecursorRangeIsValid,
                mReaction.FirstPrecursorMass,
                mReaction.LastPrecursorMass,
                mReaction.IsolationWidthOffset,
                PrecursorSelector.ExpandPrecursorCandidates(
                    precursorPeaks,
                    evidencePeaks,
                    mReaction.PrecursorMass,
                    mReaction.IsolationWidth,
                    topNprecursor,
                    mzTolerancePpm,
                    chargeStateInt));

            hdf5Writer!.AddScan(new Hdf5ScanRecord(
                mScanNumber,
                GetMsOrder(mFilter),
                mRT,
                mScan.ScanStatistics.TIC,
                mFilter.ToString(),
                GetActivation(mFilter),
                mPrecusorScanNumber,
                reaction,
                CollectPeaks(mScan)));
        }

        public void writeScansChunk(int leftPrecursorScanNumber, int rightPrecursorScanNumber,
            Scan leftPrecursorScan, Scan rightPrecursorScan, IScanFilter rightFilter,
            double leftPrecursorRT, double rightPrecursorRT)
        {
            Scan currentPrecursorScan = rightPrecursorScan;
            int currentPrecursorScanNumber = rightPrecursorScanNumber;
            if (ifMergeScans)
            {
                rightPrecursorRT = (leftPrecursorRT + rightPrecursorRT) / 2.0;
                currentPrecursorScan = merge2Scans(leftPrecursorScanNumber, rightPrecursorScanNumber);
                currentPrecursorScanNumber = rightPrecursorScanNumber;
            }
            WriteFT1Scan(currentPrecursorScanNumber, currentPrecursorScan, rightFilter, rightPrecursorRT);
            for (int i = 0; i < MSnScansChunk.Count; i++)
            {
                if (!ifMergeScans)
                {
                    if (Math.Abs(MSnScansRTsChunk[i] - leftPrecursorRT)
                    < Math.Abs(MSnScansRTsChunk[i] - rightPrecursorRT))
                    {
                        currentPrecursorScan = leftPrecursorScan;
                        currentPrecursorScanNumber = leftPrecursorScanNumber;
                    }
                    else
                    {
                        currentPrecursorScan = rightPrecursorScan;
                        currentPrecursorScanNumber = rightPrecursorScanNumber;
                    }
                }
                WriteFT2Scan(MSnScanNumbersChunk[i], currentPrecursorScanNumber, MSnScanReactionsChunk[i],
                    MSnScansChunk[i], currentPrecursorScan, MSnScanFiltersChunk[i], MSnScansRTsChunk[i]);
            }
        }

        public Scan merge2Scans(int leftScanNumber, int rightScanNumber)
        {
            List<int> scanNumbers = new(new[] { leftScanNumber, rightScanNumber });
            var options = rawFile!.DefaultMassOptions();
            options.ToleranceUnits = ToleranceUnits.ppm;
            options.Tolerance = 10.0;
            Scan mergedScan = averager!.AverageScans(scanNumbers, options);
            return mergedScan;
        }

        public void Write()
        {
            string outputFile = Path.Combine(outPath!, Path.GetFileNameWithoutExtension(rawFileName) + ".h5");
            Stopwatch totalTimer = Stopwatch.StartNew();
            using Hdf5BufferedWriter writer = new(
                outputFile,
                rawFileName!,
                rawFile!.GetInstrumentData().Model,
                raxportVersion,
                maxBufferedPeaks);
            hdf5Writer = writer;

            LogConversionStart(outputFile);
            int currentScanNumber = firstScanNumber, precursorScanCount = 0, leftPrecursorScanNumber = 0, rightPrecursorScanNumber = 0;
            int parsedMS1Scans = 0, parsedMSnScans = 0;
            averager = ScanAveragerPlus.FromFile(rawFile);
            IScanEvent currentEvent;
            IScanFilter currentFilter;
            IReaction currentReaction;
            Scan currentScan = new(), leftPrecursorScan = new(), rightPrecursorScan = new();
            double currentRT = 0, leftPrecursorRT = 0, rightPrecursorRT = 0;
            while (currentScanNumber <= lastScanNumber)
            {
                currentRT = rawFile!.RetentionTimeFromScanNumber(currentScanNumber);
                currentScan = Scan.FromFile(rawFile, currentScanNumber);
                currentFilter = rawFile.GetFilterForScanNumber(currentScanNumber);
                if (currentFilter.MSOrder == MSOrderType.Ms)
                {
                    parsedMS1Scans++;
                    precursorScanCount++;
                    if (precursorScanCount == 1)
                    {
                        leftPrecursorScanNumber = currentScanNumber;
                        leftPrecursorScan = currentScan;
                        leftPrecursorRT = currentRT;
                        if (!ifMergeScans)
                        {
                            WriteFT1Scan(leftPrecursorScanNumber, leftPrecursorScan,
                                currentFilter, leftPrecursorRT);
                        }
                    }
                    else
                    {
                        rightPrecursorScanNumber = currentScanNumber;
                        rightPrecursorScan = currentScan;
                        rightPrecursorRT = currentRT;
                        writeScansChunk(leftPrecursorScanNumber, rightPrecursorScanNumber,
                            leftPrecursorScan, rightPrecursorScan, currentFilter,
                            leftPrecursorRT, rightPrecursorRT);
                        ClearMSnChunk();
                        leftPrecursorScanNumber = currentScanNumber;
                        leftPrecursorScan = currentScan;
                        leftPrecursorRT = currentRT;
                    }
                }
                else
                {
                    parsedMSnScans++;
                    currentEvent = rawFile.GetScanEventForScanNumber(currentScanNumber);
                    currentReaction = currentEvent.GetReaction(0);
                    MSnScansChunk.Add(currentScan);
                    MSnScanNumbersChunk.Add(currentScanNumber);
                    MSnScanFiltersChunk.Add(currentFilter);
                    MSnScanReactionsChunk.Add(currentReaction);
                    MSnScansRTsChunk.Add(currentRT);
                }
                currentScanNumber++;
            }

            for (int i = 0; i < MSnScansChunk.Count; i++)
            {
                WriteFT2Scan(MSnScanNumbersChunk[i], rightPrecursorScanNumber, MSnScanReactionsChunk[i],
                             MSnScansChunk[i], rightPrecursorScan,
                             MSnScanFiltersChunk[i], MSnScansRTsChunk[i]);
            }
            hdf5Writer = null;
            writer.Dispose();
            totalTimer.Stop();
            LogTimingSummary(writer, totalTimer.Elapsed, parsedMS1Scans, parsedMSnScans);
        }

        public void SplitedWrite(int threads)
        {
            _ = threads;
            Write();
        }

        private static List<Hdf5PeakRecord> CollectPeaks(Scan mScan)
        {
            List<Hdf5PeakRecord> peaks = new();
            if (mScan.HasCentroidStream)
            {
                foreach (LabelPeak peak in mScan.CentroidScan.GetLabelPeaks())
                {
                    peaks.Add(new Hdf5PeakRecord(
                        peak.Mass,
                        peak.Intensity,
                        peak.Resolution,
                        peak.Baseline,
                        peak.SignalToNoise,
                        (int)peak.Charge));
                }
            }
            else
            {
                for (int i = 0; i < mScan.SegmentedScan.Positions.Length; i++)
                {
                    peaks.Add(new Hdf5PeakRecord(
                        mScan.SegmentedScan.Positions[i],
                        mScan.SegmentedScan.Intensities[i],
                        0,
                        0,
                        0,
                        0));
                }
            }
            return peaks;
        }

        private int GetTrailerChargeState(int scanNumber)
        {
            var trailerLabels = rawFile!.GetTrailerExtraInformation(scanNumber);
            for (int i = 0; i < trailerLabels.Labels.Length; i++)
            {
                if (trailerLabels.Labels[i] == "Charge State:" &&
                    int.TryParse(trailerLabels.Values[i].Trim(), out int chargeState))
                {
                    return chargeState;
                }
            }
            return 0;
        }

        private static int GetMsOrder(IScanFilter filter)
        {
            string msOrder = filter.MSOrder.ToString();
            if (msOrder == "Ms")
            {
                return 1;
            }
            if (msOrder.StartsWith("Ms", StringComparison.OrdinalIgnoreCase) &&
                int.TryParse(msOrder[2..], out int order))
            {
                return order;
            }
            return 0;
        }

        private static string GetActivation(IScanFilter filter)
        {
            try
            {
                return filter.GetActivation(0).ToString();
            }
            catch
            {
                return string.Empty;
            }
        }

        private void ClearMSnChunk()
        {
            MSnScansChunk.Clear();
            MSnScanNumbersChunk.Clear();
            MSnScanFiltersChunk.Clear();
            MSnScanReactionsChunk.Clear();
            MSnScansRTsChunk.Clear();
        }

        private void LogConversionStart(string outputFile)
        {
            Console.WriteLine();
            Console.WriteLine("============================================================");
            Console.WriteLine("Raxport HDF5 conversion started");
            Console.WriteLine("------------------------------------------------------------");
            Console.WriteLine($"Input raw file      : {rawFileName}");
            Console.WriteLine($"Output HDF5 file    : {outputFile}");
            Console.WriteLine($"Scan range          : {firstScanNumber} - {lastScanNumber} ({lastScanNumber - firstScanNumber + 1:N0} scans)");
            Console.WriteLine($"Peak flush limit    : {maxBufferedPeaks:N0} peaks");
            Console.WriteLine($"Merge adjacent MS1  : {ifMergeScans}");
            Console.WriteLine($"Top precursor count : {topNprecursor:N0}");
            Console.WriteLine($"m/z tolerance       : {mzTolerancePpm:0.###} ppm");
            Console.WriteLine("============================================================");
        }

        private void LogTimingSummary(Hdf5BufferedWriter writer, TimeSpan totalElapsed, int parsedMS1Scans, int parsedMSnScans)
        {
            TimeSpan hdfElapsed = writer.HdfWriteElapsed;
            TimeSpan parseAndRamElapsed = totalElapsed - hdfElapsed;
            if (parseAndRamElapsed < TimeSpan.Zero)
            {
                parseAndRamElapsed = TimeSpan.Zero;
            }

            Console.WriteLine();
            Console.WriteLine("============================================================");
            Console.WriteLine("Raxport HDF5 conversion finished");
            Console.WriteLine("------------------------------------------------------------");
            Console.WriteLine($"Input raw file              : {rawFileName}");
            Console.WriteLine($"Raw scans parsed            : {parsedMS1Scans + parsedMSnScans:N0} (MS1={parsedMS1Scans:N0}, MSn={parsedMSnScans:N0})");
            Console.WriteLine($"HDF5 scan rows written      : {writer.TotalScans:N0}");
            Console.WriteLine($"HDF5 peak rows written      : {writer.TotalPeaks:N0}");
            Console.WriteLine($"HDF5 reaction rows written  : {writer.TotalReactions:N0}");
            Console.WriteLine($"HDF5 candidate rows written : {writer.TotalCandidates:N0}");
            Console.WriteLine($"HDF5 flush count            : {writer.FlushCount:N0}");
            Console.WriteLine("------------------------------------------------------------");
            Console.WriteLine($"Scan parse + RAM buffering  : {FormatElapsed(parseAndRamElapsed)} ({Percent(parseAndRamElapsed, totalElapsed):0.0}%)");
            Console.WriteLine($"HDF5 create/flush/close     : {FormatElapsed(hdfElapsed)} ({Percent(hdfElapsed, totalElapsed):0.0}%)");
            Console.WriteLine($"Total elapsed               : {FormatElapsed(totalElapsed)}");
            Console.WriteLine("============================================================");
        }

        private static string FormatElapsed(TimeSpan elapsed)
        {
            int hours = (int)elapsed.TotalHours;
            return $"{hours:00}:{elapsed.Minutes:00}:{elapsed.Seconds:00}.{elapsed.Milliseconds:000}";
        }

        private static double Percent(TimeSpan part, TimeSpan total)
        {
            if (total.TotalMilliseconds <= 0)
            {
                return 0;
            }
            return part.TotalMilliseconds / total.TotalMilliseconds * 100.0;
        }
    }

    class MainProgram
    {
        private static string[]? rawFiles;
        private static string rawFile = "";
        private static string inPath = "";
        private static string outPath = "";
        private static int threads = 6;
        private static int peakFlushUnits = Hdf5BufferDefaults.DefaultPeakFlushUnits;
        private static long maxBufferedPeaks = Hdf5BufferDefaults.DefaultMaxBufferedPeaks;
        private static bool ifMergeScans = false;
        private static int topN = 15;
        private static double mzTolerancePpm = 10.0;

        public static void StopNoCloseWindow()
        {
            // not close window when exit
        }

        private static bool ParseArgs(string[] args)
        {
            bool rValue = false;
            inPath = Directory.GetCurrentDirectory();
            outPath = inPath;
            string help = "Usage:\n" +
                "  Windows: .\\Raxport-win-x64.exe -i 'input path' -o 'output path' -j 6 -p 2\n" +
                "           .\\Raxport-win-x64.exe -f 'one raw/.d/.d.zip file name' -o 'output path' -p 2\n" +
                "  Linux:   ./Raxport-linux-x64 -i 'input path' -o 'output path' -j 6 -p 2\n" +
                "           ./Raxport-linux-x64 -f 'one raw/.d/.d.zip file name' -o 'output path' -p 2\n" +
                "  macOS:   ./Raxport-osx-x64 -i 'input path' -o 'output path' -j 6 -p 2\n" +
                "           ./Raxport-osx-arm64 -i 'input path' -o 'output path' -j 6 -p 2\n" +
                "\n" +
                "Options:\n" +
                "  -i PATH                 Input directory containing .raw, .d, or .d.zip files. Default: current directory.\n" +
                "  -f FILE                 Convert one RAW, .d directory, or .d.zip archive instead of scanning the input directory.\n" +
                "  -o PATH                 Output directory. Default: input/current directory.\n" +
                "  -j N                    Maximum child Raxport processes for multiple files. Default: 6.\n" +
                "  -p N                    Peak flush units; one unit is 10,000,000 peak rows. Default: 2.\n" +
                "  -n N                    Maximum precursor candidates stored for each MSn scan. Default: 15.\n" +
                "  --mz-tolerance-ppm PPM  Precursor m/z matching tolerance. Default: 10.\n" +
                "  -m                      Merge adjacent MS1 scans.\n" +
                "  -h                      Show this help.\n";
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == "-h")
                {
                    Console.WriteLine(help);
                    Environment.Exit(0);
                }
                if ((i + 1) > (args.Length - 1))
                {
                    break;
                }
                else if (args[i] == "-i")
                {
                    inPath = args[++i];
                }
                else if (args[i] == "-f")
                {
                    rawFile = args[++i];
                }
                else if (args[i] == "-o")
                {
                    outPath = args[++i];
                }
                else if (args[i] == "-j")
                {
                    _ = int.TryParse(args[++i], out threads);
                }
                else if (args[i] == "-p")
                {
                    if (int.TryParse(args[++i], out int parsedPeakFlushUnits) && parsedPeakFlushUnits > 0)
                    {
                        peakFlushUnits = parsedPeakFlushUnits;
                        maxBufferedPeaks = peakFlushUnits * Hdf5BufferDefaults.PeaksPerFlushUnit;
                    }
                }
                else if (args[i] == "-m")
                {
                    ifMergeScans = true;
                }
                else if (args[i] == "-n")
                {
                    _ = int.TryParse(args[++i], out topN);
                }
                else if (args[i] == "--mz-tolerance-ppm")
                {
                    if (double.TryParse(args[++i], out double parsedMzTolerancePpm) && parsedMzTolerancePpm >= 0)
                    {
                        mzTolerancePpm = parsedMzTolerancePpm;
                    }
                }
            }
            try
            {
                if (rawFile != "")
                {
                    rawFiles = new string[1];
                    rawFiles[0] = rawFile;
                }
                else
                {
                    rawFiles = Directory.GetFiles(inPath, "*.raw")
                        .Concat(Directory.GetFiles(inPath, "*.d.zip"))
                        .Concat(Directory.GetDirectories(inPath, "*.d"))
                        .ToArray();
                }

                if (rawFiles.Length > 0)
                {
                    if (!Directory.Exists(outPath))
                    {
                        Directory.CreateDirectory(outPath);
                    }
                    if (Directory.Exists(outPath))
                    {
                        rValue = true;
                    }
                }
                else
                {
                    Console.WriteLine("Args parsing failed! no .raw, .d, or .d.zip input was found!");
                    Console.WriteLine(help);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Args parsing failed!");
                Console.WriteLine(ex.ToString());
            }
            return rValue;
        }

        private static void Main(string[] args)
        {
            if (ParseArgs(args))
            {
                if (rawFiles!.Length > 1 && threads > 1)
                {
                    ConvertRawFilesInChildProcesses();
                }
                else
                {
                    foreach (string file in rawFiles)
                    {
                        ConvertRawFileInCurrentProcess(file);
                    }
                }
                Console.WriteLine("All convert finished");
            }
            StopNoCloseWindow();
        }

        private static void ConvertRawFileInCurrentProcess(string file)
        {
            if (IsBrukerInput(file))
            {
                BrukerHdf5Converter brukerWriter = new(file, outPath, topN, mzTolerancePpm, maxBufferedPeaks);
                brukerWriter.Write();
                return;
            }

            FTwriter writer = new(file, outPath, true);
            writer.ifMergeScans = ifMergeScans;
            writer.topNprecursor = topN;
            writer.maxBufferedPeaks = maxBufferedPeaks;
            writer.mzTolerancePpm = mzTolerancePpm;
            writer.Write();
        }

        private static bool IsBrukerInput(string file)
        {
            if (Directory.Exists(file))
            {
                return file.EndsWith(".d", StringComparison.OrdinalIgnoreCase);
            }

            return file.EndsWith(".d.zip", StringComparison.OrdinalIgnoreCase);
        }

        private static void ConvertRawFilesInChildProcesses()
        {
            string executablePath = GetExecutablePath();
            Console.WriteLine($"Launching up to {threads:N0} Raxport child processes for {rawFiles!.Length:N0} raw files");

            int failureCount = 0;
            Parallel.ForEach(rawFiles, new ParallelOptions { MaxDegreeOfParallelism = threads }, file =>
            {
                int exitCode = RunChildProcess(executablePath, file);
                if (exitCode != 0)
                {
                    Interlocked.Increment(ref failureCount);
                    Console.Error.WriteLine($"{file}: child Raxport process failed with exit code {exitCode}");
                }
            });

            if (failureCount > 0)
            {
                throw new InvalidOperationException($"{failureCount} child Raxport process(es) failed.");
            }
        }

        private static int RunChildProcess(string executablePath, string file)
        {
            ProcessStartInfo startInfo = new()
            {
                FileName = executablePath,
                UseShellExecute = false,
            };
            startInfo.ArgumentList.Add("-f");
            startInfo.ArgumentList.Add(file);
            startInfo.ArgumentList.Add("-o");
            startInfo.ArgumentList.Add(outPath);
            startInfo.ArgumentList.Add("-j");
            startInfo.ArgumentList.Add("1");
            startInfo.ArgumentList.Add("-p");
            startInfo.ArgumentList.Add(peakFlushUnits.ToString(CultureInfo.InvariantCulture));
            startInfo.ArgumentList.Add("-n");
            startInfo.ArgumentList.Add(topN.ToString(CultureInfo.InvariantCulture));
            startInfo.ArgumentList.Add("--mz-tolerance-ppm");
            startInfo.ArgumentList.Add(mzTolerancePpm.ToString("R", CultureInfo.InvariantCulture));
            if (ifMergeScans)
            {
                startInfo.ArgumentList.Add("-m");
            }

            using Process? process = Process.Start(startInfo);
            if (process is null)
            {
                throw new InvalidOperationException($"Unable to start child Raxport process for {file}");
            }
            process.WaitForExit();
            return process.ExitCode;
        }

        private static string GetExecutablePath()
        {
            string? processPath = Environment.ProcessPath;
            if (!string.IsNullOrWhiteSpace(processPath) &&
                File.Exists(processPath) &&
                !string.Equals(Path.GetFileName(processPath), "dotnet", StringComparison.OrdinalIgnoreCase))
            {
                return processPath;
            }

            string commandPath = Environment.GetCommandLineArgs()[0];
            if (!string.IsNullOrWhiteSpace(commandPath) && File.Exists(commandPath))
            {
                return Path.GetFullPath(commandPath);
            }

            throw new InvalidOperationException("Unable to locate the Raxport executable for child-process conversion. Publish or run the Raxport executable directly.");
        }
    }
}
