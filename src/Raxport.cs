using ThermoFisher.CommonCore.Data;
using ThermoFisher.CommonCore.Data.Business;
using ThermoFisher.CommonCore.Data.Interfaces;
using ThermoFisher.CommonCore.RawFileReader;
using ThermoFisher.CommonCore.BackgroundSubtraction;
using System.Collections;

namespace Raxport
{
    public class FTwriter
    {
        static private readonly string raxportVersion = "5";
        // file name with full path of raw file
        private string? rawFileName;
        private readonly string? outPath;
        private IRawDataPlus? rawFile;
        private StringWriter? tmpStringChunk = new();
        private StreamWriter? FT1writer;
        private StreamWriter? FT2writer;
        private int firstScanNumber;
        private int lastScanNumber;

        // for precursor select
        public int topNprecursor = 15;
        public double intensityTreshold = 0.99;
        int[] chargesInConsideration = { 2, 3, 4 };
        public double mzTolerance = 0.01;

        // for splited FT2 files
        public IEnumerable<int>? MS1scanNumbers;
        public List<int[]>? scanNumberRanges;

        // for merge 2 adjacent MS1 scans
        public bool ifMergeScans = false;
        private IScanAveragePlus? averager;

        // for writing MS2 scans in chunk between two MS1 scans
        private List<Scan> MS2scansChunk = new();
        private List<int> MS2scanNumbersChunk = new();
        private List<IScanFilter> MS2scanFiltersChunk = new();
        private List<IReaction> MS2scanReactionsChunk = new();
        private List<double> MS2ScansRTsChunk = new();
        public FTwriter(string fileName, string path, bool createFT)
        {
            outPath = path;
            rawFileName = fileName;
            try
            {
                // Business.RawFileReaderFactory not work in stand alone package
                // Error: System.ArgumentException: Invalid path
                // rawFile = RawFileReaderFactory.ReadFile(rawFileName);
                // use CommonCore.RawFileReader directly 
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
                Console.WriteLine(rawFileName + " has " + lastScanNumber + " scans\n");
                MS1scanNumbers = rawFile.GetFilteredScanEnumerator("ms");
                if (createFT)
                {
                    FT1writer = new StreamWriter(outPath + "/" + Path.GetFileNameWithoutExtension(rawFileName) + ".FT1");
                    FT2writer = new StreamWriter(outPath + "/" + Path.GetFileNameWithoutExtension(rawFileName) + ".FT2");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString() + "\nCannot read " + rawFileName);
                MainProgram.StopNoCloseWindow();
                Environment.Exit(0);
            }
        }
        // for parralle writing
        public FTwriter()
        {
        }
        // split scanNumbers by begining with MS1 scan for splited FT files
        // each scan ranges begin with a MS1 scan and end with first MS1 of next scans range 
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
                scanNumberRanges.Add(new int[] { startScanNumber, endScanNumber });
            scanNumberRanges.Last()[1] = lastScanNumber;
        }
        public List<LabelPeak> BinarySearchMzRange(LabelPeak[] peaks, double start, double end)
        {
            List<LabelPeak> peaksInRange = new();
            int low = 0;
            int high = peaks.Length - 1;
            int mid = 0;
            double diff;
            while (low <= high)
            {
                mid = (low + high) / 2;
                diff = Math.Abs(peaks[mid].Mass - start);
                if (diff <= mzTolerance) // Found a match within the tolerance range
                {
                    break;
                }
                if (peaks[mid].Mass < start) // Search the right half
                {
                    low = mid + 1;
                }
                else // Search the left half
                {
                    high = mid - 1;
                }
            }
            int i = mid;
            if (peaks[i].Mass < start)
                i++;
            while (i < peaks.Length)
            {
                if (peaks[i].Mass > end)
                    break;
                else
                {
                    if (peaks[i].Charge > 0)
                        peaksInRange.Add(peaks[i]);
                    // guess charge state
                    else
                    {
                        foreach (int charge in chargesInConsideration)
                        {
                            // deep copy peak
                            LabelPeak mPeak = new()
                            {
                                Charge = charge,
                                Mass = peaks[i].Mass,
                                Intensity = peaks[i].Intensity
                            };
                            peaksInRange.Add(mPeak);
                        }
                    }
                }
                i++;
            }
            return (peaksInRange);
        }
        // find prescursor peaks with topN intensity in a range 
        public List<LabelPeak> FindPrecursorPeaks(Scan mScan, double precusorMZ, double isolationWindow, int topN, double intensityRatio)
        {
            List<LabelPeak> precurosrPeaks = new List<LabelPeak>();
            double neutronMass = 1.003355;
            bool foundIsotopicPeak = false;
            double totalIntensity = 0.000000001;
            double summedIntensity = 0;
            int[] tryISO = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
            List<LabelPeak> peaksInRange = BinarySearchMzRange(mScan.CentroidScan.GetLabelPeaks(),
                precusorMZ - isolationWindow / 2, precusorMZ + isolationWindow / 2);
            peaksInRange = peaksInRange.OrderByDescending(o => o.Intensity).ToList();
            foreach (LabelPeak peak in peaksInRange) { totalIntensity += peak.Intensity; }
            for (int i = 0; i < peaksInRange.Count; i++)
            {
                // stop while intensity ratio threshold of isotopic peaks reached
                if (summedIntensity / totalIntensity > intensityRatio)
                    break;
                precurosrPeaks.Add(peaksInRange[i]);
                summedIntensity += peaksInRange[i].Intensity;
                if (precurosrPeaks.Count >= topN)
                    break;
                // remove isotopic peaks
                foreach (int iso in tryISO)
                {
                    foundIsotopicPeak = false;
                    for (int j = i + 1; j < peaksInRange.Count; j++)
                    {
                        if (peaksInRange[i].Charge == peaksInRange[j].Charge)
                        {
                            if (Math.Abs((Math.Abs(peaksInRange[i].Mass - peaksInRange[j].Mass) * peaksInRange[i].Charge
                                - iso * neutronMass)) < mzTolerance)
                            {
                                foundIsotopicPeak = true;
                                summedIntensity += peaksInRange[j].Intensity;
                                peaksInRange.RemoveAt(j);
                                j--;
                            }
                        }
                    }
                    if (!foundIsotopicPeak) break;
                }
            }
            return (precurosrPeaks);
        }
        public void WriteHeader()
        {
            FT1writer!.WriteLine("H\tExtractor\t" + "Raxport V" + raxportVersion);
            FT1writer.WriteLine("H\tm/z\tIntensity\tResolution\tBaseline\tNoise\tCharge");
            FT1writer.WriteLine("H\tInstrument Model\t" + rawFile!.GetInstrumentData().Model);
            FT2writer!.WriteLine("H\tExtractor\t" + "Raxport V" + raxportVersion);
            FT2writer.WriteLine("H\tm/z\tIntensity\tResolution\tBaseline\tNoise\tCharge");
            FT2writer.WriteLine("H\tInstrument Model\t" + rawFile.GetInstrumentData().Model);
        }
        public void WritePeak(StringWriter writer, Scan mScan)
        {
            if (mScan.HasCentroidStream)
            {
                LabelPeak[] peaks = mScan.CentroidScan.GetLabelPeaks();
                foreach (LabelPeak peak in peaks)
                {
                    // SignalToNoise is peak.Noise in old V3 version
                    writer.WriteLine("{0:F6}\t{1:F2}\t{2:F0}\t{3:F2}\t{4:F2}\t{5:F0}",
                        peak.Mass, peak.Intensity, peak.Resolution, peak.Baseline, peak.SignalToNoise, peak.Charge);
                }
            }
            else
            {
                for (int i = 0; i < mScan.SegmentedScan.Positions.Length; i++)
                {
                    writer.WriteLine("{0:F6}\t{1:F2}", mScan.SegmentedScan.Positions[i], mScan.SegmentedScan.Intensities[i]);
                }
            }
        }
        public void WriteFT1Scan(int mScanNumber, Scan mScan, IScanFilter mFilter, double mRT)
        {
            tmpStringChunk!.WriteLine("S\t{0:D}\t{1:F2}", mScanNumber, mScan.ScanStatistics.TIC);
            tmpStringChunk.WriteLine("I\tRetentionTime\t{0:F6}", mRT);
            tmpStringChunk.WriteLine("I\tScanType\tMs1");
            tmpStringChunk.WriteLine("I\tScanFilter\t" + mFilter!.ToString());
            WritePeak(tmpStringChunk, mScan);
            // Write function won't change line
            FT1writer!.Write(tmpStringChunk.ToString());
            tmpStringChunk.GetStringBuilder().Clear();
        }
        public void WriteFT2Scan(int mScanNumber, int mPrecusorScanNumber, IReaction mReaction, Scan mScan,
            Scan mPrecursorScan, IScanFilter mFilter, double mRT)
        {
            // for old sipros V3 format, ScanNumber repeats once in old version
            // tmpStringChunk!.WriteLine("S\t{0:D}\t{0:D}\t{1:F6}\t{2:F2}",
            tmpStringChunk!.WriteLine("S\t{0:D}\t{1:F6}\t{2:F2}",
               mScanNumber, mReaction.PrecursorMass, mScan.ScanStatistics.TIC);
            var trailerLabels = rawFile!.GetTrailerExtraInformation(mScanNumber);
            object chargeState = 0;
            for (int i = 0; i < trailerLabels.Labels.Length; i++)
            {
                if (trailerLabels.Labels[i] == "Charge State:")
                {
                    chargeState = trailerLabels.Values[i].Trim();
                    break;
                }
            }
            int chargeStateInt = Convert.ToInt32(chargeState);
            List<LabelPeak> precursorPeaks = FindPrecursorPeaks(mPrecursorScan, mReaction.PrecursorMass, mReaction.IsolationWidth, topNprecursor,
                intensityTreshold);
            // write isolation window center, this will be precusor peak if DDA
            tmpStringChunk!.Write("Z\t{0:D}\t{1:F6}",
                chargeStateInt, chargeStateInt * mReaction.PrecursorMass);
            // write best precusor peaks' charge and MZ in isolation window 
            foreach (LabelPeak peak in precursorPeaks!)
            {
                tmpStringChunk.Write("\t{0:D}\t{1:F6}", (int)peak.Charge, peak.Mass);
            }
            tmpStringChunk.Write("\n");
            tmpStringChunk.WriteLine("I\tRetentionTime\t{0:F6}", mRT);
            // for old sipros V3 format, add " X X" because in old formt this line has 7 chunks
            //tmpStringChunk.WriteLine("I\tScanType\t" + mFilter.MSOrder + " @ " + mFilter.GetActivation(0) + " X X");
            tmpStringChunk.WriteLine("I\tScanType\t" + mFilter.MSOrder + " @ " + mFilter.GetActivation(0));
            tmpStringChunk.WriteLine("I\tScanFilter\t" + mFilter.ToString());
            tmpStringChunk.WriteLine("D\tParentScanNumber\t{0:D}", mPrecusorScanNumber);
            WritePeak(tmpStringChunk, mScan);
            FT2writer!.Write(tmpStringChunk.ToString());
            tmpStringChunk.GetStringBuilder().Clear();
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
            for (int i = 0; i < MS2scansChunk.Count; i++)
            {
                if (!ifMergeScans)
                {
                    if (Math.Abs(MS2ScansRTsChunk[i] - leftPrecursorRT)
                    < Math.Abs(MS2ScansRTsChunk[i] - rightPrecursorRT))
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
                WriteFT2Scan(MS2scanNumbersChunk[i], currentPrecursorScanNumber, MS2scanReactionsChunk[i],
                    MS2scansChunk[i], currentPrecursorScan, MS2scanFiltersChunk[i], MS2ScansRTsChunk[i]);
            }
        }
        public Scan merge2Scans(int leftScanNumber, int rightScanNumber)
        {
            List<int> scanNumbers = new(new[] { leftScanNumber, rightScanNumber });
            var options = rawFile.DefaultMassOptions();
            options.ToleranceUnits = ToleranceUnits.ppm;
            options.Tolerance = 10.0;
            Scan mergedScan = averager!.AverageScans(scanNumbers, options);
            return mergedScan;
        }
        public void Write()
        {
            WriteHeader();
            Console.WriteLine("Converting " + rawFileName + "'s " + firstScanNumber
                + " to " + lastScanNumber + " Scans");
            int currentScanNumber = firstScanNumber, precursorScanCount = 0, leftPrecursorScanNumber = 0, rightPrecursorScanNumber = 0;
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
                currentFilter = rawFile!.GetFilterForScanNumber(currentScanNumber);
                if (currentFilter.MSOrder == ThermoFisher.CommonCore.Data.FilterEnums.MSOrderType.Ms)
                {
                    precursorScanCount++;
                    if (precursorScanCount == 1)
                    {
                        leftPrecursorScanNumber = currentScanNumber;
                        leftPrecursorScan = currentScan;
                        leftPrecursorRT = currentRT;
                        if (!ifMergeScans)
                            WriteFT1Scan(leftPrecursorScanNumber, leftPrecursorScan,
                                currentFilter, leftPrecursorRT);
                    }
                    else
                    {
                        rightPrecursorScanNumber = currentScanNumber;
                        rightPrecursorScan = currentScan;
                        rightPrecursorRT = currentRT;
                        writeScansChunk(leftPrecursorScanNumber, rightPrecursorScanNumber,
                            leftPrecursorScan, rightPrecursorScan, currentFilter,
                            leftPrecursorRT, rightPrecursorRT);
                        MS2scansChunk.Clear();
                        MS2scanNumbersChunk.Clear();
                        MS2scanFiltersChunk.Clear();
                        MS2scanReactionsChunk.Clear();
                        MS2ScansRTsChunk.Clear();
                        leftPrecursorScanNumber = currentScanNumber;
                        leftPrecursorScan = currentScan;
                        leftPrecursorRT = currentRT;
                    }
                }
                else
                {
                    currentEvent = rawFile.GetScanEventForScanNumber(currentScanNumber);
                    currentReaction = currentEvent.GetReaction(0);
                    MS2scansChunk.Add(currentScan);
                    MS2scanNumbersChunk.Add(currentScanNumber);
                    MS2scanFiltersChunk.Add(currentFilter);
                    MS2scanReactionsChunk.Add(currentReaction);
                    MS2ScansRTsChunk.Add(currentRT);
                }
                currentScanNumber++;
            }
            // write left MS2 scans chunk in the end of raw file
            for (int i = 0; i < MS2scansChunk.Count; i++)
            {
                WriteFT2Scan(MS2scanNumbersChunk[i], rightPrecursorScanNumber, MS2scanReactionsChunk[i],
                             MS2scansChunk[i], rightPrecursorScan,
                             MS2scanFiltersChunk[i], MS2ScansRTsChunk[i]);
            }
            FT1writer!.Close();
            FT2writer!.Close();
            Console.WriteLine(rawFileName + "'s " + firstScanNumber + " to " + lastScanNumber + " scans convert finished");
        }
        public void SplitedWrite(int threads)
        {
            Parallel.ForEach(scanNumberRanges!, new ParallelOptions { MaxDegreeOfParallelism = threads }, (scanNumberRange) =>
            {
                FTwriter writer2 = new()
                {
                    rawFile = rawFile,
                    rawFileName = rawFileName,
                    ifMergeScans = ifMergeScans,
                    topNprecursor = topNprecursor,
                    firstScanNumber = scanNumberRange[0],
                    lastScanNumber = scanNumberRange[1],
                    FT1writer = new StreamWriter(outPath + "/" + Path.GetFileNameWithoutExtension(rawFileName)
                    + "." + scanNumberRange[0] + ".FT1"),
                    FT2writer = new StreamWriter(outPath + "/" + Path.GetFileNameWithoutExtension(rawFileName)
                    + "." + scanNumberRange[0] + ".FT2")
                };
                writer2.Write();
            });
        }
    }
    class MainProgram
    {
        static string[]? rawFiles;
        static string rawFile = "";
        static string inPath = "";
        static string outPath = "";
        static int threads = 6;
        static int scanNumbersPerFT2 = 0;
        static bool ifMergeScans = false;
        static int topN = 15;
        public static void StopNoCloseWindow()
        {
            // Console.WriteLine("Press any key to exit");
            // not close window when exit
            // Console.ReadKey();
        }
        static bool ParseArgs(string[] args)
        {
            bool rValue = false;
            inPath = Directory.GetCurrentDirectory();
            outPath = inPath;
            string help = "On windows: ./Raxport.exe -i 'input path' -o 'output path -j 'threads number'\n" +
                "Or ./Raxport.exe -f 'one raw file name' -o 'output path'\n" + "\n" +
                "On linux: ./Raxport -i 'input path' -o 'output path' -j 'threads number'\n" +
                "Or ./Raxport -f 'one raw file name' -o 'output path'\n" +
                "\n" +
                "add '-s 20000' if you want to split 20000 scans per .FT2 file \n" +
                "add '-m' if you want to merge 2 adjcent MS1 scans \n" +
                "add '-n 5' if you want to limit precursor numbers of each MS2 scan to 5 \n" +
                "Default path is ./ \n";
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == "-h")
                {
                    Console.WriteLine(help);
                    Environment.Exit(0);
                }
                if ((i + 1) > (args.Length - 1))
                    break;
                else if (args[i] == "-i")
                    inPath = args[++i];
                else if (args[i] == "-f")
                    rawFile = args[++i];
                else if (args[i] == "-o")
                    outPath = args[++i];
                else if (args[i] == "-j")
                    _ = int.TryParse(args[++i], out threads);
                else if (args[i] == "-s")
                    _ = int.TryParse(args[++i], out scanNumbersPerFT2);
                else if (args[i] == "-m")
                    ifMergeScans = true;
                else if (args[i] == "-n")
                    _ = int.TryParse(args[++i], out topN);
            }
            try
            {
                if (rawFile != "")
                {
                    // add the raw file from '-f' to rawFiles                   
                    rawFiles = new String[1];
                    rawFiles[0] = rawFile;
                }
                else
                    rawFiles = Directory.GetFiles(inPath, "*.raw");

                if (rawFiles.Length > 0)
                {
                    if (!Directory.Exists(outPath))
                        Directory.CreateDirectory(outPath);
                    if (Directory.Exists(outPath))
                        rValue = true;
                }
                else
                {
                    Console.WriteLine("Args parsing failed! no raw file was found!");
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
        static void Main(string[] args)
        {
            if (ParseArgs(args))
            {
                if (scanNumbersPerFT2 != 0)
                    foreach (var file in rawFiles!)
                    {
                        FTwriter writer = new(file, outPath, false);
                        writer.ifMergeScans = ifMergeScans;
                        writer.topNprecursor = topN;
                        writer.InitScanNumberRanges(scanNumbersPerFT2);
                        writer.SplitedWrite(threads);
                    }
                else
                {
                    Parallel.ForEach(rawFiles!, new ParallelOptions { MaxDegreeOfParallelism = threads }, (rawFile) =>
                       {
                           FTwriter writer = new(rawFile, outPath, true);
                           writer.ifMergeScans = ifMergeScans;
                           writer.topNprecursor = topN;
                           writer.Write();
                       });
                }
                Console.WriteLine("All convert finished");
            }
            StopNoCloseWindow();
        }
    }
}
