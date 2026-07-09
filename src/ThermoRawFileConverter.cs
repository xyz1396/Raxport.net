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
    internal sealed class ThermoRawFileConverter
    {
        private readonly string rawFileName;
        private readonly string outPath;
        private readonly int topNprecursor;
        private readonly double intensityThreshold;
        private readonly double mzTolerancePpm;
        private readonly long maxBufferedPeaks;
        private readonly int writerCompressionLevel;
        private readonly RaxportOutputFormat outputFormat;
        private readonly bool ifMergeScans;
        private IRawDataPlus? rawFile;
        private IRaxportWriter? scanWriter;
        private int firstScanNumber;
        private int lastScanNumber;

        private IScanAveragePlus? averager;

        private readonly List<int> MSnScanNumbersChunk = new();
        private readonly List<IScanFilter> MSnScanFiltersChunk = new();
        private readonly List<IReaction> MSnScanReactionsChunk = new();
        private readonly List<double> MSnScansRTsChunk = new();

        public ThermoRawFileConverter(
            string rawFileName,
            string outPath,
            int topNprecursor,
            double mzTolerancePpm,
            long maxBufferedPeaks,
            int writerCompressionLevel,
            RaxportOutputFormat outputFormat,
            bool ifMergeScans = false,
            double intensityThreshold = 0.99)
        {
            this.rawFileName = rawFileName;
            this.outPath = outPath;
            this.topNprecursor = topNprecursor;
            this.mzTolerancePpm = mzTolerancePpm;
            this.maxBufferedPeaks = maxBufferedPeaks;
            this.writerCompressionLevel = writerCompressionLevel;
            this.outputFormat = outputFormat;
            this.ifMergeScans = ifMergeScans;
            this.intensityThreshold = intensityThreshold;

            try
            {
                rawFile = RawFileReaderAdapter.FileFactory(rawFileName);
                if (!rawFile.IsOpen)
                {
                    Console.WriteLine("Cannot read " + rawFileName);
                    Raxport.StopNoCloseWindow();
                    Environment.Exit(0);
                }
                rawFile.SelectInstrument(Device.MS, 1);
                firstScanNumber = rawFile.RunHeaderEx.FirstSpectrum;
                lastScanNumber = rawFile.RunHeaderEx.LastSpectrum;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString() + "\nCannot read " + rawFileName);
                Raxport.StopNoCloseWindow();
                Environment.Exit(0);
            }
        }

        private void WriteFT1Scan(int mScanNumber, IScanFilter mFilter, double mRT,
            IReadOnlyList<RaxportPeakRecord>? peaks = null,
            Scan? mScan = null)
        {
            scanWriter!.AddScan(new RaxportScanRecord(
                mScanNumber,
                1,
                mRT,
                mScan?.ScanStatistics.TIC ?? GetTic(mScanNumber),
                mFilter.ToString(),
                string.Empty,
                0,
                null,
                peaks ?? (mScan is null ? CollectPeaks(mScanNumber) : CollectPeaks(mScan)),
                IsCentroided(mScanNumber, mScan),
                GetScanWindowLowerMz(mScanNumber, mScan),
                GetScanWindowUpperMz(mScanNumber, mScan),
                mFilter.Polarity.ToString()));
        }

        private void WriteFT2Scan(int mScanNumber, int mPrecusorScanNumber, IReaction mReaction,
            IScanFilter mFilter, double mRT, IReadOnlyList<RaxportPeakRecord> parentPeaks)
        {
            int chargeStateInt = GetTrailerChargeState(mScanNumber);
            List<RaxportPeakRecord> evidencePeaks = PrecursorSelector.GetPrecursorEvidencePeaks(
                parentPeaks,
                mReaction.PrecursorMass,
                mReaction.IsolationWidth,
                mzTolerancePpm);
            List<RaxportPeakRecord> isotopeEvidencePeaks = PrecursorSelector.GetIsotopeEvidencePeaks(
                parentPeaks,
                mReaction.PrecursorMass,
                mReaction.IsolationWidth,
                mzTolerancePpm);
            List<RaxportPeakRecord> precursorPeaks = PrecursorSelector.FindPrecursorPeaksFromEvidence(
                evidencePeaks,
                isotopeEvidencePeaks,
                topNprecursor,
                intensityThreshold,
                mzTolerancePpm);

            RaxportReactionRecord reaction = new(
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
                    isotopeEvidencePeaks,
                    mReaction.PrecursorMass,
                    mReaction.IsolationWidth,
                    topNprecursor,
                    mzTolerancePpm,
                    chargeStateInt));

            scanWriter!.AddScan(new RaxportScanRecord(
                mScanNumber,
                GetMsOrder(mFilter),
                mRT,
                GetTic(mScanNumber),
                mFilter.ToString(),
                GetActivation(mFilter),
                mPrecusorScanNumber,
                reaction,
                CollectPeaks(mScanNumber),
                IsCentroided(mScanNumber),
                GetScanWindowLowerMz(mScanNumber),
                GetScanWindowUpperMz(mScanNumber),
                mFilter.Polarity.ToString()));
        }

        private void WriteScansChunk(int leftPrecursorScanNumber, int rightPrecursorScanNumber,
            Scan leftPrecursorScan, Scan rightPrecursorScan, IScanFilter rightFilter,
            double leftPrecursorRT, double rightPrecursorRT,
            IReadOnlyList<RaxportPeakRecord> leftPrecursorPeaks,
            IReadOnlyList<RaxportPeakRecord> rightPrecursorPeaks)
        {
            Scan currentPrecursorScan = rightPrecursorScan;
            int currentPrecursorScanNumber = rightPrecursorScanNumber;
            IReadOnlyList<RaxportPeakRecord> currentPrecursorPeaks = rightPrecursorPeaks;
            if (ifMergeScans)
            {
                rightPrecursorRT = (leftPrecursorRT + rightPrecursorRT) / 2.0;
                currentPrecursorScan = MergeTwoScans(leftPrecursorScanNumber, rightPrecursorScanNumber);
                currentPrecursorScanNumber = rightPrecursorScanNumber;
                currentPrecursorPeaks = CollectPeaks(currentPrecursorScan);
            }
            WriteFT1Scan(currentPrecursorScanNumber, rightFilter, rightPrecursorRT, currentPrecursorPeaks, ifMergeScans ? currentPrecursorScan : null);
            for (int i = 0; i < MSnScanNumbersChunk.Count; i++)
            {
                if (!ifMergeScans)
                {
                    if (Math.Abs(MSnScansRTsChunk[i] - leftPrecursorRT)
                    < Math.Abs(MSnScansRTsChunk[i] - rightPrecursorRT))
                    {
                        currentPrecursorScanNumber = leftPrecursorScanNumber;
                        currentPrecursorPeaks = leftPrecursorPeaks;
                    }
                    else
                    {
                        currentPrecursorScanNumber = rightPrecursorScanNumber;
                        currentPrecursorPeaks = rightPrecursorPeaks;
                    }
                }
                WriteFT2Scan(MSnScanNumbersChunk[i], currentPrecursorScanNumber, MSnScanReactionsChunk[i],
                    MSnScanFiltersChunk[i], MSnScansRTsChunk[i], currentPrecursorPeaks);
            }
        }

        public Scan MergeTwoScans(int leftScanNumber, int rightScanNumber)
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
            string outputFile = Path.Combine(outPath, Path.GetFileNameWithoutExtension(rawFileName) + RaxportWriterFactory.GetOutputExtension(outputFormat));
            Stopwatch totalTimer = Stopwatch.StartNew();
            using IRaxportWriter writer = RaxportWriterFactory.Create(
                outputFile,
                rawFileName,
                rawFile!.GetInstrumentData().Model,
                outputFormat,
                maxBufferedPeaks,
                writerCompressionLevel);
            scanWriter = writer;

            LogConversionStart(outputFile);
            int currentScanNumber = firstScanNumber, precursorScanCount = 0, leftPrecursorScanNumber = 0, rightPrecursorScanNumber = 0;
            int parsedMS1Scans = 0, parsedMSnScans = 0;
            averager = ScanAveragerPlus.FromFile(rawFile);
            IScanEvent currentEvent;
            IScanFilter currentFilter;
            IReaction currentReaction;
            Scan currentScan = new(), leftPrecursorScan = new(), rightPrecursorScan = new();
            double currentRT = 0, leftPrecursorRT = 0, rightPrecursorRT = 0;
            IReadOnlyList<RaxportPeakRecord> leftPrecursorPeaks = Array.Empty<RaxportPeakRecord>();
            IReadOnlyList<RaxportPeakRecord> rightPrecursorPeaks = Array.Empty<RaxportPeakRecord>();
            while (currentScanNumber <= lastScanNumber)
            {
                currentRT = rawFile!.RetentionTimeFromScanNumber(currentScanNumber);
                currentFilter = rawFile.GetFilterForScanNumber(currentScanNumber);
                if (currentFilter.MSOrder == MSOrderType.Ms)
                {
                    if (ifMergeScans)
                    {
                        currentScan = Scan.FromFile(rawFile, currentScanNumber);
                    }
                    parsedMS1Scans++;
                    precursorScanCount++;
                    if (precursorScanCount == 1)
                    {
                        leftPrecursorScanNumber = currentScanNumber;
                        leftPrecursorScan = currentScan;
                        leftPrecursorRT = currentRT;
                        if (!ifMergeScans)
                        {
                            leftPrecursorPeaks = CollectPeaks(currentScanNumber);
                            WriteFT1Scan(leftPrecursorScanNumber, currentFilter, leftPrecursorRT, leftPrecursorPeaks);
                        }
                    }
                    else
                    {
                        rightPrecursorScanNumber = currentScanNumber;
                        rightPrecursorScan = currentScan;
                        rightPrecursorRT = currentRT;
                        rightPrecursorPeaks = ifMergeScans ? Array.Empty<RaxportPeakRecord>() : CollectPeaks(currentScanNumber);
                        WriteScansChunk(leftPrecursorScanNumber, rightPrecursorScanNumber,
                            leftPrecursorScan, rightPrecursorScan, currentFilter,
                            leftPrecursorRT, rightPrecursorRT,
                            leftPrecursorPeaks, rightPrecursorPeaks);
                        ClearMSnChunk();
                        leftPrecursorScanNumber = currentScanNumber;
                        leftPrecursorScan = currentScan;
                        leftPrecursorRT = currentRT;
                        leftPrecursorPeaks = rightPrecursorPeaks;
                    }
                }
                else
                {
                    parsedMSnScans++;
                    currentEvent = rawFile.GetScanEventForScanNumber(currentScanNumber);
                    currentReaction = currentEvent.GetReaction(0);
                    MSnScanNumbersChunk.Add(currentScanNumber);
                    MSnScanFiltersChunk.Add(currentFilter);
                    MSnScanReactionsChunk.Add(currentReaction);
                    MSnScansRTsChunk.Add(currentRT);
                }
                currentScanNumber++;
            }

            if (MSnScanNumbersChunk.Count > 0)
            {
                IReadOnlyList<RaxportPeakRecord> trailingPrecursorPeaks = ifMergeScans
                    ? CollectPeaks(rightPrecursorScan)
                    : rightPrecursorPeaks;
                for (int i = 0; i < MSnScanNumbersChunk.Count; i++)
                {
                    WriteFT2Scan(MSnScanNumbersChunk[i], rightPrecursorScanNumber, MSnScanReactionsChunk[i],
                                 MSnScanFiltersChunk[i], MSnScansRTsChunk[i], trailingPrecursorPeaks);
                }
            }
            scanWriter = null;
            writer.Dispose();
            totalTimer.Stop();
            LogTimingSummary(writer, totalTimer.Elapsed, parsedMS1Scans, parsedMSnScans);
        }

        private List<RaxportPeakRecord> CollectPeaks(int scanNumber)
        {
            CentroidStream? centroidStream = rawFile!.GetCentroidStream(scanNumber, false);
            if (centroidStream is not null && centroidStream.Length > 0)
            {
                return CollectPeaks(centroidStream);
            }

            SegmentedScan segmentedScan = rawFile.GetSegmentedScanFromScanNumber(scanNumber);
            return CollectPeaks(segmentedScan);
        }

        private static List<RaxportPeakRecord> CollectPeaks(Scan mScan)
        {
            return mScan.HasCentroidStream
                ? CollectPeaks(mScan.CentroidScan)
                : CollectPeaks(mScan.SegmentedScan);
        }

        private static List<RaxportPeakRecord> CollectPeaks(CentroidStream centroidStream)
        {
            LabelPeak[] labelPeaks = centroidStream.GetLabelPeaks();
            List<RaxportPeakRecord> peaks = new(labelPeaks.Length);
            foreach (LabelPeak peak in labelPeaks)
            {
                peaks.Add(new RaxportPeakRecord(
                    peak.Mass,
                    peak.Intensity,
                    peak.Resolution,
                    peak.Baseline,
                    peak.SignalToNoise,
                    (int)peak.Charge));
            }
            return peaks;
        }

        private static List<RaxportPeakRecord> CollectPeaks(SegmentedScan segmentedScan)
        {
            double[] positions = segmentedScan.Positions;
            double[] intensities = segmentedScan.Intensities;
            List<RaxportPeakRecord> peaks = new(positions.Length);
            for (int i = 0; i < positions.Length; i++)
            {
                peaks.Add(new RaxportPeakRecord(
                    positions[i],
                    intensities[i],
                    0,
                    0,
                    0,
                    0));
            }
            return peaks;
        }

        private bool IsCentroided(int scanNumber, Scan? scan = null)
        {
            if (scan is not null)
            {
                return scan.HasCentroidStream;
            }

            CentroidStream? centroidStream = rawFile!.GetCentroidStream(scanNumber, false);
            return centroidStream is not null && centroidStream.Length > 0;
        }

        private double? GetScanWindowLowerMz(int scanNumber, Scan? scan = null)
        {
            double lowMass = scan?.ScanStatistics.LowMass ?? rawFile!.GetScanStatsForScanNumber(scanNumber).LowMass;
            return lowMass > 0 ? lowMass : null;
        }

        private double? GetScanWindowUpperMz(int scanNumber, Scan? scan = null)
        {
            double highMass = scan?.ScanStatistics.HighMass ?? rawFile!.GetScanStatsForScanNumber(scanNumber).HighMass;
            return highMass > 0 ? highMass : null;
        }

        private double GetTic(int scanNumber)
        {
            return rawFile!.GetScanStatsForScanNumber(scanNumber).TIC;
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
            MSnScanNumbersChunk.Clear();
            MSnScanFiltersChunk.Clear();
            MSnScanReactionsChunk.Clear();
            MSnScansRTsChunk.Clear();
        }

        private void LogConversionStart(string outputFile)
        {
            Console.WriteLine();
            Console.WriteLine("============================================================");
            Console.WriteLine($"Raxport {RaxportWriterFactory.GetOutputFormatLabel(outputFormat)} conversion started");
            Console.WriteLine("------------------------------------------------------------");
            Console.WriteLine($"Input raw file      : {rawFileName}");
            Console.WriteLine($"Output {RaxportWriterFactory.GetOutputFormatLabel(outputFormat)} file : {outputFile}");
            Console.WriteLine($"Scan range          : {firstScanNumber} - {lastScanNumber} ({lastScanNumber - firstScanNumber + 1:N0} scans)");
            if (RaxportWriterFactory.UsesPeakBufferTuning(outputFormat))
            {
                Console.WriteLine($"Peak flush limit    : {maxBufferedPeaks:N0} peaks");
                Console.WriteLine($"Writer compression  : gzip level {writerCompressionLevel:N0}");
            }
            Console.WriteLine($"Merge adjacent MS1  : {ifMergeScans}");
            Console.WriteLine($"Top precursor count : {topNprecursor:N0}");
            Console.WriteLine($"m/z tolerance       : {mzTolerancePpm:0.###} ppm");
            Console.WriteLine("============================================================");
        }

        private void LogTimingSummary(IRaxportWriter writer, TimeSpan totalElapsed, int parsedMS1Scans, int parsedMSnScans)
        {
            TimeSpan writeElapsed = writer.WriteElapsed;
            TimeSpan parseAndRamElapsed = totalElapsed - writeElapsed;
            if (parseAndRamElapsed < TimeSpan.Zero)
            {
                parseAndRamElapsed = TimeSpan.Zero;
            }

            Console.WriteLine();
            Console.WriteLine("============================================================");
            Console.WriteLine($"Raxport {RaxportWriterFactory.GetOutputFormatLabel(outputFormat)} conversion finished");
            Console.WriteLine("------------------------------------------------------------");
            Console.WriteLine($"Input raw file              : {rawFileName}");
            Console.WriteLine($"Raw scans parsed            : {parsedMS1Scans + parsedMSnScans:N0} (MS1={parsedMS1Scans:N0}, MSn={parsedMSnScans:N0})");
            Console.WriteLine($"Output scan rows written    : {writer.TotalScans:N0}");
            Console.WriteLine($"Output peak rows written    : {writer.TotalPeaks:N0}");
            Console.WriteLine($"Output reaction rows written: {writer.TotalReactions:N0}");
            Console.WriteLine($"Output candidate rows       : {writer.TotalCandidates:N0}");
            Console.WriteLine($"Output flush count          : {writer.FlushCount:N0}");
            Console.WriteLine("------------------------------------------------------------");
            Console.WriteLine($"Scan parse + RAM buffering  : {FormatElapsed(parseAndRamElapsed)} ({Percent(parseAndRamElapsed, totalElapsed):0.0}%)");
            Console.WriteLine($"Output create/flush/close : {FormatElapsed(writeElapsed)} ({Percent(writeElapsed, totalElapsed):0.0}%)");
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

}
