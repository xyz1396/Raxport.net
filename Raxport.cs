using ThermoFisher.CommonCore.Data;
using ThermoFisher.CommonCore.Data.Business;
using ThermoFisher.CommonCore.Data.Interfaces;
using ThermoFisher.CommonCore.RawFileReader;
using System.Collections;

namespace Raxport
{
    public class FTwriter
    {
        static private string raxportVersion = "5";
        private string? rawFileName;
        private readonly string? outPath;
        private IRawDataPlus? rawFile;
        private int firstScanNumber;
        private int lastScanNumber;
        // for precursor select
        public int topNprecursor = 5;
        public double intensityTreshold = 0.95;
        public double mzTolerance = 0.01;
        public IEnumerable<int>? MS1scanNumbers;
        public List<int[]>? scanNumberRanges;
        private int currentScanNumber;
        private IScanEvent? currentEvent;
        public Scan? currentPrecusorScan;
        public List<LabelPeak>? currentPrecurosrPeaks;
        private Scan? currentScan;
        private int currentPrecusorScanNumber;
        private IScanFilter? currentFilter;
        private IReaction? currentReaction;
        private bool hasCharge;
        private StreamWriter? FT1writer;
        private StreamWriter? FT2writer;
        // rawFileName: file name with full path of raw file
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
        public void InitScanNumberRanges(int splitRange)
        {
            int startScanNumber = firstScanNumber, endScanNumber = lastScanNumber;
            IEnumerator g = MS1scanNumbers!.GetEnumerator();
            scanNumberRanges = new List<int[]>();
            while (g.MoveNext())
            {
                if ((int)g.Current - startScanNumber > splitRange)
                {
                    endScanNumber = (int)g.Current - 1;
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
            if (i == peaks.Length - 1)
                return (peaksInRange);
            if (peaks[i].Mass < start)
                i++;
            while (end > peaks[i].Mass && i < peaks.Length - 1)
            {
                // remove peak without charge
                if (peaks[i].Charge > 0)
                    peaksInRange.Add(peaks[i]);
                i++;
            }
            return (peaksInRange);
        }
        // find prescursor peaks with topN intensity in a range 
        public void FindPrecursorPeaks(double precusorMZ, double isolationWindow, int topN, double intensityRatio)
        {
            currentPrecurosrPeaks = new List<LabelPeak>();
            double neutronMass = 1.003355;
            bool foundIsotopicPeak = false;
            double totalIntensity = 0.000000001;
            double summedIntensity = 0;
            int[] tryISO = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
            List<LabelPeak> peaksInRange = BinarySearchMzRange(currentPrecusorScan!.CentroidScan.GetLabelPeaks(),
                precusorMZ - isolationWindow / 2, precusorMZ + isolationWindow / 2);
            peaksInRange = peaksInRange.OrderByDescending(o => o.Intensity).ToList();
            foreach (LabelPeak peak in peaksInRange) { totalIntensity += peak.Intensity; }
            for (int i = 0; i < peaksInRange.Count; i++)
            {
                // stop while intensity ratio threshold of isotopic peaks reached
                if (summedIntensity / totalIntensity > intensityRatio)
                    break;
                currentPrecurosrPeaks.Add(peaksInRange[i]);
                summedIntensity += peaksInRange[i].Intensity;
                if (currentPrecurosrPeaks.Count >= topN)
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
        public void WritePeak(StreamWriter writer)
        {
            if (hasCharge)
            {
                LabelPeak[] peaks = currentScan!.CentroidScan.GetLabelPeaks();
                foreach (LabelPeak peak in peaks)
                {
                    // SignalToNoise is peak.Noise in old V3 version
                    writer.WriteLine("{0:F6}\t{1:F2}\t{2:F0}\t{3:F2}\t{4:F2}\t{5:F0}",
                        peak.Mass, peak.Intensity, peak.Resolution, peak.Baseline, peak.SignalToNoise, peak.Charge);
                }
            }
            else
            {
                for (int i = 0; i < currentScan!.SegmentedScan.Positions.Length; i++)
                {
                    writer.WriteLine("{0:F6}\t{1:F2}", currentScan.SegmentedScan.Positions[i], currentScan.SegmentedScan.Intensities[i]);
                }
            }
        }
        public void WriteFT1Scan()
        {
            FT1writer!.WriteLine("S\t{0:D}\t{1:F2}", currentScanNumber, currentScan!.ScanStatistics.TIC);
            FT1writer.WriteLine("I\tRetentionTime\t{0:F6}", rawFile!.RetentionTimeFromScanNumber(currentScanNumber));
            FT1writer.WriteLine("I\tScanType\tMs1");
            FT1writer.WriteLine("I\tScanFilter\t" + currentFilter!.ToString());
            WritePeak(FT1writer);
        }
        public void WriteFT2Scan()
        {
            // for old sipros V3 format, currentScanNumber repeats once in old version
            //FT2writer.WriteLine("S\t{0:D}\t{0:D}\t{1:F6}\t{2:F2}",
            FT2writer!.WriteLine("S\t{0:D}\t{1:F6}\t{2:F2}",
               currentScanNumber, currentReaction!.PrecursorMass, currentScan!.ScanStatistics.TIC);
            var trailerLabels = rawFile!.GetTrailerExtraInformation(currentScanNumber);
            object chargeState = 0;
            for (int i = 0; i < trailerLabels.Labels.Length; i++)
            {
                if (trailerLabels.Labels[i] == "Charge State:")
                {
                    chargeState = rawFile.GetTrailerExtraValue(currentScanNumber, i);
                    break;
                }
            }
            int chargeStateInt = Convert.ToInt32(chargeState);
            FindPrecursorPeaks(currentReaction.PrecursorMass, currentReaction.IsolationWidth, topNprecursor,
                intensityTreshold);
            // write isolation window center, this will be precusor peak if DDA
            FT2writer.Write("Z\t{0:D}\t{1:F6}",
                chargeStateInt, chargeStateInt * currentReaction.PrecursorMass);
            // write best precusor peaks' charge and MZ in isolation window 
            foreach (LabelPeak peak in currentPrecurosrPeaks!)
            {
                FT2writer.Write("\t{0:D}\t{1:F6}", (int)peak.Charge, peak.Mass);
            }
            FT2writer.Write("\n");
            FT2writer.WriteLine("I\tRetentionTime\t{0:F6}", rawFile.RetentionTimeFromScanNumber(currentScanNumber));
            // for old sipros V3 format, add " X X" because in old formt this line has 7 chunks
            //FT2writer.WriteLine("I\tScanType\t" + currentFilter.MSOrder + " @ " + currentFilter.GetActivation(0) + " X X");
            FT2writer.WriteLine("I\tScanType\t" + currentFilter!.MSOrder + " @ " + currentFilter.GetActivation(0));
            FT2writer.WriteLine("I\tScanFilter\t" + currentFilter.ToString());
            FT2writer.WriteLine("D\tParentScanNumber\t{0:D}", currentPrecusorScanNumber);
            WritePeak(FT2writer);
        }
        public void Write()
        {
            WriteHeader();
            currentScanNumber = firstScanNumber;
            Console.WriteLine("Converting " + rawFileName + "'s " + firstScanNumber + " to " + lastScanNumber + " Scans");
            while (currentScanNumber <= lastScanNumber)
            {
                currentScan = Scan.FromFile(rawFile, currentScanNumber);
                currentFilter = rawFile!.GetFilterForScanNumber(currentScanNumber);
                currentEvent = rawFile.GetScanEventForScanNumber(currentScanNumber);
                if (currentScan.HasCentroidStream)
                    hasCharge = true;
                else
                    hasCharge = false;
                if (currentFilter.MSOrder == ThermoFisher.CommonCore.Data.FilterEnums.MSOrderType.Ms)
                {
                    currentPrecusorScanNumber = currentScanNumber;
                    currentPrecusorScan = Scan.FromFile(rawFile, currentScanNumber);
                    WriteFT1Scan();
                }
                if (currentFilter.MSOrder == ThermoFisher.CommonCore.Data.FilterEnums.MSOrderType.Ms2)
                {
                    currentReaction = currentEvent.GetReaction(0);
                    WriteFT2Scan();
                }
                currentScanNumber++;
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
                        writer.InitScanNumberRanges(scanNumbersPerFT2);
                        writer.SplitedWrite(threads);
                    }
                else
                {
                    Parallel.ForEach(rawFiles!, new ParallelOptions { MaxDegreeOfParallelism = threads }, (rawFile) =>
                       {
                           FTwriter writer = new(rawFile, outPath, true);
                           writer.Write();
                       });
                }
                Console.WriteLine("All convert finished");
            }
            StopNoCloseWindow();
        }
    }
}
