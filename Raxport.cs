using ThermoFisher.CommonCore.Data.Business;
using ThermoFisher.CommonCore.Data.Interfaces;
using ThermoFisher.CommonCore.RawFileReader;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Raxport
{
    public class FTwriter
    {
        private string rawFileName;
        private IRawDataPlus rawFile;
        private int firstScanNumber;
        private int lastScanNumber;
        private int currentScanNumber;
        private IScanEvent currentEvent;
        private Scan currentScan;
        private int currentPrecusorScanNumber;
        private IScanFilter currentFilter;
        private IReaction currentReaction;
        private bool hasCharge;
        private StreamWriter FT1writer;
        private StreamWriter FT2writer;
        // rawFileName: file name with full path of raw file
        public FTwriter(string fileName, string outPath)
        {
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
                    mainProgram.stopNoCloseWindow();
                    Environment.Exit(0);
                }
                rawFile.SelectInstrument(Device.MS, 1);
                firstScanNumber = rawFile.RunHeaderEx.FirstSpectrum;
                lastScanNumber = rawFile.RunHeaderEx.LastSpectrum;
                FT1writer = new StreamWriter(outPath + "/" + Path.GetFileNameWithoutExtension(rawFileName) + ".FT1");
                FT2writer = new StreamWriter(outPath + "/" + Path.GetFileNameWithoutExtension(rawFileName) + ".FT2");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString() + "\nCannot read " + rawFileName);
                mainProgram.stopNoCloseWindow();
                Environment.Exit(0);
            }
        }
        public void writeHeader()
        {
            FT1writer.WriteLine("H\tExtractor\tRaxport V3.4");
            FT1writer.WriteLine("H\tm/z\tIntensity\tResolution\tBaseline\tNoise\tCharge");
            FT1writer.WriteLine("H\tInstrument Model\t" + rawFile.GetInstrumentData().Model);

            FT2writer.WriteLine("H\tExtractor\tRaxport V3.4");
            FT2writer.WriteLine("H\tm/z\tIntensity\tResolution\tBaseline\tNoise\tCharge");
            FT2writer.WriteLine("H\tInstrument Model\t" + rawFile.GetInstrumentData().Model);
        }
        public void writePeak(StreamWriter writer)
        {
            if (hasCharge)
            {
                LabelPeak[] peaks = currentScan.CentroidScan.GetLabelPeaks();
                foreach (LabelPeak peak in peaks)
                {
                    // SignalToNoise is peak.Noise in old V3 version
                    writer.WriteLine("{0:F6}\t{1:F2}\t{2:F0}\t{3:F2}\t{4:F2}\t{5:F0}",
                        peak.Mass, peak.Intensity, peak.Resolution, peak.Baseline, peak.SignalToNoise, peak.Charge);
                }
            }
            else
            {
                for (int i = 0; i < currentScan.SegmentedScan.Positions.Length; i++)
                {
                    writer.WriteLine("{0:F6}\t{1:F2}", currentScan.SegmentedScan.Positions[i], currentScan.SegmentedScan.Intensities[i]);
                }
            }
        }
        public void writeFT1Scan(Scan FT1Scan)
        {
            FT1writer.WriteLine("S\t{0:D}\t{1:F2}", currentScanNumber, currentScan.ScanStatistics.TIC);
            FT1writer.WriteLine("I\tRetentionTime\t{0:F6}", rawFile.RetentionTimeFromScanNumber(currentScanNumber));
            FT1writer.WriteLine("I\tScanType\tMs1");
            FT1writer.WriteLine("I\tScanFilter\t" + currentFilter.ToString());
            writePeak(FT1writer);
        }
        public void writeFT2Scan(Scan FT2Scan)
        {
            // for old sipros V3 format
            // FT2writer.WriteLine("S\t{0:D}\t{0:D}\t{1:F6}\t{2:F2}",
            FT2writer.WriteLine("S\t{0:D}\t{1:F6}\t{2:F2}",
                currentScanNumber, currentReaction.PrecursorMass, currentScan.ScanStatistics.TIC);
            var trailerLabels = rawFile.GetTrailerExtraInformation(currentScanNumber);
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
            FT2writer.WriteLine("Z\t{0:D}\t{1:F6}",
                chargeStateInt, chargeStateInt * currentReaction.PrecursorMass);
            FT2writer.WriteLine("I\tRetentionTime\t{0:F6}", rawFile.RetentionTimeFromScanNumber(currentScanNumber));
            FT2writer.WriteLine("I\tScanType\t" + currentFilter.MSOrder + " @ " + currentFilter.GetActivation(0));
            FT2writer.WriteLine("I\tScanFilter\t" + currentFilter.ToString());
            FT2writer.WriteLine("D\tParentScanNumber\t{0:D}", currentPrecusorScanNumber);
            writePeak(FT2writer);
        }
        public void write()
        {
            writeHeader();
            currentScanNumber = firstScanNumber;
            Console.WriteLine(rawFileName + " has " + firstScanNumber + " to " + lastScanNumber + " Scans");
            Console.Write("Converting scans\n");
            while (currentScanNumber <= lastScanNumber)
            {
                currentScan = Scan.FromFile(rawFile, currentScanNumber);
                currentFilter = rawFile.GetFilterForScanNumber(currentScanNumber);
                currentEvent = rawFile.GetScanEventForScanNumber(currentScanNumber);
                if (currentScan.HasCentroidStream)
                    hasCharge = true;
                else
                    hasCharge = false;
                if (currentFilter.MSOrder == ThermoFisher.CommonCore.Data.FilterEnums.MSOrderType.Ms)
                {
                    currentPrecusorScanNumber = currentScanNumber;
                    writeFT1Scan(currentScan);
                }
                if (currentFilter.MSOrder == ThermoFisher.CommonCore.Data.FilterEnums.MSOrderType.Ms2)
                {
                    currentReaction = currentEvent.GetReaction(0);
                    writeFT2Scan(currentScan);
                }
                currentScanNumber++;
            }
            FT1writer.Close();
            FT2writer.Close();
            Console.WriteLine("\n" + rawFileName + " convert finished");
        }
    }

    class mainProgram
    {
        static string[] rawFiles;
        static string inPath;
        static string outPath;
        static int threads = 6;
        public static void stopNoCloseWindow()
        {
            // Console.WriteLine("Press any key to exit");
            // not close window when exit
            // Console.ReadKey();
        }
        static bool parseArgs(string[] args)
        {
            bool rValue = false;
            inPath = Directory.GetCurrentDirectory();
            outPath = inPath;
            string help = "On windows: Raxport.exe -i 'input path' -o 'output path -j 'threads number'\n" +
                "On linux: mono Raxport.exe -i 'input path' -o 'output path' -j 'threads number'\n" +
                "Default path is ./";
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == "-i")
                    inPath = args[++i];
                else if (args[i] == "-o")
                    outPath = args[++i];
                else if (args[i] == "-j")
                    Int32.TryParse(args[++i], out threads);
                else if (args[i] == "-h")
                {
                    Console.WriteLine(help);
                    Environment.Exit(0);
                }
            }
            try
            {
                rawFiles = Directory.GetFiles(inPath, "*.raw");
                if (!Directory.Exists(outPath))
                {
                    Directory.CreateDirectory(outPath);
                }
                if (rawFiles.Length > 0 && Directory.Exists(outPath))
                {
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
            if (parseArgs(args))
            {
                Parallel.ForEach(rawFiles, new ParallelOptions { MaxDegreeOfParallelism = threads }, (rawFile) =>
                   {
                       FTwriter writer = new FTwriter(rawFile, outPath);
                       writer.write();
                   });
                Console.WriteLine("All convert finished");
            }
            stopNoCloseWindow();
        }
    }
}
