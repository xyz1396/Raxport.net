using System.Diagnostics;
using System.Globalization;

namespace Raxport
{
    internal static class Raxport
    {
        private static string[]? rawFiles;
        private static string rawFile = "";
        private static string inPath = "";
        private static string outPath = "";
        private static int threads = 6;
        private static int peakFlushUnits = Hdf5BufferDefaults.DefaultPeakFlushUnits;
        private static long maxBufferedPeaks = Hdf5BufferDefaults.DefaultMaxBufferedPeaks;
        private static int writerCompressionLevel = Hdf5BufferDefaults.DefaultHdf5CompressionLevel;
        private static RaxportOutputFormat outputFormat = RaxportOutputFormat.Hdf5;
        private static bool ifMergeScans = false;
        private static int topN = 15;
        private static double mzTolerancePpm = 10.0;

        public static void StopNoCloseWindow()
        {
            // not close window when exit
        }

        private static bool TryParseOutputFormat(string value, out RaxportOutputFormat format)
        {
            switch (value.Trim().ToLowerInvariant())
            {
                case "h5":
                case "hdf5":
                    format = RaxportOutputFormat.Hdf5;
                    return true;
                case "mzml":
                case "indexed-mzml":
                case "indexedmzml":
                    format = RaxportOutputFormat.MzMl;
                    return true;
                default:
                    format = RaxportOutputFormat.Hdf5;
                    return false;
            }
        }

        private static string OutputFormatArgument()
        {
            return outputFormat == RaxportOutputFormat.MzMl ? "mzml" : "hdf5";
        }

        private static bool ParseArgs(string[] args)
        {
            bool rValue = false;
            inPath = Directory.GetCurrentDirectory();
            outPath = inPath;
            outputFormat = RaxportOutputFormat.Hdf5;
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
                "  --hdf5-compression-level N  HDF5 gzip compression level 0-9. 0 disables compression; default: 1.\n" +
                "  --format FORMAT          Output format: hdf5 or mzml. Default: hdf5.\n" +
                "  -n N                    Precursor m/z peaks selected for each MSn scan; charge-expanded candidates may be larger. Default: 15.\n" +
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
                if (args[i] == "-m")
                {
                    ifMergeScans = true;
                    continue;
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
                else if (args[i] == "--hdf5-compression-level")
                {
                    if (int.TryParse(args[++i], out int parsedCompressionLevel) &&
                        parsedCompressionLevel >= 0 && parsedCompressionLevel <= 9)
                    {
                        writerCompressionLevel = parsedCompressionLevel;
                    }
                }
                else if (args[i] == "--format")
                {
                    if (!TryParseOutputFormat(args[++i], out outputFormat))
                    {
                        Console.WriteLine($"Args parsing failed! unsupported output format '{args[i]}'. Expected hdf5 or mzml.");
                        Console.WriteLine(help);
                        return false;
                    }
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
                BrukerRawFileConverter brukerWriter = new(file, outPath, topN, mzTolerancePpm, maxBufferedPeaks, writerCompressionLevel, outputFormat);
                brukerWriter.Write();
                return;
            }

            ThermoRawFileConverter writer = new(
                file,
                outPath,
                topN,
                mzTolerancePpm,
                maxBufferedPeaks,
                writerCompressionLevel,
                outputFormat,
                ifMergeScans);
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
            startInfo.ArgumentList.Add("--hdf5-compression-level");
            startInfo.ArgumentList.Add(writerCompressionLevel.ToString(CultureInfo.InvariantCulture));
            startInfo.ArgumentList.Add("--format");
            startInfo.ArgumentList.Add(OutputFormatArgument());
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
