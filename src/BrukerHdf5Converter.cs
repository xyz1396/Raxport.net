using System.Diagnostics;
using System.Globalization;
using System.IO.Compression;
using System.Runtime.InteropServices;

namespace Raxport;

internal sealed class BrukerHdf5Converter
{
    private const string RaxportVersion = "6.0";
    private readonly string inputPath;
    private readonly string outPath;
    private readonly int topNprecursor;
    private readonly double mzTolerancePpm;
    private readonly long maxBufferedPeaks;
    private readonly int hdf5CompressionLevel;
    private readonly double intensityThreshold;
    private readonly Dictionary<long, BrukerFrame> frames = new();
    private readonly Dictionary<long, IReadOnlyList<Hdf5PeakRecord>> parentPeaksByFrame = new();
    private readonly Dictionary<long, IReadOnlyList<double>> parentOneOverK0ByFrame = new();
    private readonly Dictionary<long, int> tdfParentUseCounts = new();
    private readonly List<BrukerFrame> ms1Frames = new();
    private long tdfMs1CentroidsWithoutTrace;
    private string? tempDirectory;

    public BrukerHdf5Converter(
        string inputPath,
        string outPath,
        int topNprecursor,
        double mzTolerancePpm,
        long maxBufferedPeaks,
        int hdf5CompressionLevel = Hdf5BufferDefaults.DefaultHdf5CompressionLevel,
        double intensityThreshold = 0.99)
    {
        this.inputPath = inputPath;
        this.outPath = outPath;
        this.topNprecursor = topNprecursor;
        this.mzTolerancePpm = mzTolerancePpm;
        this.maxBufferedPeaks = maxBufferedPeaks;
        this.hdf5CompressionLevel = hdf5CompressionLevel;
        this.intensityThreshold = intensityThreshold;
    }

    public void Write()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            throw new PlatformNotSupportedException("Bruker TIMS/TSF conversion is not available on macOS because ./dll does not include a macOS timsdata library.");
        }

        Stopwatch totalTimer = Stopwatch.StartNew();
        TimeSpan inputPrepareElapsed = TimeSpan.Zero;
        try
        {
            Stopwatch prepareTimer = Stopwatch.StartNew();
            string analysisDirectory = PrepareAnalysisDirectory(inputPath);
            prepareTimer.Stop();
            inputPrepareElapsed = prepareTimer.Elapsed;

            string outputFile = Path.Combine(outPath, GetOutputBaseName(inputPath) + ".h5");
            string? tsfPath = Path.Combine(analysisDirectory, "analysis.tsf");
            string? tdfPath = Path.Combine(analysisDirectory, "analysis.tdf");
            if (File.Exists(tsfPath))
            {
                WriteTsf(analysisDirectory, outputFile, totalTimer, inputPrepareElapsed);
            }
            else if (File.Exists(tdfPath))
            {
                WriteTdf(analysisDirectory, outputFile, totalTimer, inputPrepareElapsed);
            }
            else
            {
                throw new InvalidOperationException($"{inputPath}: no analysis.tsf or analysis.tdf file found.");
            }
        }
        finally
        {
            if (totalTimer.IsRunning)
            {
                totalTimer.Stop();
            }
            if (tempDirectory is not null)
            {
                Directory.Delete(tempDirectory, recursive: true);
            }
        }
    }

    private void WriteTsf(string analysisDirectory, string outputFile, Stopwatch totalTimer, TimeSpan inputPrepareElapsed)
    {
        using NativeSqliteConnection database = NativeSqliteConnection.Open(Path.Combine(analysisDirectory, "analysis.tsf"));
        string instrumentModel = ReadInstrumentModel(database);
        LoadTsfFrames(database);
        List<TsfMsMsInfo> ms2Rows = ReadTsfMsMsInfo(database);
        int ms1RowsPlanned = frames.Values.Count(frame => frame.MsMsType == 0);
        int ms2RowsPlanned = ms2Rows.Count;
        int precursorRowsPlanned = ms2Rows.Count;
        int ms1RowsWritten = 0;
        int ms2RowsWritten = 0;

        using BrukerTsfReader reader = new(analysisDirectory);
        using Hdf5BufferedWriter writer = CreateWriter(outputFile, instrumentModel);
        LogConversionStart(outputFile, "TSF AutoMSMS", frames.Count, ms1RowsPlanned, ms2RowsPlanned, precursorRowsPlanned);

        foreach (BrukerFrame frame in frames.Values.OrderBy(frame => frame.Id))
        {
            if (frame.MsMsType != 0)
            {
                continue;
            }

            List<Hdf5PeakRecord> peaks = reader.ReadLineSpectrum(frame.Id);
            parentPeaksByFrame[frame.Id] = peaks;
            ms1Frames.Add(frame);
            writer.AddScan(CreateMs1Scan(frame, peaks));
            ms1RowsWritten++;
        }

        long syntheticScanNumber = frames.Keys.DefaultIfEmpty(0).Max() + 1;
        foreach (TsfMsMsInfo ms2 in ms2Rows)
        {
            BrukerFrame frame = frames[ms2.FrameId];
            IReadOnlyList<Hdf5PeakRecord> parentPeaks = GetParentPeaks(ms2.ParentFrameId);
            List<Hdf5PeakRecord> evidencePeaks = PrecursorSelector.GetPrecursorEvidencePeaks(
                parentPeaks,
                ms2.TriggerMass,
                ms2.IsolationWidth,
                mzTolerancePpm);
            List<Hdf5PeakRecord> isotopeEvidencePeaks = PrecursorSelector.GetIsotopeEvidencePeaks(
                parentPeaks,
                ms2.TriggerMass,
                ms2.IsolationWidth,
                mzTolerancePpm);
            List<Hdf5PeakRecord> selectedPeaks = PrecursorSelector.FindPrecursorPeaksFromEvidence(
                evidencePeaks,
                isotopeEvidencePeaks,
                topNprecursor,
                intensityThreshold,
                mzTolerancePpm,
                preferredCharge: ms2.PrecursorCharge ?? 0);
            Hdf5ReactionRecord reaction = CreateReaction(
                ms2.TriggerMass,
                ms2.IsolationWidth,
                ms2.PrecursorCharge ?? 0,
                ms2.CollisionEnergy,
                "CID",
                selectedPeaks,
                isotopeEvidencePeaks);
            writer.AddScan(new Hdf5ScanRecord(
                checked((int)syntheticScanNumber++),
                2,
                frame.TimeSeconds / 60.0,
                frame.SummedIntensities,
                $"Bruker TSF MS2 frame={ms2.FrameId} parent={ms2.ParentFrameId}",
                "CID",
                checked((int)(ms2.ParentFrameId ?? 0)),
                reaction,
                reader.ReadLineSpectrum(frame.Id)));
            ms2RowsWritten++;
        }

        writer.Flush();
        writer.Dispose();
        totalTimer.Stop();
        LogConversionFinished(writer, "TSF AutoMSMS", ms1RowsWritten, ms2RowsWritten, totalTimer.Elapsed, inputPrepareElapsed);
    }

    private void WriteTdf(string analysisDirectory, string outputFile, Stopwatch totalTimer, TimeSpan inputPrepareElapsed)
    {
        using NativeSqliteConnection database = NativeSqliteConnection.Open(Path.Combine(analysisDirectory, "analysis.tdf"));
        string instrumentModel = ReadInstrumentModel(database);
        LoadTdfFrames(database);
        parentPeaksByFrame.Clear();
        parentOneOverK0ByFrame.Clear();
        tdfParentUseCounts.Clear();
        tdfMs1CentroidsWithoutTrace = 0;
        List<PasefMsMsInfo> pasefRows = ReadPasefMsMsInfo(database);
        List<DiaMsMsInfo> diaRows = ReadDiaMsMsInfo(database);
        ms1Frames.Clear();
        ms1Frames.AddRange(frames.Values.Where(frame => frame.MsMsType == 0).OrderBy(frame => frame.Id));
        int ms1RowsPlanned = ms1Frames.Count;
        int ms2RowsPlanned = pasefRows.Count + diaRows.Count;
        int precursorRowsPlanned = pasefRows.Count + diaRows.Count;
        int ms1RowsWritten = 0;
        int ms2RowsWritten = 0;

        using BrukerTimsReader reader = new(analysisDirectory);
        using Hdf5BufferedWriter writer = CreateWriter(outputFile, instrumentModel);
        LogConversionStart(outputFile, "TDF", frames.Count, ms1RowsPlanned, ms2RowsPlanned, precursorRowsPlanned);

        InitializeTdfParentUseCounts(pasefRows, diaRows);
        Dictionary<long, List<PasefMsMsInfo>> pasefRowsByFrame = pasefRows
            .GroupBy(row => row.FragmentFrameId)
            .ToDictionary(group => group.Key, group => group.OrderBy(row => row.ScanBegin).ToList());
        Dictionary<long, List<DiaMsMsInfo>> diaRowsByFrame = diaRows
            .GroupBy(row => row.FrameId)
            .ToDictionary(group => group.Key, group => group.OrderBy(row => row.ScanBegin).ToList());

        long syntheticScanNumber = frames.Keys.DefaultIfEmpty(0).Max() + 1;
        foreach (BrukerFrame frame in frames.Values.OrderBy(frame => frame.Id))
        {
            if (frame.MsMsType == 0)
            {
                IReadOnlyList<Hdf5PeakRecord> peaks = EnsureTdfMs1Peaks(reader, frame);
                writer.AddScan(CreateMs1Scan(frame, peaks));
                ms1RowsWritten++;
                ReleaseTdfParentPeaksIfUnused(frame.Id);
            }

            if (pasefRowsByFrame.TryGetValue(frame.Id, out List<PasefMsMsInfo>? framePasefRows))
            {
                reader.ReadPasefMsMsBatch(framePasefRows.Select(row => row.PrecursorId).ToArray());
                foreach (PasefMsMsInfo ms2 in framePasefRows)
                {
                    syntheticScanNumber = WritePasefScan(reader, writer, ms2, syntheticScanNumber);
                    ms2RowsWritten++;
                }
            }

            if (diaRowsByFrame.TryGetValue(frame.Id, out List<DiaMsMsInfo>? frameDiaRows))
            {
                foreach (DiaMsMsInfo ms2 in frameDiaRows)
                {
                    syntheticScanNumber = WriteDiaScan(reader, writer, ms2, syntheticScanNumber);
                    ms2RowsWritten++;
                }
            }
        }

        writer.Flush();
        if (tdfMs1CentroidsWithoutTrace > 0)
        {
            Console.WriteLine($"TDF MS1 centroids without mobility trace omitted : {tdfMs1CentroidsWithoutTrace:N0}");
        }
        writer.Dispose();
        totalTimer.Stop();
        LogConversionFinished(writer, "TDF", ms1RowsWritten, ms2RowsWritten, totalTimer.Elapsed, inputPrepareElapsed);
    }

    private Hdf5BufferedWriter CreateWriter(string outputFile, string instrumentModel)
    {
        return new Hdf5BufferedWriter(outputFile, inputPath, instrumentModel, RaxportVersion, maxBufferedPeaks, hdf5CompressionLevel);
    }

    private static Hdf5ScanRecord CreateMs1Scan(BrukerFrame frame, IReadOnlyList<Hdf5PeakRecord> peaks)
    {
        return new Hdf5ScanRecord(
            checked((int)frame.Id),
            1,
            frame.TimeSeconds / 60.0,
            frame.SummedIntensities,
            $"Bruker MS1 frame={frame.Id}",
            string.Empty,
            0,
            null,
            peaks);
    }

    private long WritePasefScan(
        BrukerTimsReader reader,
        Hdf5BufferedWriter writer,
        PasefMsMsInfo ms2,
        long syntheticScanNumber)
    {
        (double oneOverK0Begin, double oneOverK0End) = reader.ScanRangeToOneOverK0Range(ms2.FragmentFrameId, ms2.ScanBegin, ms2.ScanEnd);
        IReadOnlyList<Hdf5PeakRecord> parentPeaks = GetTdfParentPeaks(reader, ms2.ParentFrameId);
        IReadOnlyList<double> parentOneOverK0Axis = GetTdfParentOneOverK0Axis(reader, ms2.ParentFrameId);
        List<Hdf5PeakRecord> evidencePeaks = PrecursorSelector.GetPrecursorEvidencePeaks(
            parentPeaks,
            ms2.IsolationMz,
            ms2.IsolationWidth,
            mzTolerancePpm,
            oneOverK0Begin,
            oneOverK0End,
            parentOneOverK0Axis);
        List<Hdf5PeakRecord> isotopeEvidencePeaks = PrecursorSelector.GetIsotopeEvidencePeaks(
            parentPeaks,
            ms2.IsolationMz,
            ms2.IsolationWidth,
            mzTolerancePpm,
            oneOverK0Begin,
            oneOverK0End,
            parentOneOverK0Axis);
        List<Hdf5PeakRecord> selectedPeaks = PrecursorSelector.FindPrecursorPeaksFromEvidence(
            evidencePeaks,
            isotopeEvidencePeaks,
            topNprecursor,
            intensityThreshold,
            mzTolerancePpm,
            preferredCharge: ms2.Charge ?? 0);
        Hdf5ReactionRecord reaction = CreateReaction(
            ms2.IsolationMz,
            ms2.IsolationWidth,
            ms2.Charge ?? 0,
            ms2.CollisionEnergy,
            "CID",
            selectedPeaks,
            isotopeEvidencePeaks,
            oneOverK0Begin,
            oneOverK0End);
        List<Hdf5PeakRecord> fragmentPeaks = reader.ReadPasefMsMs(ms2.PrecursorId);
        writer.AddScan(new Hdf5ScanRecord(
            checked((int)syntheticScanNumber),
            2,
            ms2.FragmentTimeSeconds / 60.0,
            0,
            $"Bruker TDF PASEF precursor={ms2.PrecursorId} frame={ms2.FragmentFrameId} scan={ms2.ScanBegin}-{ms2.ScanEnd}",
            "CID",
            checked((int)(ms2.ParentFrameId ?? 0)),
            reaction,
            fragmentPeaks));
        reader.ReleasePasefMsMs(ms2.PrecursorId);
        ReleaseTdfParentPeaks(ms2.ParentFrameId);
        return syntheticScanNumber + 1;
    }

    private long WriteDiaScan(
        BrukerTimsReader reader,
        Hdf5BufferedWriter writer,
        DiaMsMsInfo ms2,
        long syntheticScanNumber)
    {
        BrukerFrame frame = frames[ms2.FrameId];
        long? parentFrameId = FindNearestMs1Frame(frame.TimeSeconds);
        (double oneOverK0Begin, double oneOverK0End) = reader.ScanRangeToOneOverK0Range(ms2.FrameId, ms2.ScanBegin, ms2.ScanEnd);
        IReadOnlyList<Hdf5PeakRecord> parentPeaks = GetTdfParentPeaks(reader, parentFrameId);
        IReadOnlyList<double> parentOneOverK0Axis = GetTdfParentOneOverK0Axis(reader, parentFrameId);
        List<Hdf5PeakRecord> evidencePeaks = PrecursorSelector.GetPrecursorEvidencePeaks(
            parentPeaks,
            ms2.IsolationMz,
            ms2.IsolationWidth,
            mzTolerancePpm,
            oneOverK0Begin,
            oneOverK0End,
            parentOneOverK0Axis);
        List<Hdf5PeakRecord> isotopeEvidencePeaks = PrecursorSelector.GetIsotopeEvidencePeaks(
            parentPeaks,
            ms2.IsolationMz,
            ms2.IsolationWidth,
            mzTolerancePpm,
            oneOverK0Begin,
            oneOverK0End,
            parentOneOverK0Axis);
        List<Hdf5PeakRecord> selectedPeaks = PrecursorSelector.FindPrecursorPeaksFromEvidence(
            evidencePeaks,
            isotopeEvidencePeaks,
            topNprecursor,
            intensityThreshold,
            mzTolerancePpm,
            preferredCharge: 0);
        Hdf5ReactionRecord reaction = CreateReaction(
            ms2.IsolationMz,
            ms2.IsolationWidth,
            0,
            ms2.CollisionEnergy,
            "CID",
            selectedPeaks,
            isotopeEvidencePeaks,
            oneOverK0Begin,
            oneOverK0End);
        writer.AddScan(new Hdf5ScanRecord(
            checked((int)syntheticScanNumber),
            2,
            frame.TimeSeconds / 60.0,
            0,
            $"Bruker TDF DIA frame={ms2.FrameId} windowGroup={ms2.WindowGroup} scan={ms2.ScanBegin}-{ms2.ScanEnd}",
            "CID",
            checked((int)(parentFrameId ?? 0)),
            reaction,
            reader.ExtractCentroidedFrame(ms2.FrameId, checked((uint)ms2.ScanBegin), checked((uint)ms2.ScanEnd))));
        ReleaseTdfParentPeaks(parentFrameId);
        return syntheticScanNumber + 1;
    }

    private Hdf5ReactionRecord CreateReaction(
        double precursorMass,
        double isolationWidth,
        int chargeState,
        double collisionEnergy,
        string activation,
        IReadOnlyList<Hdf5PeakRecord> precursorPeaks,
        IReadOnlyList<Hdf5PeakRecord> evidencePeaks,
        double oneOverK0Begin = 0,
        double oneOverK0End = 0)
    {
        return new Hdf5ReactionRecord(
            precursorMass,
            isolationWidth,
            chargeState,
            collisionEnergy,
            true,
            activation,
            false,
            false,
            0,
            0,
            0,
            PrecursorSelector.ExpandPrecursorCandidates(
                precursorPeaks,
                evidencePeaks,
                precursorMass,
                isolationWidth,
                topNprecursor,
                mzTolerancePpm,
                chargeState),
            oneOverK0Begin,
            oneOverK0End);
    }

    private IReadOnlyList<Hdf5PeakRecord> GetParentPeaks(long? parentFrameId)
    {
        if (parentFrameId is null || !parentPeaksByFrame.TryGetValue(parentFrameId.Value, out IReadOnlyList<Hdf5PeakRecord>? peaks))
        {
            return Array.Empty<Hdf5PeakRecord>();
        }

        return peaks;
    }

    private IReadOnlyList<Hdf5PeakRecord> GetTdfParentPeaks(BrukerTimsReader reader, long? parentFrameId)
    {
        if (parentFrameId is null || !frames.TryGetValue(parentFrameId.Value, out BrukerFrame? frame))
        {
            return Array.Empty<Hdf5PeakRecord>();
        }

        return EnsureTdfMs1Peaks(reader, frame);
    }

    private IReadOnlyList<Hdf5PeakRecord> EnsureTdfMs1Peaks(BrukerTimsReader reader, BrukerFrame frame)
    {
        if (parentPeaksByFrame.TryGetValue(frame.Id, out IReadOnlyList<Hdf5PeakRecord>? peaks))
        {
            return peaks;
        }

        List<Hdf5PeakRecord> readPeaks = reader.ReadMs1FrameWithMobilityTraces(
            frame.Id,
            frame.NumScans,
            mzTolerancePpm,
            out int omittedCentroids);
        tdfMs1CentroidsWithoutTrace += omittedCentroids;
        parentPeaksByFrame[frame.Id] = readPeaks;
        return readPeaks;
    }

    private IReadOnlyList<double> GetTdfParentOneOverK0Axis(BrukerTimsReader reader, long? parentFrameId)
    {
        if (parentFrameId is null || !frames.TryGetValue(parentFrameId.Value, out BrukerFrame? frame))
        {
            return Array.Empty<double>();
        }

        if (parentOneOverK0ByFrame.TryGetValue(frame.Id, out IReadOnlyList<double>? oneOverK0Axis))
        {
            return oneOverK0Axis;
        }

        double[] readAxis = reader.GetOneOverK0Axis(frame.Id, frame.NumScans);
        parentOneOverK0ByFrame[frame.Id] = readAxis;
        return readAxis;
    }

    private void InitializeTdfParentUseCounts(IEnumerable<PasefMsMsInfo> pasefRows, IEnumerable<DiaMsMsInfo> diaRows)
    {
        foreach (PasefMsMsInfo row in pasefRows)
        {
            AddTdfParentUse(row.ParentFrameId);
        }

        foreach (DiaMsMsInfo row in diaRows)
        {
            if (frames.TryGetValue(row.FrameId, out BrukerFrame? frame))
            {
                AddTdfParentUse(FindNearestMs1Frame(frame.TimeSeconds));
            }
        }
    }

    private void AddTdfParentUse(long? parentFrameId)
    {
        if (parentFrameId is null)
        {
            return;
        }

        tdfParentUseCounts.TryGetValue(parentFrameId.Value, out int count);
        tdfParentUseCounts[parentFrameId.Value] = count + 1;
    }

    private void ReleaseTdfParentPeaks(long? parentFrameId)
    {
        if (parentFrameId is null || !tdfParentUseCounts.TryGetValue(parentFrameId.Value, out int count))
        {
            return;
        }

        if (count <= 1)
        {
            tdfParentUseCounts.Remove(parentFrameId.Value);
            parentPeaksByFrame.Remove(parentFrameId.Value);
            parentOneOverK0ByFrame.Remove(parentFrameId.Value);
        }
        else
        {
            tdfParentUseCounts[parentFrameId.Value] = count - 1;
        }
    }

    private void ReleaseTdfParentPeaksIfUnused(long frameId)
    {
        if (!tdfParentUseCounts.ContainsKey(frameId))
        {
            parentPeaksByFrame.Remove(frameId);
            parentOneOverK0ByFrame.Remove(frameId);
        }
    }

    private long? FindNearestMs1Frame(double timeSeconds)
    {
        if (ms1Frames.Count == 0)
        {
            return null;
        }

        int low = 0;
        int high = ms1Frames.Count - 1;
        while (low <= high)
        {
            int mid = low + ((high - low) / 2);
            if (ms1Frames[mid].TimeSeconds < timeSeconds)
            {
                low = mid + 1;
            }
            else
            {
                high = mid - 1;
            }
        }

        int rightIndex = low < ms1Frames.Count ? low : ms1Frames.Count - 1;
        int leftIndex = rightIndex > 0 ? rightIndex - 1 : rightIndex;
        BrukerFrame nearest = ms1Frames[leftIndex];
        if (rightIndex != leftIndex &&
            Math.Abs(ms1Frames[rightIndex].TimeSeconds - timeSeconds) < Math.Abs(nearest.TimeSeconds - timeSeconds))
        {
            nearest = ms1Frames[rightIndex];
        }

        return nearest.Id;
    }

    private void LoadTsfFrames(NativeSqliteConnection database)
    {
        frames.Clear();
        foreach (SqliteRow row in database.Query("SELECT Id, Time, MsMsType, SummedIntensities, COALESCE(NumPeaks, 0) FROM Frames ORDER BY Id"))
        {
            BrukerFrame frame = new(
                row.GetInt64(0),
                row.GetDouble(1),
                row.GetInt32(2),
                row.GetDouble(3),
                0,
                row.GetInt32(4));
            frames[frame.Id] = frame;
        }
    }

    private void LoadTdfFrames(NativeSqliteConnection database)
    {
        frames.Clear();
        foreach (SqliteRow row in database.Query("SELECT Id, Time, MsMsType, SummedIntensities, NumScans, NumPeaks FROM Frames ORDER BY Id"))
        {
            BrukerFrame frame = new(
                row.GetInt64(0),
                row.GetDouble(1),
                row.GetInt32(2),
                row.GetDouble(3),
                row.GetInt32(4),
                row.GetInt32(5));
            frames[frame.Id] = frame;
        }
    }

    private static string ReadInstrumentModel(NativeSqliteConnection database)
    {
        foreach (SqliteRow row in database.Query("SELECT Value FROM GlobalMetadata WHERE Key = 'InstrumentName' LIMIT 1"))
        {
            return row.GetString(0);
        }

        return "Bruker timsTOF";
    }

    private static List<TsfMsMsInfo> ReadTsfMsMsInfo(NativeSqliteConnection database)
    {
        List<TsfMsMsInfo> rows = new();
        foreach (SqliteRow row in database.Query("SELECT Frame, Parent, TriggerMass, IsolationWidth, PrecursorCharge, CollisionEnergy FROM FrameMsMsInfo ORDER BY Frame"))
        {
            rows.Add(new TsfMsMsInfo(
                row.GetInt64(0),
                row.GetNullableInt64(1),
                row.GetDouble(2),
                row.GetDouble(3),
                row.GetNullableInt32(4),
                row.GetDouble(5)));
        }

        return rows;
    }

    private List<PasefMsMsInfo> ReadPasefMsMsInfo(NativeSqliteConnection database)
    {
        if (!TableExists(database, "PasefFrameMsMsInfo") || !TableExists(database, "Precursors"))
        {
            return new List<PasefMsMsInfo>();
        }

        Dictionary<long, PasefMsMsInfo> byPrecursor = new();
        string sql =
            "SELECT p.Id, p.Charge, p.Parent, m.Frame, f.Time, m.ScanNumBegin, m.ScanNumEnd, " +
            "m.IsolationMz, m.IsolationWidth, m.CollisionEnergy " +
            "FROM PasefFrameMsMsInfo m " +
            "JOIN Precursors p ON p.Id = m.Precursor " +
            "JOIN Frames f ON f.Id = m.Frame " +
            "ORDER BY p.Id, m.Frame, m.ScanNumBegin";
        foreach (SqliteRow row in database.Query(sql))
        {
            long precursorId = row.GetInt64(0);
            if (byPrecursor.ContainsKey(precursorId))
            {
                continue;
            }

            byPrecursor.Add(precursorId, new PasefMsMsInfo(
                precursorId,
                row.GetNullableInt32(1),
                row.GetNullableInt64(2),
                row.GetInt64(3),
                row.GetDouble(4),
                row.GetInt32(5),
                row.GetInt32(6),
                row.GetDouble(7),
                row.GetDouble(8),
                row.GetDouble(9)));
        }

        return byPrecursor.Values.OrderBy(row => row.FragmentFrameId).ThenBy(row => row.ScanBegin).ToList();
    }

    private static List<DiaMsMsInfo> ReadDiaMsMsInfo(NativeSqliteConnection database)
    {
        if (!TableExists(database, "DiaFrameMsMsInfo") || !TableExists(database, "DiaFrameMsMsWindows"))
        {
            return new List<DiaMsMsInfo>();
        }

        List<DiaMsMsInfo> rows = new();
        string sql =
            "SELECT i.Frame, i.WindowGroup, w.ScanNumBegin, w.ScanNumEnd, w.IsolationMz, " +
            "w.IsolationWidth, w.CollisionEnergy " +
            "FROM DiaFrameMsMsInfo i " +
            "JOIN DiaFrameMsMsWindows w ON w.WindowGroup = i.WindowGroup " +
            "ORDER BY i.Frame, w.ScanNumBegin";
        foreach (SqliteRow row in database.Query(sql))
        {
            rows.Add(new DiaMsMsInfo(
                row.GetInt64(0),
                row.GetInt64(1),
                row.GetInt32(2),
                row.GetInt32(3),
                row.GetDouble(4),
                row.GetDouble(5),
                row.GetDouble(6)));
        }

        return rows;
    }

    private static bool TableExists(NativeSqliteConnection database, string tableName)
    {
        string escapedTableName = tableName.Replace("'", "''");
        foreach (SqliteRow _ in database.Query($"SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '{escapedTableName}' LIMIT 1"))
        {
            return true;
        }

        return false;
    }

    private string PrepareAnalysisDirectory(string path)
    {
        if (Directory.Exists(path))
        {
            return FindAnalysisDirectory(path);
        }

        if (File.Exists(path) && path.EndsWith(".d.zip", StringComparison.OrdinalIgnoreCase))
        {
            tempDirectory = Path.Combine(Path.GetTempPath(), $"raxport-bruker-{Guid.NewGuid():N}");
            ZipFile.ExtractToDirectory(path, tempDirectory);
            return FindAnalysisDirectory(tempDirectory);
        }

        throw new InvalidOperationException($"{path}: expected a Bruker .d directory or .d.zip archive.");
    }

    private static string FindAnalysisDirectory(string root)
    {
        foreach (string file in Directory.EnumerateFiles(root, "analysis.tsf", SearchOption.AllDirectories))
        {
            return Path.GetDirectoryName(file)!;
        }

        foreach (string file in Directory.EnumerateFiles(root, "analysis.tdf", SearchOption.AllDirectories))
        {
            return Path.GetDirectoryName(file)!;
        }

        throw new InvalidOperationException($"{root}: no analysis.tsf or analysis.tdf file found.");
    }

    public static string GetOutputBaseName(string path)
    {
        string name = Directory.Exists(path)
            ? new DirectoryInfo(path).Name
            : Path.GetFileName(path);
        if (name.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
        {
            name = Path.GetFileNameWithoutExtension(name);
        }

        if (name.EndsWith(".d", StringComparison.OrdinalIgnoreCase))
        {
            name = name[..^2];
        }

        return name;
    }

    private void LogConversionStart(
        string outputFile,
        string format,
        int frameCount,
        int ms1Count,
        int ms2Count,
        int precursorCount)
    {
        Console.WriteLine();
        Console.WriteLine("============================================================");
        Console.WriteLine("Raxport Bruker HDF5 conversion started");
        Console.WriteLine("------------------------------------------------------------");
        Console.WriteLine($"Input Bruker data   : {inputPath}");
        Console.WriteLine($"Output HDF5 file    : {outputFile}");
        Console.WriteLine($"Bruker format       : {format}");
        Console.WriteLine($"Frame rows          : {frameCount:N0}");
        Console.WriteLine($"MS1 rows planned    : {ms1Count:N0}");
        Console.WriteLine($"MS2 rows planned    : {ms2Count:N0}");
        Console.WriteLine($"Precursors planned  : {precursorCount:N0}");
        Console.WriteLine($"Peak flush limit    : {maxBufferedPeaks:N0} peaks");
        Console.WriteLine($"HDF5 compression    : gzip level {hdf5CompressionLevel:N0}");
        Console.WriteLine($"Top precursor count : {topNprecursor:N0}");
        Console.WriteLine($"m/z tolerance       : {mzTolerancePpm:0.###} ppm");
        Console.WriteLine("============================================================");
    }

    private static void LogConversionFinished(
        Hdf5BufferedWriter writer,
        string format,
        int ms1RowsWritten,
        int ms2RowsWritten,
        TimeSpan totalElapsed,
        TimeSpan inputPrepareElapsed)
    {
        TimeSpan hdfElapsed = writer.HdfWriteElapsed;
        TimeSpan readAndRamElapsed = totalElapsed - inputPrepareElapsed - hdfElapsed;
        if (readAndRamElapsed < TimeSpan.Zero)
        {
            readAndRamElapsed = TimeSpan.Zero;
        }

        Console.WriteLine();
        Console.WriteLine("============================================================");
        Console.WriteLine("Raxport Bruker HDF5 conversion finished");
        Console.WriteLine("------------------------------------------------------------");
        Console.WriteLine($"Bruker format               : {format}");
        Console.WriteLine($"HDF5 scan rows written      : {writer.TotalScans:N0}");
        Console.WriteLine($"HDF5 MS1 scan rows written  : {ms1RowsWritten:N0}");
        Console.WriteLine($"HDF5 MS2 scan rows written  : {ms2RowsWritten:N0}");
        Console.WriteLine($"HDF5 peak rows written      : {writer.TotalPeaks:N0}");
        Console.WriteLine($"HDF5 reaction rows written  : {writer.TotalReactions:N0}");
        Console.WriteLine($"HDF5 precursor candidates   : {writer.TotalCandidates:N0}");
        Console.WriteLine($"HDF5 flush count            : {writer.FlushCount:N0}");
        Console.WriteLine("------------------------------------------------------------");
        Console.WriteLine($"Input prepare               : {FormatElapsed(inputPrepareElapsed)} ({Percent(inputPrepareElapsed, totalElapsed):0.0}%)");
        Console.WriteLine($"Bruker read + RAM buffering : {FormatElapsed(readAndRamElapsed)} ({Percent(readAndRamElapsed, totalElapsed):0.0}%)");
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

    private sealed record BrukerFrame(
        long Id,
        double TimeSeconds,
        int MsMsType,
        double SummedIntensities,
        int NumScans,
        int NumPeaks);

    private sealed record TsfMsMsInfo(
        long FrameId,
        long? ParentFrameId,
        double TriggerMass,
        double IsolationWidth,
        int? PrecursorCharge,
        double CollisionEnergy);

    private sealed record PasefMsMsInfo(
        long PrecursorId,
        int? Charge,
        long? ParentFrameId,
        long FragmentFrameId,
        double FragmentTimeSeconds,
        int ScanBegin,
        int ScanEnd,
        double IsolationMz,
        double IsolationWidth,
        double CollisionEnergy);

    private sealed record DiaMsMsInfo(
        long FrameId,
        long WindowGroup,
        int ScanBegin,
        int ScanEnd,
        double IsolationMz,
        double IsolationWidth,
        double CollisionEnergy);
}

internal sealed class BrukerTsfReader : IDisposable
{
    private ulong handle;
    private int lineBufferSize = 4096;

    public BrukerTsfReader(string analysisDirectory)
    {
        handle = BrukerNative.TsfOpen(analysisDirectory, 0);
        if (handle == 0)
        {
            throw new InvalidOperationException($"Unable to open TSF data: {BrukerNative.GetTsfLastError()}");
        }
    }

    public List<Hdf5PeakRecord> ReadLineSpectrum(long frameId)
    {
        double[] indices;
        float[] intensities;
        int requiredLength;
        while (true)
        {
            indices = new double[lineBufferSize];
            intensities = new float[lineBufferSize];
            requiredLength = BrukerNative.TsfReadLineSpectrum(handle, frameId, indices, intensities, lineBufferSize);
            if (requiredLength < 0)
            {
                throw new InvalidOperationException($"Unable to read TSF spectrum {frameId}: {BrukerNative.GetTsfLastError()}");
            }

            if (requiredLength <= lineBufferSize)
            {
                break;
            }

            lineBufferSize = requiredLength;
        }

        double[] mz = new double[requiredLength];
        double[] indexSlice = new double[requiredLength];
        Array.Copy(indices, indexSlice, requiredLength);
        if (requiredLength > 0 && BrukerNative.TsfIndexToMz(handle, frameId, indexSlice, mz, checked((uint)requiredLength)) == 0)
        {
            throw new InvalidOperationException($"Unable to convert TSF indices to m/z for spectrum {frameId}: {BrukerNative.GetTsfLastError()}");
        }

        List<Hdf5PeakRecord> peaks = new(requiredLength);
        for (int i = 0; i < requiredLength; i++)
        {
            peaks.Add(new Hdf5PeakRecord(mz[i], intensities[i], 0, 0, 0, 0));
        }

        return peaks.OrderBy(peak => peak.Mz).ToList();
    }

    public void Dispose()
    {
        if (handle != 0)
        {
            BrukerNative.TsfClose(handle);
            handle = 0;
        }
    }
}

internal sealed class BrukerTimsReader : IDisposable
{
    private const int InitialScanBufferUInt32Length = 4096;
    private readonly BrukerNative.MsMsSpectrumCallback spectrumCallback;
    private readonly Dictionary<long, List<Hdf5PeakRecord>> pasefCache = new();
    private int scanBufferUInt32Length = InitialScanBufferUInt32Length;
    private ulong handle;

    public BrukerTimsReader(string analysisDirectory)
    {
        handle = BrukerNative.TimsOpenV2(analysisDirectory, 0, 0);
        if (handle == 0)
        {
            throw new InvalidOperationException($"Unable to open TDF data: {BrukerNative.GetTimsLastError()}");
        }

        spectrumCallback = OnSpectrum;
    }

    public List<Hdf5PeakRecord> ExtractCentroidedFrame(long frameId, uint scanBegin, uint scanEnd)
    {
        List<Hdf5PeakRecord>? result = null;
        void Callback(long id, uint numPeaks, IntPtr mzValues, IntPtr areaValues, IntPtr userData)
        {
            _ = id;
            _ = userData;
            result = CopyPeaks(numPeaks, mzValues, areaValues);
        }

        BrukerNative.MsMsSpectrumCallback callback = Callback;
        uint rc = BrukerNative.TimsExtractCentroidedSpectrumForFrameV3(handle, frameId, scanBegin, scanEnd, callback, IntPtr.Zero);
        if (rc == 0)
        {
            throw new InvalidOperationException($"Unable to extract TDF frame {frameId}: {BrukerNative.GetTimsLastError()}");
        }

        return result ?? new List<Hdf5PeakRecord>();
    }

    public List<Hdf5PeakRecord> ReadMs1FrameWithMobilityTraces(long frameId, int numScans, double mzTolerancePpm, out int omittedCentroids)
    {
        List<Hdf5PeakRecord> centroids = ExtractCentroidedFrame(frameId, 0, checked((uint)numScans));
        omittedCentroids = 0;
        if (centroids.Count == 0 || numScans <= 0)
        {
            return centroids;
        }

        IReadOnlyList<BrukerRawScan> rawScans = ReadRawScans(frameId, 0, numScans);
        double[] oneOverK0ByIndex = GetOneOverK0Axis(frameId, numScans);

        List<double[]> mzByScan = new(rawScans.Count);
        foreach (BrukerRawScan scan in rawScans)
        {
            double[] indexValues = Array.ConvertAll(scan.Indices, value => (double)value);
            double[] mzValues = new double[indexValues.Length];
            if (mzValues.Length > 0)
            {
                uint rc = BrukerNative.TimsIndexToMz(handle, frameId, indexValues, mzValues, checked((uint)mzValues.Length));
                if (rc == 0)
                {
                    throw new InvalidOperationException($"Unable to convert TDF indices to m/z for frame {frameId}: {BrukerNative.GetTimsLastError()}");
                }
            }
            mzByScan.Add(mzValues);
        }

        return BuildMobilityTracePeaks(centroids, rawScans, mzByScan, oneOverK0ByIndex, mzTolerancePpm, out omittedCentroids);
    }

    public double[] GetOneOverK0Axis(long frameId, int numScans)
    {
        if (numScans < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(numScans), "Scan count cannot be negative.");
        }

        double[] scanNumbers = new double[numScans];
        for (int i = 0; i < scanNumbers.Length; i++)
        {
            scanNumbers[i] = i;
        }

        double[] oneOverK0 = new double[numScans];
        if (scanNumbers.Length > 0)
        {
            uint rc = BrukerNative.TimsScanNumToOneOverK0(handle, frameId, scanNumbers, oneOverK0, checked((uint)scanNumbers.Length));
            if (rc == 0)
            {
                throw new InvalidOperationException($"Unable to convert TDF scan numbers to 1/K0 for frame {frameId}: {BrukerNative.GetTimsLastError()}");
            }
        }

        return oneOverK0;
    }

    public (double Begin, double End) ScanRangeToOneOverK0Range(long frameId, int scanBegin, int scanEnd)
    {
        double[] scanNumbers = { scanBegin, scanEnd };
        double[] oneOverK0 = new double[2];
        uint rc = BrukerNative.TimsScanNumToOneOverK0(handle, frameId, scanNumbers, oneOverK0, 2);
        if (rc == 0)
        {
            throw new InvalidOperationException($"Unable to convert TDF scan range to 1/K0 for frame {frameId}: {BrukerNative.GetTimsLastError()}");
        }

        return (oneOverK0[0], oneOverK0[1]);
    }

    public void ReadPasefMsMsBatch(IReadOnlyList<long> precursorIds)
    {
        List<long> missingPrecursors = new();
        foreach (long precursorId in precursorIds)
        {
            if (!pasefCache.ContainsKey(precursorId))
            {
                missingPrecursors.Add(precursorId);
            }
        }

        if (missingPrecursors.Count == 0)
        {
            return;
        }

        long[] precursors = missingPrecursors.ToArray();
        uint rc = BrukerNative.TimsReadPasefMsMsV2(handle, precursors, checked((uint)precursors.Length), spectrumCallback, IntPtr.Zero);
        if (rc == 0)
        {
            throw new InvalidOperationException($"Unable to read {precursors.Length:N0} PASEF precursor spectra: {BrukerNative.GetTimsLastError()}");
        }
    }

    public List<Hdf5PeakRecord> ReadPasefMsMs(long precursorId)
    {
        if (!pasefCache.TryGetValue(precursorId, out List<Hdf5PeakRecord>? peaks))
        {
            ReadPasefMsMsBatch(new[] { precursorId });
        }

        return pasefCache.TryGetValue(precursorId, out peaks) ? peaks : new List<Hdf5PeakRecord>();
    }

    public void ReleasePasefMsMs(long precursorId)
    {
        pasefCache.Remove(precursorId);
    }

    public void Dispose()
    {
        if (handle != 0)
        {
            BrukerNative.TimsClose(handle);
            handle = 0;
        }
    }

    internal static IReadOnlyList<BrukerRawScan> ParseRawScanBuffer(uint[] buffer, int scanBegin, int scanEnd)
    {
        int scanCount = scanEnd - scanBegin;
        if (scanCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(scanEnd), "Scan end must be greater than or equal to scan begin.");
        }
        if (buffer.Length < scanCount)
        {
            throw new ArgumentException("Raw scan buffer is shorter than the scan count header.", nameof(buffer));
        }

        List<BrukerRawScan> scans = new(scanCount);
        int offset = scanCount;
        for (int i = 0; i < scanCount; i++)
        {
            int peakCount = checked((int)buffer[i]);
            if (offset + peakCount * 2 > buffer.Length)
            {
                throw new ArgumentException("Raw scan buffer is shorter than the encoded peak arrays.", nameof(buffer));
            }

            uint[] indices = new uint[peakCount];
            Array.Copy(buffer, offset, indices, 0, peakCount);
            offset += peakCount;
            uint[] intensities = new uint[peakCount];
            Array.Copy(buffer, offset, intensities, 0, peakCount);
            offset += peakCount;
            scans.Add(new BrukerRawScan(scanBegin + i, indices, intensities));
        }

        return scans;
    }


    internal static List<Hdf5PeakRecord> BuildMobilityTracePeaks(
        IReadOnlyList<Hdf5PeakRecord> centroids,
        IReadOnlyList<BrukerRawScan> rawScans,
        IReadOnlyList<double[]> mzByScan,
        IReadOnlyList<double> oneOverK0ByIndex,
        double mzTolerancePpm,
        out int omittedCentroids)
    {
        if (rawScans.Count != mzByScan.Count)
        {
            throw new ArgumentException("Raw scans and m/z arrays must have matching counts.");
        }

        Dictionary<long, double> traceIntensityByCentroidAndMobility = new();
        for (int scanIndex = 0; scanIndex < rawScans.Count; scanIndex++)
        {
            BrukerRawScan scan = rawScans[scanIndex];
            double[] mzValues = mzByScan[scanIndex];
            if (scan.Intensities.Length != mzValues.Length)
            {
                throw new ArgumentException("Each scan's intensity and m/z arrays must have matching lengths.");
            }

            int mobilityIndex = scan.ScanNumber;
            if ((uint)mobilityIndex >= (uint)oneOverK0ByIndex.Count)
            {
                throw new ArgumentException("Raw scan number is outside the supplied 1/K0 axis.");
            }

            for (int peakIndex = 0; peakIndex < mzValues.Length; peakIndex++)
            {
                uint intensity = scan.Intensities[peakIndex];
                if (intensity == 0)
                {
                    continue;
                }

                int centroidIndex = FindNearestCentroid(centroids, mzValues[peakIndex], mzTolerancePpm);
                if (centroidIndex < 0)
                {
                    continue;
                }

                long key = PackTraceKey(centroidIndex, mobilityIndex);
                traceIntensityByCentroidAndMobility.TryGetValue(key, out double existingIntensity);
                traceIntensityByCentroidAndMobility[key] = existingIntensity + intensity;
            }
        }

        List<MobilityTracePoint>?[] tracePointsByCentroid = new List<MobilityTracePoint>?[centroids.Count];
        foreach (KeyValuePair<long, double> pair in traceIntensityByCentroidAndMobility)
        {
            if (pair.Value <= 0)
            {
                continue;
            }

            int centroidIndex = TraceKeyCentroidIndex(pair.Key);
            int oneOverK0Index = TraceKeyOneOverK0Index(pair.Key);
            List<MobilityTracePoint> tracePoints = tracePointsByCentroid[centroidIndex] ??= new List<MobilityTracePoint>();
            tracePoints.Add(new MobilityTracePoint(oneOverK0Index, (float)pair.Value));
        }

        omittedCentroids = 0;
        List<Hdf5PeakRecord> peaks = new(centroids.Count);
        for (int centroidIndex = 0; centroidIndex < centroids.Count; centroidIndex++)
        {
            Hdf5PeakRecord centroid = centroids[centroidIndex];
            if (tracePointsByCentroid[centroidIndex] is not { Count: > 0 } tracePoints)
            {
                omittedCentroids++;
                continue;
            }

            tracePoints.Sort(static (left, right) => left.OneOverK0Index.CompareTo(right.OneOverK0Index));
            int[] oneOverK0Indices = new int[tracePoints.Count];
            float[] intensities = new float[tracePoints.Count];
            for (int i = 0; i < tracePoints.Count; i++)
            {
                oneOverK0Indices[i] = tracePoints[i].OneOverK0Index;
                intensities[i] = tracePoints[i].Intensity;
            }

            peaks.Add(centroid with { MobilityTrace = new Hdf5PeakMobilityTrace(oneOverK0Indices, intensities) });
        }

        return peaks;
    }


    private readonly record struct MobilityTracePoint(int OneOverK0Index, float Intensity);

    private static long PackTraceKey(int centroidIndex, int oneOverK0Index)
    {
        return ((long)centroidIndex << 32) | (uint)oneOverK0Index;
    }

    private static int TraceKeyCentroidIndex(long key)
    {
        return (int)(key >> 32);
    }

    private static int TraceKeyOneOverK0Index(long key)
    {
        return (int)key;
    }

    private IReadOnlyList<BrukerRawScan> ReadRawScans(long frameId, int scanBegin, int scanEnd)
    {
        while (true)
        {
            uint[] buffer = new uint[scanBufferUInt32Length];
            uint requiredBytes = BrukerNative.TimsReadScansV2(
                handle,
                frameId,
                checked((uint)scanBegin),
                checked((uint)scanEnd),
                buffer,
                checked((uint)(buffer.Length * sizeof(uint))));
            if (requiredBytes == 0)
            {
                throw new InvalidOperationException($"Unable to read TDF scans for frame {frameId}: {BrukerNative.GetTimsLastError()}");
            }

            if (requiredBytes <= buffer.Length * sizeof(uint))
            {
                int usedUInt32Length = checked((int)((requiredBytes + sizeof(uint) - 1) / sizeof(uint)));
                if (usedUInt32Length < buffer.Length)
                {
                    Array.Resize(ref buffer, usedUInt32Length);
                }
                return ParseRawScanBuffer(buffer, scanBegin, scanEnd);
            }

            scanBufferUInt32Length = checked((int)(requiredBytes / sizeof(uint) + 1));
        }
    }

    private static int FindNearestCentroid(IReadOnlyList<Hdf5PeakRecord> centroids, double mz, double mzTolerancePpm)
    {
        int low = 0;
        int high = centroids.Count - 1;
        while (low <= high)
        {
            int mid = (low + high) / 2;
            if (centroids[mid].Mz < mz)
            {
                low = mid + 1;
            }
            else
            {
                high = mid - 1;
            }
        }

        int bestIndex = -1;
        double bestDelta = double.MaxValue;
        CheckCandidate(low - 1);
        CheckCandidate(low);
        return bestIndex;

        void CheckCandidate(int index)
        {
            if (index < 0 || index >= centroids.Count)
            {
                return;
            }

            double centroidMz = centroids[index].Mz;
            double tolerance = Math.Abs(centroidMz) * mzTolerancePpm / 1_000_000.0;
            double delta = Math.Abs(centroidMz - mz);
            if (delta <= tolerance && delta < bestDelta)
            {
                bestIndex = index;
                bestDelta = delta;
            }
        }
    }

    private void OnSpectrum(long id, uint numPeaks, IntPtr mzValues, IntPtr areaValues, IntPtr userData)
    {
        _ = userData;
        pasefCache[id] = CopyPeaks(numPeaks, mzValues, areaValues);
    }

    private static List<Hdf5PeakRecord> CopyPeaks(uint numPeaks, IntPtr mzValues, IntPtr areaValues)
    {
        int count = checked((int)numPeaks);
        double[] mz = new double[count];
        float[] area = new float[count];
        if (count > 0)
        {
            Marshal.Copy(mzValues, mz, 0, count);
            Marshal.Copy(areaValues, area, 0, count);
        }

        List<Hdf5PeakRecord> peaks = new(count);
        for (int i = 0; i < count; i++)
        {
            peaks.Add(new Hdf5PeakRecord(mz[i], area[i], 0, 0, 0, 0));
        }

        return peaks.OrderBy(peak => peak.Mz).ToList();
    }
}

internal sealed record BrukerRawScan(int ScanNumber, uint[] Indices, uint[] Intensities);

internal static partial class BrukerNative
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void MsMsSpectrumCallback(long id, uint numPeaks, IntPtr mzValues, IntPtr areaValues, IntPtr userData);

    [DllImport("timsdata", EntryPoint = "tsf_open", CallingConvention = CallingConvention.Cdecl)]
    public static extern ulong TsfOpen(string analysisDirectory, uint useRecalibratedState);

    [DllImport("timsdata", EntryPoint = "tsf_close", CallingConvention = CallingConvention.Cdecl)]
    public static extern void TsfClose(ulong handle);

    [DllImport("timsdata", EntryPoint = "tsf_get_last_error_string", CallingConvention = CallingConvention.Cdecl)]
    private static extern uint TsfGetLastErrorString(byte[]? buffer, uint length);

    [DllImport("timsdata", EntryPoint = "tsf_read_line_spectrum_v2", CallingConvention = CallingConvention.Cdecl)]
    public static extern int TsfReadLineSpectrum(
        ulong handle,
        long spectrumId,
        double[] indexArray,
        float[] intensityArray,
        int length);

    [DllImport("timsdata", EntryPoint = "tsf_index_to_mz", CallingConvention = CallingConvention.Cdecl)]
    public static extern uint TsfIndexToMz(
        ulong handle,
        long frameId,
        double[] input,
        double[] output,
        uint count);

    [DllImport("timsdata", EntryPoint = "tims_open_v2", CallingConvention = CallingConvention.Cdecl)]
    public static extern ulong TimsOpenV2(string analysisDirectory, uint useRecalibratedState, uint pressureCompensationStrategy);

    [DllImport("timsdata", EntryPoint = "tims_close", CallingConvention = CallingConvention.Cdecl)]
    public static extern void TimsClose(ulong handle);

    [DllImport("timsdata", EntryPoint = "tims_read_scans_v2", CallingConvention = CallingConvention.Cdecl)]
    public static extern uint TimsReadScansV2(
        ulong handle,
        long frameId,
        uint scanBegin,
        uint scanEnd,
        uint[] buffer,
        uint length);

    [DllImport("timsdata", EntryPoint = "tims_index_to_mz", CallingConvention = CallingConvention.Cdecl)]
    public static extern uint TimsIndexToMz(
        ulong handle,
        long frameId,
        double[] input,
        double[] output,
        uint count);

    [DllImport("timsdata", EntryPoint = "tims_scannum_to_oneoverk0", CallingConvention = CallingConvention.Cdecl)]
    public static extern uint TimsScanNumToOneOverK0(
        ulong handle,
        long frameId,
        double[] scanNumbers,
        double[] oneOverK0,
        uint count);

    [DllImport("timsdata", EntryPoint = "tims_get_last_error_string", CallingConvention = CallingConvention.Cdecl)]
    private static extern uint TimsGetLastErrorString(byte[]? buffer, uint length);

    [DllImport("timsdata", EntryPoint = "tims_read_pasef_msms_v2", CallingConvention = CallingConvention.Cdecl)]
    public static extern uint TimsReadPasefMsMsV2(
        ulong handle,
        long[] precursors,
        uint numPrecursors,
        MsMsSpectrumCallback callback,
        IntPtr userData);

    [DllImport("timsdata", EntryPoint = "tims_extract_centroided_spectrum_for_frame_v3", CallingConvention = CallingConvention.Cdecl)]
    public static extern uint TimsExtractCentroidedSpectrumForFrameV3(
        ulong handle,
        long frameId,
        uint scanBegin,
        uint scanEnd,
        MsMsSpectrumCallback callback,
        IntPtr userData);

    public static string GetTsfLastError()
    {
        return GetLastError(TsfGetLastErrorString);
    }

    public static string GetTimsLastError()
    {
        return GetLastError(TimsGetLastErrorString);
    }

    private static string GetLastError(Func<byte[]?, uint, uint> getError)
    {
        uint length = getError(null, 0);
        if (length == 0)
        {
            return "unknown Bruker SDK error";
        }

        byte[] buffer = new byte[length];
        getError(buffer, length);
        return System.Text.Encoding.UTF8.GetString(buffer).TrimEnd('\0');
    }
}

internal sealed class NativeSqliteConnection : IDisposable
{
    private const int SqliteOk = 0;
    private const int SqliteRow = 100;
    private const int SqliteDone = 101;
    private const int SqliteOpenReadonly = 0x00000001;
    private IntPtr database;

    private NativeSqliteConnection(IntPtr database)
    {
        this.database = database;
    }

    public static NativeSqliteConnection Open(string path)
    {
        byte[] pathBytes = ToUtf8(path);
        int rc = NativeSqliteMethods.OpenV2(pathBytes, out IntPtr database, SqliteOpenReadonly, IntPtr.Zero);
        if (rc != SqliteOk)
        {
            string error = database != IntPtr.Zero ? NativeSqliteMethods.ErrorMessage(database) : "unable to allocate SQLite handle";
            if (database != IntPtr.Zero)
            {
                NativeSqliteMethods.Close(database);
            }

            throw new InvalidOperationException($"Unable to open SQLite database '{path}': {error}");
        }

        return new NativeSqliteConnection(database);
    }

    public List<SqliteRow> Query(string sql)
    {
        ObjectDisposedException.ThrowIf(database == IntPtr.Zero, this);
        byte[] sqlBytes = ToUtf8(sql);
        int rc = NativeSqliteMethods.PrepareV2(database, sqlBytes, -1, out IntPtr statement, IntPtr.Zero);
        if (rc != SqliteOk)
        {
            throw new InvalidOperationException($"Unable to prepare SQLite query: {NativeSqliteMethods.ErrorMessage(database)}\n{sql}");
        }

        try
        {
            List<SqliteRow> rows = new();
            while (true)
            {
                rc = NativeSqliteMethods.Step(statement);
                if (rc == SqliteDone)
                {
                    break;
                }

                if (rc != SqliteRow)
                {
                    throw new InvalidOperationException($"Unable to execute SQLite query: {NativeSqliteMethods.ErrorMessage(database)}\n{sql}");
                }

                int columnCount = NativeSqliteMethods.ColumnCount(statement);
                object?[] values = new object?[columnCount];
                for (int i = 0; i < columnCount; i++)
                {
                    values[i] = ReadColumn(statement, i);
                }

                rows.Add(new SqliteRow(values));
            }

            return rows;
        }
        finally
        {
            NativeSqliteMethods.Finalize(statement);
        }
    }

    public void Dispose()
    {
        if (database != IntPtr.Zero)
        {
            NativeSqliteMethods.Close(database);
            database = IntPtr.Zero;
        }
    }

    private static object? ReadColumn(IntPtr statement, int index)
    {
        return NativeSqliteMethods.ColumnType(statement, index) switch
        {
            1 => NativeSqliteMethods.ColumnInt64(statement, index),
            2 => NativeSqliteMethods.ColumnDouble(statement, index),
            3 => Marshal.PtrToStringUTF8(NativeSqliteMethods.ColumnText(statement, index)),
            5 => null,
            _ => Marshal.PtrToStringUTF8(NativeSqliteMethods.ColumnText(statement, index)),
        };
    }

    private static byte[] ToUtf8(string value)
    {
        return System.Text.Encoding.UTF8.GetBytes(value + '\0');
    }
}

internal sealed class SqliteRow
{
    private readonly object?[] values;

    public SqliteRow(object?[] values)
    {
        this.values = values;
    }

    public long GetInt64(int index)
    {
        return Convert.ToInt64(values[index], CultureInfo.InvariantCulture);
    }

    public long? GetNullableInt64(int index)
    {
        return values[index] is null ? null : GetInt64(index);
    }

    public int GetInt32(int index)
    {
        return Convert.ToInt32(values[index], CultureInfo.InvariantCulture);
    }

    public int? GetNullableInt32(int index)
    {
        return values[index] is null ? null : GetInt32(index);
    }

    public double GetDouble(int index)
    {
        return Convert.ToDouble(values[index], CultureInfo.InvariantCulture);
    }

    public string GetString(int index)
    {
        return Convert.ToString(values[index], CultureInfo.InvariantCulture) ?? string.Empty;
    }
}

internal static class NativeSqliteMethods
{
    public static int OpenV2(byte[] filename, out IntPtr database, int flags, IntPtr vfs)
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? WinSqlite.OpenV2(filename, out database, flags, vfs)
            : UnixSqlite.OpenV2(filename, out database, flags, vfs);
    }

    public static int Close(IntPtr database)
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? WinSqlite.Close(database)
            : UnixSqlite.Close(database);
    }

    public static int PrepareV2(IntPtr database, byte[] sql, int byteCount, out IntPtr statement, IntPtr tail)
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? WinSqlite.PrepareV2(database, sql, byteCount, out statement, tail)
            : UnixSqlite.PrepareV2(database, sql, byteCount, out statement, tail);
    }

    public static int Step(IntPtr statement)
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? WinSqlite.Step(statement)
            : UnixSqlite.Step(statement);
    }

    public static int Finalize(IntPtr statement)
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? WinSqlite.Finalize(statement)
            : UnixSqlite.Finalize(statement);
    }

    public static int ColumnCount(IntPtr statement)
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? WinSqlite.ColumnCount(statement)
            : UnixSqlite.ColumnCount(statement);
    }

    public static int ColumnType(IntPtr statement, int index)
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? WinSqlite.ColumnType(statement, index)
            : UnixSqlite.ColumnType(statement, index);
    }

    public static long ColumnInt64(IntPtr statement, int index)
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? WinSqlite.ColumnInt64(statement, index)
            : UnixSqlite.ColumnInt64(statement, index);
    }

    public static double ColumnDouble(IntPtr statement, int index)
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? WinSqlite.ColumnDouble(statement, index)
            : UnixSqlite.ColumnDouble(statement, index);
    }

    public static IntPtr ColumnText(IntPtr statement, int index)
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? WinSqlite.ColumnText(statement, index)
            : UnixSqlite.ColumnText(statement, index);
    }

    public static string ErrorMessage(IntPtr database)
    {
        IntPtr pointer = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? WinSqlite.ErrMsg(database)
            : UnixSqlite.ErrMsg(database);
        return Marshal.PtrToStringUTF8(pointer) ?? "unknown SQLite error";
    }

    private static class UnixSqlite
    {
        [DllImport("sqlite3", EntryPoint = "sqlite3_open_v2", CallingConvention = CallingConvention.Cdecl)]
        public static extern int OpenV2(byte[] filename, out IntPtr database, int flags, IntPtr vfs);

        [DllImport("sqlite3", EntryPoint = "sqlite3_close", CallingConvention = CallingConvention.Cdecl)]
        public static extern int Close(IntPtr database);

        [DllImport("sqlite3", EntryPoint = "sqlite3_prepare_v2", CallingConvention = CallingConvention.Cdecl)]
        public static extern int PrepareV2(IntPtr database, byte[] sql, int byteCount, out IntPtr statement, IntPtr tail);

        [DllImport("sqlite3", EntryPoint = "sqlite3_step", CallingConvention = CallingConvention.Cdecl)]
        public static extern int Step(IntPtr statement);

        [DllImport("sqlite3", EntryPoint = "sqlite3_finalize", CallingConvention = CallingConvention.Cdecl)]
        public static extern int Finalize(IntPtr statement);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_count", CallingConvention = CallingConvention.Cdecl)]
        public static extern int ColumnCount(IntPtr statement);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_type", CallingConvention = CallingConvention.Cdecl)]
        public static extern int ColumnType(IntPtr statement, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_int64", CallingConvention = CallingConvention.Cdecl)]
        public static extern long ColumnInt64(IntPtr statement, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_double", CallingConvention = CallingConvention.Cdecl)]
        public static extern double ColumnDouble(IntPtr statement, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_text", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr ColumnText(IntPtr statement, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_errmsg", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr ErrMsg(IntPtr database);
    }

    private static class WinSqlite
    {
        [DllImport("sqlite3", EntryPoint = "sqlite3_open_v2", CallingConvention = CallingConvention.Cdecl)]
        public static extern int OpenV2(byte[] filename, out IntPtr database, int flags, IntPtr vfs);

        [DllImport("sqlite3", EntryPoint = "sqlite3_close", CallingConvention = CallingConvention.Cdecl)]
        public static extern int Close(IntPtr database);

        [DllImport("sqlite3", EntryPoint = "sqlite3_prepare_v2", CallingConvention = CallingConvention.Cdecl)]
        public static extern int PrepareV2(IntPtr database, byte[] sql, int byteCount, out IntPtr statement, IntPtr tail);

        [DllImport("sqlite3", EntryPoint = "sqlite3_step", CallingConvention = CallingConvention.Cdecl)]
        public static extern int Step(IntPtr statement);

        [DllImport("sqlite3", EntryPoint = "sqlite3_finalize", CallingConvention = CallingConvention.Cdecl)]
        public static extern int Finalize(IntPtr statement);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_count", CallingConvention = CallingConvention.Cdecl)]
        public static extern int ColumnCount(IntPtr statement);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_type", CallingConvention = CallingConvention.Cdecl)]
        public static extern int ColumnType(IntPtr statement, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_int64", CallingConvention = CallingConvention.Cdecl)]
        public static extern long ColumnInt64(IntPtr statement, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_double", CallingConvention = CallingConvention.Cdecl)]
        public static extern double ColumnDouble(IntPtr statement, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_column_text", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr ColumnText(IntPtr statement, int index);

        [DllImport("sqlite3", EntryPoint = "sqlite3_errmsg", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr ErrMsg(IntPtr database);
    }
}
