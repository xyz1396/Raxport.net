using System.Runtime.InteropServices;
using System.Diagnostics;
using System.Text;

namespace Raxport;

internal sealed record Hdf5PeakRecord(
    double Mz,
    double Intensity,
    double Resolution,
    double Baseline,
    double Noise,
    int Charge);

internal sealed record Hdf5PrecursorCandidateRecord(
    int Charge,
    double Mz);

internal sealed record Hdf5ReactionRecord(
    double PrecursorMass,
    double IsolationWidth,
    int ChargeState,
    double CollisionEnergy,
    bool CollisionEnergyValid,
    string ActivationType,
    bool MultipleActivation,
    bool PrecursorRangeValid,
    double FirstPrecursorMass,
    double LastPrecursorMass,
    double IsolationWidthOffset,
    IReadOnlyList<Hdf5PrecursorCandidateRecord> Candidates);

internal sealed record Hdf5ScanRecord(
    int ScanNumber,
    int MsOrder,
    double RetentionTime,
    double Tic,
    string ScanFilter,
    string Activation,
    int ParentScanNumber,
    Hdf5ReactionRecord? Reaction,
    IReadOnlyList<Hdf5PeakRecord> Peaks);

internal sealed partial class Hdf5BufferedWriter : IDisposable
{
    private const int ErrorBufferLength = 4096;
    private readonly long maxBufferedPeaks;
    private readonly List<BufferedScan> scans = new();
    private readonly List<Hdf5PeakRecord> peaks = new();
    private readonly List<BufferedReaction> reactions = new();
    private readonly List<Hdf5PrecursorCandidateRecord> candidates = new();
    private readonly Dictionary<string, int> scanFilterIds = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> activationIds = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> reactionActivationTypeIds = new(StringComparer.Ordinal);
    private readonly List<string> pendingScanFilters = new();
    private readonly List<string> pendingActivations = new();
    private readonly List<string> pendingReactionActivationTypes = new();
    private readonly string path;
    private IntPtr handle;
    private long totalPeaks;
    private long totalReactions;
    private long totalCandidates;
    private long totalScans;
    private bool disposed;

    public Hdf5BufferedWriter(
        string path,
        string sourceRawFile,
        string instrumentModel,
        string raxportVersion,
        long maxBufferedPeaks)
    {
        if (maxBufferedPeaks <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBufferedPeaks), "Peak flush limit must be positive.");
        }

        this.path = path;
        this.maxBufferedPeaks = maxBufferedPeaks;

        byte[] error = new byte[ErrorBufferLength];
        Stopwatch writeTimer = Stopwatch.StartNew();
        int rc = NativeMethods.Create(path, sourceRawFile, instrumentModel, raxportVersion, out handle, error, error.Length);
        writeTimer.Stop();
        HdfWriteElapsed += writeTimer.Elapsed;
        if (rc != 0 || handle == IntPtr.Zero)
        {
            throw new InvalidOperationException($"Unable to create HDF5 file '{path}': {ReadError(error)}");
        }
    }

    public TimeSpan HdfWriteElapsed { get; private set; }

    public int FlushCount { get; private set; }

    public long TotalScans => totalScans;

    public long TotalPeaks => totalPeaks;

    public long TotalReactions => totalReactions;

    public long TotalCandidates => totalCandidates;

    public void AddScan(Hdf5ScanRecord scan)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        if (scans.Count > 0 && peaks.Count + (long)scan.Peaks.Count > maxBufferedPeaks)
        {
            Flush();
        }

        long peakStart = totalPeaks + peaks.Count;
        peaks.AddRange(scan.Peaks);

        long reactionStart = -1;
        int reactionCount = 0;
        if (scan.Reaction is not null)
        {
            long candidateStart = totalCandidates + candidates.Count;
            candidates.AddRange(scan.Reaction.Candidates);
            reactionStart = totalReactions + reactions.Count;
            reactionCount = 1;
            int reactionActivationTypeId = GetStringId(
                reactionActivationTypeIds,
                pendingReactionActivationTypes,
                scan.Reaction.ActivationType);
            reactions.Add(new BufferedReaction(scan.Reaction, reactionActivationTypeId, candidateStart, scan.Reaction.Candidates.Count));
        }

        int scanFilterId = GetStringId(scanFilterIds, pendingScanFilters, scan.ScanFilter);
        int activationId = GetStringId(activationIds, pendingActivations, scan.Activation);
        scans.Add(new BufferedScan(
            scan.ScanNumber,
            scan.MsOrder,
            scan.RetentionTime,
            scan.Tic,
            scanFilterId,
            activationId,
            scan.ParentScanNumber,
            reactionStart,
            reactionCount,
            peakStart,
            scan.Peaks.Count));

        if (peaks.Count >= maxBufferedPeaks)
        {
            Flush();
        }
    }

    public void Flush()
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        if (scans.Count == 0)
        {
            return;
        }

        Stopwatch writeTimer = Stopwatch.StartNew();
        int scanBatchCount = scans.Count;
        int peakBatchCount = peaks.Count;
        int reactionBatchCount = reactions.Count;
        int candidateBatchCount = candidates.Count;

        byte[] error = new byte[ErrorBufferLength];
        int rc;
        try
        {
            using NativeStringArray newScanFilters = NativeStringArray.FromStrings(pendingScanFilters);
            using NativeStringArray newActivations = NativeStringArray.FromStrings(pendingActivations);
            using NativeStringArray newReactionActivationTypes = NativeStringArray.FromStrings(pendingReactionActivationTypes);
            int[] scanNumber = new int[scanBatchCount];
            int[] msOrder = new int[scanBatchCount];
            double[] retentionTime = new double[scanBatchCount];
            double[] tic = new double[scanBatchCount];
            int[] scanFilterId = new int[scanBatchCount];
            int[] activationId = new int[scanBatchCount];
            int[] parentScanNumber = new int[scanBatchCount];
            long[] reactionStart = new long[scanBatchCount];
            int[] reactionCount = new int[scanBatchCount];
            long[] peakStart = new long[scanBatchCount];
            int[] peakCount = new int[scanBatchCount];
            for (int i = 0; i < scanBatchCount; i++)
            {
                BufferedScan scan = scans[i];
                scanNumber[i] = scan.ScanNumber;
                msOrder[i] = scan.MsOrder;
                retentionTime[i] = scan.RetentionTime;
                tic[i] = scan.Tic;
                scanFilterId[i] = scan.ScanFilterId;
                activationId[i] = scan.ActivationId;
                parentScanNumber[i] = scan.ParentScanNumber;
                reactionStart[i] = scan.ReactionStart;
                reactionCount[i] = scan.ReactionCount;
                peakStart[i] = scan.PeakStart;
                peakCount[i] = scan.PeakCount;
            }

            double[] peakMz = new double[peakBatchCount];
            double[] peakIntensity = new double[peakBatchCount];
            double[] peakResolution = new double[peakBatchCount];
            double[] peakBaseline = new double[peakBatchCount];
            double[] peakNoise = new double[peakBatchCount];
            int[] peakCharge = new int[peakBatchCount];
            for (int i = 0; i < peakBatchCount; i++)
            {
                Hdf5PeakRecord peak = peaks[i];
                peakMz[i] = peak.Mz;
                peakIntensity[i] = peak.Intensity;
                peakResolution[i] = peak.Resolution;
                peakBaseline[i] = peak.Baseline;
                peakNoise[i] = peak.Noise;
                peakCharge[i] = peak.Charge;
            }

            double[] reactionPrecursorMass = new double[reactionBatchCount];
            double[] reactionIsolationWidth = new double[reactionBatchCount];
            int[] reactionChargeState = new int[reactionBatchCount];
            double[] reactionCollisionEnergy = new double[reactionBatchCount];
            int[] reactionCollisionEnergyValid = new int[reactionBatchCount];
            int[] reactionActivationTypeId = new int[reactionBatchCount];
            int[] reactionMultipleActivation = new int[reactionBatchCount];
            int[] reactionPrecursorRangeValid = new int[reactionBatchCount];
            double[] reactionFirstPrecursorMass = new double[reactionBatchCount];
            double[] reactionLastPrecursorMass = new double[reactionBatchCount];
            double[] reactionIsolationWidthOffset = new double[reactionBatchCount];
            long[] reactionCandidateStart = new long[reactionBatchCount];
            int[] reactionCandidateCount = new int[reactionBatchCount];
            for (int i = 0; i < reactionBatchCount; i++)
            {
                BufferedReaction reaction = reactions[i];
                reactionPrecursorMass[i] = reaction.PrecursorMass;
                reactionIsolationWidth[i] = reaction.IsolationWidth;
                reactionChargeState[i] = reaction.ChargeState;
                reactionCollisionEnergy[i] = reaction.CollisionEnergy;
                reactionCollisionEnergyValid[i] = reaction.CollisionEnergyValid ? 1 : 0;
                reactionActivationTypeId[i] = reaction.ActivationTypeId;
                reactionMultipleActivation[i] = reaction.MultipleActivation ? 1 : 0;
                reactionPrecursorRangeValid[i] = reaction.PrecursorRangeValid ? 1 : 0;
                reactionFirstPrecursorMass[i] = reaction.FirstPrecursorMass;
                reactionLastPrecursorMass[i] = reaction.LastPrecursorMass;
                reactionIsolationWidthOffset[i] = reaction.IsolationWidthOffset;
                reactionCandidateStart[i] = reaction.CandidateStart;
                reactionCandidateCount[i] = reaction.CandidateCount;
            }

            int[] candidateCharge = new int[candidateBatchCount];
            double[] candidateMz = new double[candidateBatchCount];
            for (int i = 0; i < candidateBatchCount; i++)
            {
                Hdf5PrecursorCandidateRecord candidate = candidates[i];
                candidateCharge[i] = candidate.Charge;
                candidateMz[i] = candidate.Mz;
            }

            rc = NativeMethods.Append(
                handle,
                scanBatchCount,
                scanNumber,
                msOrder,
                retentionTime,
                tic,
                scanFilterId,
                activationId,
                parentScanNumber,
                reactionStart,
                reactionCount,
                peakStart,
                peakCount,
                peakBatchCount,
                peakMz,
                peakIntensity,
                peakResolution,
                peakBaseline,
                peakNoise,
                peakCharge,
                reactionBatchCount,
                reactionPrecursorMass,
                reactionIsolationWidth,
                reactionChargeState,
                reactionCollisionEnergy,
                reactionCollisionEnergyValid,
                reactionActivationTypeId,
                reactionMultipleActivation,
                reactionPrecursorRangeValid,
                reactionFirstPrecursorMass,
                reactionLastPrecursorMass,
                reactionIsolationWidthOffset,
                reactionCandidateStart,
                reactionCandidateCount,
                candidateBatchCount,
                candidateCharge,
                candidateMz,
                pendingScanFilters.Count,
                newScanFilters.Pointers,
                pendingActivations.Count,
                newActivations.Pointers,
                pendingReactionActivationTypes.Count,
                newReactionActivationTypes.Pointers,
                error,
                error.Length);
        }
        finally
        {
            writeTimer.Stop();
            HdfWriteElapsed += writeTimer.Elapsed;
        }

        if (rc != 0)
        {
            throw new InvalidOperationException($"Unable to append HDF5 data to '{path}': {ReadError(error)}");
        }

        totalScans += scanBatchCount;
        totalPeaks += peakBatchCount;
        totalReactions += reactionBatchCount;
        totalCandidates += candidateBatchCount;
        FlushCount++;
        scans.Clear();
        peaks.Clear();
        reactions.Clear();
        candidates.Clear();
        pendingScanFilters.Clear();
        pendingActivations.Clear();
        pendingReactionActivationTypes.Clear();
    }

    private static int GetStringId(Dictionary<string, int> ids, List<string> pendingValues, string? value)
    {
        string safeValue = value ?? string.Empty;
        if (ids.TryGetValue(safeValue, out int id))
        {
            return id;
        }

        id = ids.Count;
        ids.Add(safeValue, id);
        pendingValues.Add(safeValue);
        return id;
    }

    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        try
        {
            Flush();
        }
        finally
        {
            if (handle != IntPtr.Zero)
            {
                byte[] error = new byte[ErrorBufferLength];
                Stopwatch writeTimer = Stopwatch.StartNew();
                int rc = NativeMethods.Close(handle, error, error.Length);
                writeTimer.Stop();
                HdfWriteElapsed += writeTimer.Elapsed;
                handle = IntPtr.Zero;
                if (rc != 0)
                {
                    throw new InvalidOperationException($"Unable to close HDF5 file '{path}': {ReadError(error)}");
                }
            }

            disposed = true;
        }
    }

    private static string ReadError(byte[] error)
    {
        int length = Array.IndexOf(error, (byte)0);
        if (length < 0)
        {
            length = error.Length;
        }

        string message = Encoding.UTF8.GetString(error, 0, length).Trim();
        return string.IsNullOrWhiteSpace(message) ? "unknown native HDF5 error" : message;
    }

    private sealed record BufferedScan(
        int ScanNumber,
        int MsOrder,
        double RetentionTime,
        double Tic,
        int ScanFilterId,
        int ActivationId,
        int ParentScanNumber,
        long ReactionStart,
        int ReactionCount,
        long PeakStart,
        int PeakCount);

    private sealed record BufferedReaction(
        double PrecursorMass,
        double IsolationWidth,
        int ChargeState,
        double CollisionEnergy,
        bool CollisionEnergyValid,
        int ActivationTypeId,
        bool MultipleActivation,
        bool PrecursorRangeValid,
        double FirstPrecursorMass,
        double LastPrecursorMass,
        double IsolationWidthOffset,
        long CandidateStart,
        int CandidateCount)
    {
        public BufferedReaction(Hdf5ReactionRecord reaction, int activationTypeId, long candidateStart, int candidateCount)
            : this(
                reaction.PrecursorMass,
                reaction.IsolationWidth,
                reaction.ChargeState,
                reaction.CollisionEnergy,
                reaction.CollisionEnergyValid,
                activationTypeId,
                reaction.MultipleActivation,
                reaction.PrecursorRangeValid,
                reaction.FirstPrecursorMass,
                reaction.LastPrecursorMass,
                reaction.IsolationWidthOffset,
                candidateStart,
                candidateCount)
        {
        }
    }

    private sealed class NativeStringArray : IDisposable
    {
        private readonly IntPtr[] allocated;

        private NativeStringArray(IntPtr[] allocated)
        {
            this.allocated = allocated;
            Pointers = allocated.Length == 0 ? new[] { IntPtr.Zero } : allocated;
        }

        public IntPtr[] Pointers { get; }

        public static NativeStringArray FromStrings(IReadOnlyList<string> values)
        {
            IntPtr[] pointers = new IntPtr[values.Count];
            for (int i = 0; i < values.Count; i++)
            {
                pointers[i] = AllocateUtf8(values[i]);
            }
            return new NativeStringArray(pointers);
        }

        public void Dispose()
        {
            foreach (IntPtr pointer in allocated)
            {
                if (pointer != IntPtr.Zero)
                {
                    Marshal.FreeHGlobal(pointer);
                }
            }
        }

        private static IntPtr AllocateUtf8(string? value)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(value ?? string.Empty);
            IntPtr pointer = Marshal.AllocHGlobal(bytes.Length + 1);
            Marshal.Copy(bytes, 0, pointer, bytes.Length);
            Marshal.WriteByte(pointer, bytes.Length, 0);
            return pointer;
        }
    }

    private static partial class NativeMethods
    {
        [LibraryImport("raxport_hdf5", EntryPoint = "rax_h5_create", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial int Create(
            string path,
            string sourceRawFile,
            string instrumentModel,
            string raxportVersion,
            out IntPtr writer,
            byte[] error,
            int errorLength);

        [LibraryImport("raxport_hdf5", EntryPoint = "rax_h5_append")]
        internal static partial int Append(
            IntPtr writer,
            int scanCount,
            int[] scanNumber,
            int[] msOrder,
            double[] retentionTime,
            double[] tic,
            int[] scanFilterId,
            int[] activationId,
            int[] parentScanNumber,
            long[] reactionStart,
            int[] reactionCount,
            long[] peakStart,
            int[] peakCount,
            int peakTotal,
            double[] peakMz,
            double[] peakIntensity,
            double[] peakResolution,
            double[] peakBaseline,
            double[] peakNoise,
            int[] peakCharge,
            int reactionTotal,
            double[] reactionPrecursorMass,
            double[] reactionIsolationWidth,
            int[] reactionChargeState,
            double[] reactionCollisionEnergy,
            int[] reactionCollisionEnergyValid,
            int[] reactionActivationTypeId,
            int[] reactionMultipleActivation,
            int[] reactionPrecursorRangeValid,
            double[] reactionFirstPrecursorMass,
            double[] reactionLastPrecursorMass,
            double[] reactionIsolationWidthOffset,
            long[] reactionCandidateStart,
            int[] reactionCandidateCount,
            int candidateTotal,
            int[] candidateCharge,
            double[] candidateMz,
            int newScanFilterTotal,
            IntPtr[] newScanFilters,
            int newActivationTotal,
            IntPtr[] newActivations,
            int newReactionActivationTypeTotal,
            IntPtr[] newReactionActivationTypes,
            byte[] error,
            int errorLength);

        [LibraryImport("raxport_hdf5", EntryPoint = "rax_h5_close")]
        internal static partial int Close(IntPtr writer, byte[] error, int errorLength);
    }
}
