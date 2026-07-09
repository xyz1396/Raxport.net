namespace Raxport;

public enum RaxportOutputFormat
{
    Hdf5,
    MzMl
}

internal sealed record RaxportPeakRecord(
    double Mz,
    double Intensity,
    double Resolution,
    double Baseline,
    double Noise,
    int Charge,
    RaxportPeakMobilityTrace? MobilityTrace = null,
    double CandidateOneOverK0 = 0);

internal sealed record RaxportPeakMobilityTrace(
    int[] OneOverK0Indices,
    float[] Intensities)
{
    public int Count
    {
        get
        {
            if (OneOverK0Indices.Length != Intensities.Length)
            {
                throw new InvalidOperationException("Mobility trace index and intensity arrays must have matching lengths.");
            }

            return OneOverK0Indices.Length;
        }
    }
}

internal sealed record RaxportPrecursorCandidateRecord(
    int Charge,
    double Mz,
    double Intensity = 0,
    double OneOverK0 = 0);

internal sealed record RaxportReactionRecord(
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
    IReadOnlyList<RaxportPrecursorCandidateRecord> Candidates,
    double OneOverK0Begin = 0,
    double OneOverK0End = 0);

internal sealed record RaxportScanRecord(
    int ScanNumber,
    int MsOrder,
    double RetentionTime,
    double Tic,
    string ScanFilter,
    string Activation,
    int ParentScanNumber,
    RaxportReactionRecord? Reaction,
    IReadOnlyList<RaxportPeakRecord> Peaks,
    bool IsCentroided = true,
    double? ScanWindowLowerMz = null,
    double? ScanWindowUpperMz = null,
    string Polarity = "");

internal interface IRaxportWriter : IDisposable
{
    TimeSpan WriteElapsed { get; }

    int FlushCount { get; }

    long TotalScans { get; }

    long TotalPeaks { get; }

    long TotalReactions { get; }

    long TotalCandidates { get; }

    void AddScan(RaxportScanRecord scan);

    void Flush();
}

internal static class RaxportWriterFactory
{
    public const string RaxportVersion = "6.0";

    public static string GetOutputExtension(RaxportOutputFormat outputFormat)
    {
        return outputFormat == RaxportOutputFormat.MzMl ? ".mzML" : ".h5";
    }

    public static string GetOutputFormatLabel(RaxportOutputFormat outputFormat)
    {
        return outputFormat == RaxportOutputFormat.MzMl ? "indexed mzML" : "HDF5";
    }

    public static bool UsesPeakBufferTuning(RaxportOutputFormat outputFormat)
    {
        return outputFormat == RaxportOutputFormat.Hdf5;
    }

    public static IRaxportWriter Create(
        string outputFile,
        string sourcePath,
        string instrumentModel,
        RaxportOutputFormat outputFormat,
        long maxBufferedPeaks,
        int writerCompressionLevel)
    {
        if (outputFormat == RaxportOutputFormat.MzMl)
        {
            return new MzMlWriter(outputFile, sourcePath, instrumentModel, RaxportVersion);
        }

        return new Hdf5Writer(
            outputFile,
            sourcePath,
            instrumentModel,
            RaxportVersion,
            maxBufferedPeaks,
            writerCompressionLevel);
    }
}
