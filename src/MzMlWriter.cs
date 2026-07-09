using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;

namespace Raxport;

internal sealed class MzMlWriter : IRaxportWriter
{
    private const string MzMlNamespace = "http://psi.hupo.org/ms/mzml";
    private const string SourceFileId = "RAW1";
    private const string InstrumentConfigurationId = "IC1";
    private const string SoftwareId = "Raxport";
    private const string DataProcessingId = "RaxportProcessing";
    private const int FileCopyBufferSize = 1024 * 1024;
    private const CompressionLevel BinaryArrayCompressionLevel = CompressionLevel.Fastest;
    private static readonly UTF8Encoding Utf8NoBom = new(false);

    private readonly string path;
    private readonly string sourcePath;
    private readonly string instrumentModel;
    private readonly string raxportVersion;
    private readonly string mzMlId;
    private readonly string tempSpectrumPath;
    private readonly FileStream spectrumStream;
    private readonly List<IndexEntry> spectrumIndex = new();
    private readonly List<double> chromatogramTimes = new();
    private readonly List<double> basePeakIntensities = new();
    private readonly Stopwatch writeTimer = new();
    private bool disposed;
    private bool hasMs1;
    private bool hasMsn;

    public MzMlWriter(string path, string sourcePath, string instrumentModel, string raxportVersion)
    {
        this.path = path;
        this.sourcePath = sourcePath;
        this.instrumentModel = instrumentModel;
        this.raxportVersion = raxportVersion;
        mzMlId = ToNcName(Path.GetFileNameWithoutExtension(path));
        tempSpectrumPath = Path.Combine(Path.GetTempPath(), $"raxport-mzml-spectra-{Guid.NewGuid():N}.tmp");
        spectrumStream = new FileStream(tempSpectrumPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 1024 * 1024);
    }

    public TimeSpan WriteElapsed { get; private set; }

    public int FlushCount { get; private set; }

    public long TotalScans { get; private set; }

    public long TotalPeaks { get; private set; }

    public long TotalReactions { get; private set; }

    public long TotalCandidates { get; private set; }

    public void AddScan(RaxportScanRecord scan)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        string id = SpectrumId(scan.ScanNumber);
        long relativeOffset = spectrumStream.Position;
        string spectrumXml = BuildSpectrumXml(scan, checked((int)TotalScans), id);
        WriteTimerStart();
        try
        {
            byte[] bytes = Utf8NoBom.GetBytes(spectrumXml);
            spectrumStream.Write(bytes, 0, bytes.Length);
        }
        finally
        {
            WriteTimerStop();
        }

        spectrumIndex.Add(new IndexEntry(id, relativeOffset));
        TotalScans++;
        TotalPeaks += scan.Peaks.Count;
        if (scan.MsOrder == 1)
        {
            hasMs1 = true;
        }
        else if (scan.MsOrder > 1)
        {
            hasMsn = true;
        }

        if (scan.Reaction is not null)
        {
            TotalReactions++;
            TotalCandidates += scan.Reaction.Candidates.Count;
        }

        chromatogramTimes.Add(scan.RetentionTime);
        basePeakIntensities.Add(GetBasePeakIntensity(scan.Peaks));
    }

    public void Flush()
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        WriteTimerStart();
        try
        {
            spectrumStream.Flush();
        }
        finally
        {
            WriteTimerStop();
        }
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
            spectrumStream.Dispose();
            WriteFinalFile();
            FlushCount = TotalScans > 0 ? 1 : 0;
        }
        finally
        {
            if (File.Exists(tempSpectrumPath))
            {
                File.Delete(tempSpectrumPath);
            }

            disposed = true;
        }
    }

    private void WriteFinalFile()
    {
        WriteTimerStart();
        try
        {
            using FileStream output = new(path, FileMode.Create, FileAccess.Write, FileShare.None, 1024 * 1024);
            using SHA1 sha1 = SHA1.Create();

            WriteString(output, sha1, BuildHeader(), hash: true);
            long spectrumBaseOffset = output.Position;
            CopySpectrumTempFile(output, sha1);
            WriteString(output, sha1, "  </spectrumList>\n", hash: true);

            List<IndexEntry> chromatogramIndex = new();
            if (chromatogramTimes.Count > 0)
            {
                WriteString(output, sha1, "  <chromatogramList count=\"1\" defaultDataProcessingRef=\"" + DataProcessingId + "\">\n", hash: true);
                long chromatogramOffset = output.Position;
                const string chromatogramId = "BasePeak_0";
                WriteString(output, sha1, BuildBasePeakChromatogramXml(chromatogramId), hash: true);
                chromatogramIndex.Add(new IndexEntry(chromatogramId, chromatogramOffset));
                WriteString(output, sha1, "  </chromatogramList>\n", hash: true);
            }

            WriteString(output, sha1, " </run>\n</mzML>\n", hash: true);
            long indexListOffset = output.Position;
            WriteString(output, sha1, BuildIndexList(spectrumBaseOffset, chromatogramIndex), hash: true);
            WriteString(output, sha1, " <indexListOffset>" + indexListOffset.ToString(CultureInfo.InvariantCulture) + "</indexListOffset>\n", hash: true);
            sha1.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
            string checksum = Convert.ToHexString(sha1.Hash ?? Array.Empty<byte>()).ToLowerInvariant();
            WriteString(output, sha1, " <fileChecksum>" + checksum + "</fileChecksum>\n</indexedmzML>\n", hash: false);
        }
        finally
        {
            WriteTimerStop();
        }
    }

    private void CopySpectrumTempFile(FileStream output, SHA1 sha1)
    {
        using FileStream input = new(tempSpectrumPath, FileMode.Open, FileAccess.Read, FileShare.Read, FileCopyBufferSize);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(FileCopyBufferSize);
        try
        {
            int read;
            while ((read = input.Read(buffer, 0, buffer.Length)) > 0)
            {
                WriteBytes(output, sha1, buffer, read, hash: true);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private string BuildHeader()
    {
        StringBuilder sb = new();
        string sourceName = Path.GetFileName(sourcePath.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        string sourceLocation = GetSourceLocation(sourcePath);
        string startTimeStamp = GetSourceTimestamp(sourcePath);

        sb.Append("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");
        sb.Append("<indexedmzML xmlns=\"").Append(MzMlNamespace).Append("\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://psi.hupo.org/ms/mzml http://psidev.info/files/ms/mzML/xsd/mzML1.1.2_idx.xsd\">\n");
        sb.Append("<mzML version=\"1.1.0\" id=\"").Append(XmlEscape(mzMlId)).Append("\">\n");
        sb.Append(" <cvList count=\"2\">\n");
        sb.Append("  <cv id=\"MS\" fullName=\"Mass spectrometry ontology\" version=\"4.1.204\" URI=\"https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo\" />\n");
        sb.Append("  <cv id=\"UO\" fullName=\"Unit Ontology\" version=\"09:04:2014\" URI=\"https://raw.githubusercontent.com/bio-ontology-research-group/unit-ontology/master/unit.obo\" />\n");
        sb.Append(" </cvList>\n");
        sb.Append(" <fileDescription>\n");
        sb.Append("  <fileContent>\n");
        if (hasMs1)
        {
            AppendCvParam(sb, "   ", "MS:1000579", "MS1 spectrum");
        }
        if (hasMsn)
        {
            AppendCvParam(sb, "   ", "MS:1000580", "MSn spectrum");
        }
        if (chromatogramTimes.Count > 0)
        {
            AppendCvParam(sb, "   ", "MS:1000628", "basepeak chromatogram");
        }
        sb.Append("  </fileContent>\n");
        sb.Append("  <sourceFileList count=\"1\">\n");
        sb.Append("   <sourceFile id=\"").Append(SourceFileId).Append("\" name=\"").Append(XmlEscape(sourceName)).Append("\" location=\"").Append(XmlEscape(sourceLocation)).Append("\">\n");
        if (sourcePath.EndsWith(".raw", StringComparison.OrdinalIgnoreCase))
        {
            AppendCvParam(sb, "    ", "MS:1000768", "Thermo nativeID format");
            AppendCvParam(sb, "    ", "MS:1000563", "Thermo RAW format");
        }
        else
        {
            AppendUserParam(sb, "    ", "source format", "Bruker native data", "xsd:string");
        }
        string? sourceChecksum = TryCalculateSourceChecksum(sourcePath);
        if (sourceChecksum is not null)
        {
            AppendCvParam(sb, "    ", "MS:1000569", "SHA-1", sourceChecksum);
        }
        sb.Append("   </sourceFile>\n");
        sb.Append("  </sourceFileList>\n");
        sb.Append(" </fileDescription>\n");
        sb.Append(" <referenceableParamGroupList count=\"1\">\n");
        sb.Append("  <referenceableParamGroup id=\"commonInstrumentParams\">\n");
        AppendUserParam(sb, "   ", "instrument model", instrumentModel, "xsd:string");
        sb.Append("  </referenceableParamGroup>\n");
        sb.Append(" </referenceableParamGroupList>\n");
        sb.Append(" <softwareList count=\"1\">\n");
        sb.Append("  <software id=\"").Append(SoftwareId).Append("\" version=\"").Append(XmlEscape(raxportVersion)).Append("\">\n");
        AppendUserParam(sb, "   ", "software", "Raxport", "xsd:string");
        sb.Append("  </software>\n");
        sb.Append(" </softwareList>\n");
        sb.Append(" <instrumentConfigurationList count=\"1\">\n");
        sb.Append("  <instrumentConfiguration id=\"").Append(InstrumentConfigurationId).Append("\">\n");
        sb.Append("   <referenceableParamGroupRef ref=\"commonInstrumentParams\" />\n");
        sb.Append("  </instrumentConfiguration>\n");
        sb.Append(" </instrumentConfigurationList>\n");
        sb.Append(" <dataProcessingList count=\"1\">\n");
        sb.Append("  <dataProcessing id=\"").Append(DataProcessingId).Append("\">\n");
        sb.Append("   <processingMethod order=\"0\" softwareRef=\"").Append(SoftwareId).Append("\">\n");
        AppendCvParam(sb, "    ", "MS:1000544", "Conversion to mzML");
        sb.Append("   </processingMethod>\n");
        sb.Append("  </dataProcessing>\n");
        sb.Append(" </dataProcessingList>\n");
        sb.Append(" <run id=\"").Append(XmlEscape(mzMlId)).Append("\" defaultInstrumentConfigurationRef=\"").Append(InstrumentConfigurationId).Append("\" defaultSourceFileRef=\"").Append(SourceFileId).Append("\" startTimeStamp=\"").Append(startTimeStamp).Append("\">\n");
        sb.Append("  <spectrumList count=\"").Append(TotalScans.ToString(CultureInfo.InvariantCulture)).Append("\" defaultDataProcessingRef=\"").Append(DataProcessingId).Append("\">\n");
        return sb.ToString();
    }

    private string BuildSpectrumXml(RaxportScanRecord scan, int index, string id)
    {
        StringBuilder sb = new();
        int defaultArrayLength = scan.Peaks.Count;
        sb.Append("<spectrum index=\"").Append(index.ToString(CultureInfo.InvariantCulture)).Append("\" id=\"").Append(XmlEscape(id)).Append("\" defaultArrayLength=\"").Append(defaultArrayLength.ToString(CultureInfo.InvariantCulture)).Append("\">\n");
        if (scan.MsOrder == 1)
        {
            AppendCvParam(sb, " ", "MS:1000579", "MS1 spectrum");
        }
        else
        {
            AppendCvParam(sb, " ", "MS:1000580", "MSn spectrum");
        }
        AppendCvParam(sb, " ", "MS:1000511", "ms level", Math.Max(scan.MsOrder, 1).ToString(CultureInfo.InvariantCulture));
        AppendCvParam(sb, " ", scan.IsCentroided ? "MS:1000127" : "MS:1000128", scan.IsCentroided ? "centroid spectrum" : "profile spectrum");
        AppendPolarity(sb, scan.Polarity);
        AppendCvParam(sb, " ", "MS:1000285", "total ion current", Format(scan.Tic));
        AppendPeakSummaries(sb, scan.Peaks);
        AppendScanList(sb, scan);
        if (scan.Reaction is not null)
        {
            AppendPrecursorList(sb, scan);
        }
        AppendBinaryDataArrayList(sb, scan.Peaks);
        sb.Append("</spectrum>\n");
        return sb.ToString();
    }

    private static double GetBasePeakIntensity(IReadOnlyList<RaxportPeakRecord> peaks)
    {
        double basePeakIntensity = 0;
        for (int i = 0; i < peaks.Count; i++)
        {
            if (peaks[i].Intensity > basePeakIntensity)
            {
                basePeakIntensity = peaks[i].Intensity;
            }
        }
        return basePeakIntensity;
    }

    private void AppendPeakSummaries(StringBuilder sb, IReadOnlyList<RaxportPeakRecord> peaks)
    {
        if (peaks.Count == 0)
        {
            return;
        }

        RaxportPeakRecord basePeak = peaks[0];
        double lowestMz = peaks[0].Mz;
        double highestMz = peaks[0].Mz;
        foreach (RaxportPeakRecord peak in peaks)
        {
            if (peak.Intensity > basePeak.Intensity)
            {
                basePeak = peak;
            }
            lowestMz = Math.Min(lowestMz, peak.Mz);
            highestMz = Math.Max(highestMz, peak.Mz);
        }

        AppendCvParam(sb, " ", "MS:1000504", "base peak m/z", Format(basePeak.Mz), "MS:1000040", "m/z", "MS");
        AppendCvParam(sb, " ", "MS:1000505", "base peak intensity", Format(basePeak.Intensity), "MS:1000131", "number of detector counts", "MS");
        AppendCvParam(sb, " ", "MS:1000528", "lowest observed m/z", Format(lowestMz), "MS:1000040", "m/z", "MS");
        AppendCvParam(sb, " ", "MS:1000527", "highest observed m/z", Format(highestMz), "MS:1000040", "m/z", "MS");
    }

    private void AppendScanList(StringBuilder sb, RaxportScanRecord scan)
    {
        sb.Append(" <scanList count=\"1\">\n");
        AppendCvParam(sb, "  ", "MS:1000795", "no combination");
        sb.Append("  <scan instrumentConfigurationRef=\"").Append(InstrumentConfigurationId).Append("\">\n");
        AppendCvParam(sb, "   ", "MS:1000016", "scan start time", Format(scan.RetentionTime), "UO:0000031", "minute", "UO");
        if (!string.IsNullOrWhiteSpace(scan.ScanFilter))
        {
            AppendCvParam(sb, "   ", "MS:1000512", "filter string", scan.ScanFilter);
        }
        if (scan.ScanWindowLowerMz.HasValue || scan.ScanWindowUpperMz.HasValue)
        {
            sb.Append("   <scanWindowList count=\"1\">\n");
            sb.Append("    <scanWindow>\n");
            if (scan.ScanWindowLowerMz.HasValue)
            {
                AppendCvParam(sb, "     ", "MS:1000501", "scan window lower limit", Format(scan.ScanWindowLowerMz.Value), "MS:1000040", "m/z", "MS");
            }
            if (scan.ScanWindowUpperMz.HasValue)
            {
                AppendCvParam(sb, "     ", "MS:1000500", "scan window upper limit", Format(scan.ScanWindowUpperMz.Value), "MS:1000040", "m/z", "MS");
            }
            sb.Append("    </scanWindow>\n");
            sb.Append("   </scanWindowList>\n");
        }
        sb.Append("  </scan>\n");
        sb.Append(" </scanList>\n");
    }

    private void AppendPrecursorList(StringBuilder sb, RaxportScanRecord scan)
    {
        RaxportReactionRecord reaction = scan.Reaction!;
        sb.Append(" <precursorList count=\"1\">\n");
        sb.Append("  <precursor");
        if (scan.ParentScanNumber > 0)
        {
            sb.Append(" spectrumRef=\"").Append(XmlEscape(SpectrumId(scan.ParentScanNumber))).Append("\"");
        }
        sb.Append(">\n");
        sb.Append("   <isolationWindow>\n");
        AppendCvParam(sb, "    ", "MS:1000827", "isolation window target m/z", Format(reaction.PrecursorMass), "MS:1000040", "m/z", "MS");
        if (reaction.IsolationWidth > 0)
        {
            double upperOffset = reaction.IsolationWidth / 2.0 + reaction.IsolationWidthOffset;
            double lowerOffset = reaction.IsolationWidth - upperOffset;
            AppendCvParam(sb, "    ", "MS:1000828", "isolation window lower offset", Format(lowerOffset), "MS:1000040", "m/z", "MS");
            AppendCvParam(sb, "    ", "MS:1000829", "isolation window upper offset", Format(upperOffset), "MS:1000040", "m/z", "MS");
        }
        if (reaction.OneOverK0Begin != 0 || reaction.OneOverK0End != 0)
        {
            AppendUserParam(sb, "    ", "one_over_k0_begin", Format(reaction.OneOverK0Begin), "xsd:double");
            AppendUserParam(sb, "    ", "one_over_k0_end", Format(reaction.OneOverK0End), "xsd:double");
        }
        sb.Append("   </isolationWindow>\n");
        sb.Append("   <selectedIonList count=\"1\">\n");
        sb.Append("    <selectedIon>\n");
        double selectedMz = reaction.Candidates.Count > 0 ? reaction.Candidates[0].Mz : reaction.PrecursorMass;
        double selectedIntensity = reaction.Candidates.Count > 0 ? reaction.Candidates[0].Intensity : 0;
        AppendCvParam(sb, "     ", "MS:1000744", "selected ion m/z", Format(selectedMz), "MS:1000040", "m/z", "MS");
        if (reaction.ChargeState != 0)
        {
            AppendCvParam(sb, "     ", "MS:1000041", "charge state", reaction.ChargeState.ToString(CultureInfo.InvariantCulture));
        }
        if (selectedIntensity > 0)
        {
            AppendCvParam(sb, "     ", "MS:1000042", "peak intensity", Format(selectedIntensity), "MS:1000131", "number of detector counts", "MS");
        }
        AppendPrecursorCandidateUserParams(sb, "     ", reaction.Candidates);
        sb.Append("    </selectedIon>\n");
        sb.Append("   </selectedIonList>\n");
        sb.Append("   <activation>\n");
        if (reaction.CollisionEnergyValid)
        {
            AppendCvParam(sb, "    ", "MS:1000045", "collision energy", Format(reaction.CollisionEnergy), "UO:0000266", "electronvolt", "UO");
        }
        AppendActivationCvParam(sb, reaction.ActivationType);
        sb.Append("   </activation>\n");
        sb.Append("  </precursor>\n");
        sb.Append(" </precursorList>\n");
    }

    private static void AppendPrecursorCandidateUserParams(
        StringBuilder sb,
        string indent,
        IReadOnlyList<RaxportPrecursorCandidateRecord> candidates)
    {
        AppendUserParam(sb, indent, "Raxport precursor candidate count", candidates.Count.ToString(CultureInfo.InvariantCulture), "xsd:int");
        for (int i = 0; i < candidates.Count; i++)
        {
            RaxportPrecursorCandidateRecord candidate = candidates[i];
            string prefix = "Raxport precursor candidate " + i.ToString(CultureInfo.InvariantCulture) + " ";
            AppendUserParam(sb, indent, prefix + "charge", candidate.Charge.ToString(CultureInfo.InvariantCulture), "xsd:int");
            AppendUserParam(sb, indent, prefix + "mz", Format(candidate.Mz), "xsd:double");
            AppendUserParam(sb, indent, prefix + "intensity", Format(candidate.Intensity), "xsd:double");
            AppendUserParam(sb, indent, prefix + "one_over_k0", Format(candidate.OneOverK0), "xsd:double");
        }
    }

    private void AppendBinaryDataArrayList(StringBuilder sb, IReadOnlyList<RaxportPeakRecord> peaks)
    {
        bool hasCharge = false;
        bool hasResolution = false;
        bool hasBaseline = false;
        bool hasSignalToNoise = false;
        bool hasMobilityTrace = false;
        for (int i = 0; i < peaks.Count; i++)
        {
            RaxportPeakRecord peak = peaks[i];
            hasCharge |= peak.Charge != 0;
            hasResolution |= peak.Resolution != 0;
            hasBaseline |= peak.Baseline != 0;
            hasSignalToNoise |= peak.Noise != 0;
            hasMobilityTrace |= (peak.MobilityTrace?.Count ?? 0) > 0;
        }

        int arrayCount = 2 + (hasCharge ? 1 : 0) + (hasResolution ? 1 : 0) + (hasBaseline ? 1 : 0) + (hasSignalToNoise ? 1 : 0) + (hasMobilityTrace ? 4 : 0);
        sb.Append(" <binaryDataArrayList count=\"").Append(arrayCount.ToString(CultureInfo.InvariantCulture)).Append("\">\n");
        AppendPeakBinaryDataArray(sb, peaks, PeakArrayKind.Mz, "MS:1000514", "m/z array", "MS:1000040", "m/z", "MS");
        AppendPeakBinaryDataArray(sb, peaks, PeakArrayKind.Intensity, "MS:1000515", "intensity array", "MS:1000131", "number of counts", "MS");
        if (hasCharge)
        {
            AppendPeakBinaryDataArray(sb, peaks, PeakArrayKind.Charge, "MS:1000516", "charge array");
        }
        if (hasResolution)
        {
            AppendPeakBinaryDataArray(sb, peaks, PeakArrayKind.Resolution, "MS:1002529", "resolution array");
        }
        if (hasBaseline)
        {
            AppendPeakBinaryDataArray(sb, peaks, PeakArrayKind.Baseline, "MS:1002745", "sampled noise baseline array");
        }
        if (hasSignalToNoise)
        {
            AppendUserPeakBinaryDataArray(sb, peaks, PeakArrayKind.SignalToNoise, "Raxport signal-to-noise array");
        }
        if (hasMobilityTrace)
        {
            AppendMobilityTraceBinaryDataArrays(sb, peaks);
        }
        sb.Append(" </binaryDataArrayList>\n");
    }

    private string BuildBasePeakChromatogramXml(string id)
    {
        StringBuilder sb = new();
        sb.Append("<chromatogram index=\"0\" id=\"").Append(id).Append("\" defaultArrayLength=\"").Append(chromatogramTimes.Count.ToString(CultureInfo.InvariantCulture)).Append("\">\n");
        AppendCvParam(sb, " ", "MS:1000628", "basepeak chromatogram");
        sb.Append(" <binaryDataArrayList count=\"2\">\n");
        AppendDoubleListBinaryDataArray(sb, chromatogramTimes, "MS:1000595", "time array", "UO:0000031", "minute", "UO");
        AppendDoubleListBinaryDataArray(sb, basePeakIntensities, "MS:1000515", "intensity array", "MS:1000131", "number of counts", "MS");
        sb.Append(" </binaryDataArrayList>\n");
        sb.Append("</chromatogram>\n");
        return sb.ToString();
    }

    private string BuildIndexList(long spectrumBaseOffset, IReadOnlyList<IndexEntry> chromatogramIndex)
    {
        int indexCount = (spectrumIndex.Count > 0 ? 1 : 0) + (chromatogramIndex.Count > 0 ? 1 : 0);
        StringBuilder sb = new();
        sb.Append(" <indexList count=\"").Append(indexCount.ToString(CultureInfo.InvariantCulture)).Append("\">\n");
        if (spectrumIndex.Count > 0)
        {
            sb.Append("  <index name=\"spectrum\">\n");
            foreach (IndexEntry entry in spectrumIndex)
            {
                sb.Append("   <offset idRef=\"").Append(XmlEscape(entry.Id)).Append("\">").Append((spectrumBaseOffset + entry.Offset).ToString(CultureInfo.InvariantCulture)).Append("</offset>\n");
            }
            sb.Append("  </index>\n");
        }
        if (chromatogramIndex.Count > 0)
        {
            sb.Append("  <index name=\"chromatogram\">\n");
            foreach (IndexEntry entry in chromatogramIndex)
            {
                sb.Append("   <offset idRef=\"").Append(XmlEscape(entry.Id)).Append("\">").Append(entry.Offset.ToString(CultureInfo.InvariantCulture)).Append("</offset>\n");
            }
            sb.Append("  </index>\n");
        }
        sb.Append(" </indexList>\n");
        return sb.ToString();
    }

    private enum PeakArrayKind
    {
        Mz,
        Intensity,
        Charge,
        Resolution,
        Baseline,
        SignalToNoise
    }

    private static void AppendPeakBinaryDataArray(
        StringBuilder sb,
        IReadOnlyList<RaxportPeakRecord> peaks,
        PeakArrayKind kind,
        string arrayAccession,
        string arrayName,
        string? unitAccession = null,
        string? unitName = null,
        string? unitCvRef = null)
    {
        string binary = GetZLib64BitBase64(peaks, kind);
        AppendBinaryDataArray(sb, binary, peaks.Count, arrayAccession, arrayName, unitAccession, unitName, unitCvRef);
    }

    private static void AppendDoubleListBinaryDataArray(
        StringBuilder sb,
        IReadOnlyList<double> values,
        string arrayAccession,
        string arrayName,
        string? unitAccession = null,
        string? unitName = null,
        string? unitCvRef = null)
    {
        string binary = GetZLib64BitBase64(values);
        AppendBinaryDataArray(sb, binary, values.Count, arrayAccession, arrayName, unitAccession, unitName, unitCvRef);
    }

    private static void AppendBinaryDataArray(
        StringBuilder sb,
        string binary,
        int valueCount,
        string arrayAccession,
        string arrayName,
        string? unitAccession = null,
        string? unitName = null,
        string? unitCvRef = null)
    {
        sb.Append("  <binaryDataArray encodedLength=\"").Append(binary.Length.ToString(CultureInfo.InvariantCulture)).Append("\">\n");
        AppendCvParam(sb, "   ", arrayAccession, arrayName, string.Empty, unitAccession, unitName, unitCvRef);
        AppendCvParam(sb, "   ", "MS:1000523", "64-bit float");
        AppendCompressionCvParam(sb, valueCount);
        sb.Append("   <binary>").Append(binary).Append("</binary>\n");
        sb.Append("  </binaryDataArray>\n");
    }

    private static void AppendUserPeakBinaryDataArray(StringBuilder sb, IReadOnlyList<RaxportPeakRecord> peaks, PeakArrayKind kind, string arrayName)
    {
        string binary = GetZLib64BitBase64(peaks, kind);
        sb.Append("  <binaryDataArray encodedLength=\"").Append(binary.Length.ToString(CultureInfo.InvariantCulture)).Append("\">\n");
        AppendUserParam(sb, "   ", arrayName, string.Empty, "xsd:string");
        AppendCvParam(sb, "   ", "MS:1000523", "64-bit float");
        AppendCompressionCvParam(sb, peaks.Count);
        sb.Append("   <binary>").Append(binary).Append("</binary>\n");
        sb.Append("  </binaryDataArray>\n");
    }

    private static void AppendMobilityTraceBinaryDataArrays(StringBuilder sb, IReadOnlyList<RaxportPeakRecord> peaks)
    {
        List<double> traceStart = new(peaks.Count);
        List<double> traceCount = new(peaks.Count);
        List<double> traceOneOverK0Index = new();
        List<double> traceIntensity = new();
        for (int i = 0; i < peaks.Count; i++)
        {
            RaxportPeakMobilityTrace? trace = peaks[i].MobilityTrace;
            int count = trace?.Count ?? 0;
            if (count > 0)
            {
                traceStart.Add(traceOneOverK0Index.Count);
                traceCount.Add(count);
                for (int pointIndex = 0; pointIndex < count; pointIndex++)
                {
                    traceOneOverK0Index.Add(trace!.OneOverK0Indices[pointIndex]);
                    traceIntensity.Add(trace.Intensities[pointIndex]);
                }
            }
            else
            {
                traceStart.Add(-1);
                traceCount.Add(0);
            }
        }

        AppendUserDoubleListBinaryDataArray(sb, traceStart, "Raxport mobility trace start array");
        AppendUserDoubleListBinaryDataArray(sb, traceCount, "Raxport mobility trace count array");
        AppendUserDoubleListBinaryDataArray(sb, traceOneOverK0Index, "Raxport mobility trace one_over_k0_index array");
        AppendUserDoubleListBinaryDataArray(sb, traceIntensity, "Raxport mobility trace intensity array");
    }

    private static void AppendUserDoubleListBinaryDataArray(StringBuilder sb, IReadOnlyList<double> values, string arrayName)
    {
        string binary = GetZLib64BitBase64(values);
        sb.Append("  <binaryDataArray encodedLength=\"").Append(binary.Length.ToString(CultureInfo.InvariantCulture)).Append("\">\n");
        AppendUserParam(sb, "   ", arrayName, string.Empty, "xsd:string");
        AppendCvParam(sb, "   ", "MS:1000523", "64-bit float");
        AppendCompressionCvParam(sb, values.Count);
        sb.Append("   <binary>").Append(binary).Append("</binary>\n");
        sb.Append("  </binaryDataArray>\n");
    }

    private static void AppendCompressionCvParam(StringBuilder sb, int valueCount)
    {
        AppendCvParam(sb, "   ", valueCount == 0 ? "MS:1000576" : "MS:1000574", valueCount == 0 ? "no compression" : "zlib compression");
    }

    private static string GetZLib64BitBase64(IReadOnlyList<RaxportPeakRecord> peaks, PeakArrayKind kind)
    {
        if (peaks.Count == 0)
        {
            return string.Empty;
        }

        int byteCount = checked(peaks.Count * sizeof(double));
        byte[] bytes = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            Span<byte> span = bytes.AsSpan(0, byteCount);
            int offset = 0;
            for (int i = 0; i < peaks.Count; i++)
            {
                BinaryPrimitives.WriteDoubleLittleEndian(span.Slice(offset, sizeof(double)), GetPeakArrayValue(peaks[i], kind));
                offset += sizeof(double);
            }
            return Compress64BitBytesToBase64(bytes, byteCount);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bytes);
        }
    }

    private static string GetZLib64BitBase64(IReadOnlyList<double> values)
    {
        if (values.Count == 0)
        {
            return string.Empty;
        }

        int byteCount = checked(values.Count * sizeof(double));
        byte[] bytes = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            Span<byte> span = bytes.AsSpan(0, byteCount);
            int offset = 0;
            for (int i = 0; i < values.Count; i++)
            {
                BinaryPrimitives.WriteDoubleLittleEndian(span.Slice(offset, sizeof(double)), values[i]);
                offset += sizeof(double);
            }
            return Compress64BitBytesToBase64(bytes, byteCount);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bytes);
        }
    }

    private static string Compress64BitBytesToBase64(byte[] bytes, int byteCount)
    {
        using MemoryStream memoryStream = new();
        using (ZLibStream zlib = new(memoryStream, BinaryArrayCompressionLevel, leaveOpen: true))
        {
            zlib.Write(bytes, 0, byteCount);
        }
        return Convert.ToBase64String(memoryStream.GetBuffer(), 0, checked((int)memoryStream.Length));
    }

    private static double GetPeakArrayValue(RaxportPeakRecord peak, PeakArrayKind kind)
    {
        return kind switch
        {
            PeakArrayKind.Mz => peak.Mz,
            PeakArrayKind.Intensity => peak.Intensity,
            PeakArrayKind.Charge => peak.Charge,
            PeakArrayKind.Resolution => peak.Resolution,
            PeakArrayKind.Baseline => peak.Baseline,
            PeakArrayKind.SignalToNoise => peak.Noise,
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
        };
    }

    private static void AppendPolarity(StringBuilder sb, string polarity)
    {
        if (string.Equals(polarity, "Positive", StringComparison.OrdinalIgnoreCase))
        {
            AppendCvParam(sb, " ", "MS:1000130", "positive scan");
        }
        else if (string.Equals(polarity, "Negative", StringComparison.OrdinalIgnoreCase))
        {
            AppendCvParam(sb, " ", "MS:1000129", "negative scan");
        }
    }

    private static void AppendActivationCvParam(StringBuilder sb, string activationType)
    {
        string normalized = activationType.Replace(" ", string.Empty, StringComparison.OrdinalIgnoreCase);
        if (normalized.Contains("HigherEnergyCollisionalDissociation", StringComparison.OrdinalIgnoreCase) ||
            normalized.Contains("HCD", StringComparison.OrdinalIgnoreCase))
        {
            AppendCvParam(sb, "    ", "MS:1000422", "beam-type collision-induced dissociation");
        }
        else if (normalized.Contains("CollisionInducedDissociation", StringComparison.OrdinalIgnoreCase) ||
                 normalized.Contains("CID", StringComparison.OrdinalIgnoreCase))
        {
            AppendCvParam(sb, "    ", "MS:1000133", "collision-induced dissociation");
        }
        else if (normalized.Contains("ElectronTransferDissociation", StringComparison.OrdinalIgnoreCase) ||
                 normalized.Contains("ETD", StringComparison.OrdinalIgnoreCase))
        {
            AppendCvParam(sb, "    ", "MS:1000598", "electron transfer dissociation");
        }
        else if (normalized.Contains("ElectronCaptureDissociation", StringComparison.OrdinalIgnoreCase) ||
                 normalized.Contains("ECD", StringComparison.OrdinalIgnoreCase))
        {
            AppendCvParam(sb, "    ", "MS:1000250", "electron capture dissociation");
        }
        else
        {
            AppendCvParam(sb, "    ", "MS:1000044", "Activation Method");
            if (!string.IsNullOrWhiteSpace(activationType))
            {
                AppendUserParam(sb, "    ", "activation type", activationType, "xsd:string");
            }
        }
    }

    private static void AppendCvParam(
        StringBuilder sb,
        string indent,
        string accession,
        string name,
        string? value = "",
        string? unitAccession = null,
        string? unitName = null,
        string? unitCvRef = null,
        string cvRef = "MS")
    {
        sb.Append(indent).Append("<cvParam cvRef=\"").Append(cvRef).Append("\" accession=\"").Append(accession).Append("\" name=\"").Append(XmlEscape(name)).Append("\" value=\"").Append(XmlEscape(value ?? string.Empty)).Append("\"");
        if (!string.IsNullOrWhiteSpace(unitAccession) && !string.IsNullOrWhiteSpace(unitName) && !string.IsNullOrWhiteSpace(unitCvRef))
        {
            sb.Append(" unitCvRef=\"").Append(unitCvRef).Append("\" unitAccession=\"").Append(unitAccession).Append("\" unitName=\"").Append(XmlEscape(unitName)).Append("\"");
        }
        sb.Append(" />\n");
    }

    private static void AppendUserParam(StringBuilder sb, string indent, string name, string value, string type)
    {
        sb.Append(indent).Append("<userParam name=\"").Append(XmlEscape(name)).Append("\" type=\"").Append(type).Append("\" value=\"").Append(XmlEscape(value)).Append("\" />\n");
    }

    private static void WriteString(FileStream output, SHA1 sha1, string text, bool hash)
    {
        byte[] bytes = Utf8NoBom.GetBytes(text);
        WriteBytes(output, sha1, bytes, bytes.Length, hash);
    }

    private static void WriteBytes(FileStream output, SHA1 sha1, byte[] bytes, int count, bool hash)
    {
        if (hash && count > 0)
        {
            sha1.TransformBlock(bytes, 0, count, null, 0);
        }
        output.Write(bytes, 0, count);
    }

    private void WriteTimerStart()
    {
        writeTimer.Restart();
    }

    private void WriteTimerStop()
    {
        writeTimer.Stop();
        WriteElapsed += writeTimer.Elapsed;
    }

    private static string SpectrumId(int scanNumber)
    {
        return "controllerType=0 controllerNumber=1 scan=" + scanNumber.ToString(CultureInfo.InvariantCulture);
    }

    private static string Format(double value)
    {
        return value.ToString("R", CultureInfo.InvariantCulture);
    }

    private static string XmlEscape(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return string.Empty;
        }

        return value
            .Replace("&", "&amp;", StringComparison.Ordinal)
            .Replace("\"", "&quot;", StringComparison.Ordinal)
            .Replace("<", "&lt;", StringComparison.Ordinal)
            .Replace(">", "&gt;", StringComparison.Ordinal)
            .Replace("'", "&apos;", StringComparison.Ordinal);
    }

    private static string ToNcName(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "Raxport";
        }

        StringBuilder sb = new(value.Length + 1);
        for (int i = 0; i < value.Length; i++)
        {
            char c = value[i];
            sb.Append(char.IsLetterOrDigit(c) || c == '_' || c == '-' || c == '.' ? c : '_');
        }
        if (char.IsDigit(sb[0]))
        {
            sb.Insert(0, '_');
        }
        return sb.ToString();
    }

    private static string GetSourceLocation(string sourcePath)
    {
        string fullPath = Path.GetFullPath(sourcePath).Replace(Path.DirectorySeparatorChar, '/');
        return "file:///" + fullPath.TrimStart('/');
    }

    private static string GetSourceTimestamp(string sourcePath)
    {
        DateTime timestamp = DateTime.UtcNow;
        if (File.Exists(sourcePath))
        {
            timestamp = File.GetCreationTimeUtc(sourcePath);
        }
        else if (Directory.Exists(sourcePath))
        {
            timestamp = Directory.GetCreationTimeUtc(sourcePath);
        }
        return timestamp.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
    }

    private static string? TryCalculateSourceChecksum(string sourcePath)
    {
        if (!File.Exists(sourcePath))
        {
            return null;
        }

        using SHA1 sha1 = SHA1.Create();
        using FileStream stream = File.OpenRead(sourcePath);
        return Convert.ToHexString(sha1.ComputeHash(stream)).ToLowerInvariant();
    }

    private sealed record IndexEntry(string Id, long Offset);
}
