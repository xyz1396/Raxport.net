namespace Raxport;

internal static class PrecursorSelector
{
    private const double NeutronMass = 1.003355;
    private const double IsotopeEvidenceMzPadding = 1.5;
    private const int StrongIsotopeMatchCount = 3;
    private const int MinTrustedPrecursorCharge = 1;
    private const int MaxTrustedPrecursorCharge = 7;
    private static readonly int[] DefaultGuessedCharges = { 2, 3, 4 };
    private static readonly int[] ChargesInConsideration = { 2, 3, 4, 5, 6, 1 };
    private static readonly int[] PopularChargeOrder = { 2, 3, 4, 5, 6, 1 };
    private static readonly int[] ConservativeFallbackCharges = { 2, 3, 4 };
    private static readonly int[] IsotopeOffsets = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    public static List<RaxportPeakRecord> FindPrecursorPeaks(
        IReadOnlyList<RaxportPeakRecord> peaks,
        double precursorMz,
        double isolationWindow,
        int topN,
        double intensityRatio,
        double mzTolerancePpm,
        double? oneOverK0Begin = null,
        double? oneOverK0End = null,
        IReadOnlyList<double>? oneOverK0ByIndex = null,
        int preferredCharge = 0)
    {
        if (topN <= 0 || peaks.Count == 0)
        {
            return new List<RaxportPeakRecord>();
        }

        List<RaxportPeakRecord> evidencePeaks = GetPrecursorEvidencePeaks(
            peaks,
            precursorMz,
            isolationWindow,
            mzTolerancePpm,
            oneOverK0Begin,
            oneOverK0End,
            oneOverK0ByIndex);
        List<RaxportPeakRecord> isotopeEvidencePeaks = GetIsotopeEvidencePeaks(
            peaks,
            precursorMz,
            isolationWindow,
            mzTolerancePpm,
            oneOverK0Begin,
            oneOverK0End,
            oneOverK0ByIndex);

        return FindPrecursorPeaksFromEvidence(
            evidencePeaks,
            isotopeEvidencePeaks,
            topN,
            intensityRatio,
            mzTolerancePpm,
            preferredCharge);
    }

    public static List<RaxportPeakRecord> FindPrecursorPeaksFromEvidence(
        IReadOnlyList<RaxportPeakRecord> evidencePeaks,
        IReadOnlyList<RaxportPeakRecord> isotopeEvidencePeaks,
        int topN,
        double intensityRatio,
        double mzTolerancePpm,
        int preferredCharge = 0)
    {
        List<RaxportPeakRecord> precursorPeaks = new();
        if (topN <= 0 || evidencePeaks.Count == 0)
        {
            return precursorPeaks;
        }

        List<RaxportPeakRecord> intensityOrderedEvidencePeaks = evidencePeaks.OrderByDescending(peak => peak.Intensity).ToList();
        double totalIntensity = 0.000000001;
        foreach (RaxportPeakRecord peak in intensityOrderedEvidencePeaks)
        {
            totalIntensity += peak.Intensity;
        }

        List<RaxportPeakRecord> selectionPool = BuildHighIntensityPool(intensityOrderedEvidencePeaks, topN, intensityRatio, totalIntensity);
        double summedIntensity = 0;
        for (int i = 0; i < selectionPool.Count; i++)
        {
            if (summedIntensity / totalIntensity > intensityRatio)
            {
                break;
            }

            RaxportPeakRecord selectedPeak = selectionPool[i];
            precursorPeaks.Add(selectedPeak);
            summedIntensity += selectedPeak.Intensity;
            if (precursorPeaks.Count >= topN)
            {
                break;
            }

            IReadOnlyList<int> isotopeCharges = ResolveIsotopeRemovalCharges(
                selectedPeak,
                isotopeEvidencePeaks,
                preferredCharge,
                mzTolerancePpm);
            summedIntensity += RemoveIsotopicPeaks(selectionPool, selectedPeak, i, isotopeCharges, mzTolerancePpm);
        }

        return precursorPeaks;
    }

    public static List<RaxportPeakRecord> GetPrecursorEvidencePeaks(
        IReadOnlyList<RaxportPeakRecord> peaks,
        double precursorMz,
        double isolationWindow,
        double mzTolerancePpm,
        double? oneOverK0Begin = null,
        double? oneOverK0End = null,
        IReadOnlyList<double>? oneOverK0ByIndex = null)
    {
        if (peaks.Count == 0)
        {
            return new List<RaxportPeakRecord>();
        }

        List<RaxportPeakRecord> peaksInRange = FindPeaksInRange(
            peaks,
            precursorMz - isolationWindow / 2,
            precursorMz + isolationWindow / 2,
            mzTolerancePpm);
        if (oneOverK0Begin.HasValue && oneOverK0End.HasValue)
        {
            peaksInRange = peaksInRange
                .Select(peak => ProjectPeakToMobilityWindow(peak, oneOverK0Begin.Value, oneOverK0End.Value, oneOverK0ByIndex))
                .Where(peak => peak is not null)
                .Cast<RaxportPeakRecord>()
                .ToList();
        }

        return peaksInRange.OrderBy(peak => peak.Mz).ToList();
    }

    public static List<RaxportPeakRecord> GetIsotopeEvidencePeaks(
        IReadOnlyList<RaxportPeakRecord> peaks,
        double precursorMz,
        double isolationWindow,
        double mzTolerancePpm,
        double? oneOverK0Begin = null,
        double? oneOverK0End = null,
        IReadOnlyList<double>? oneOverK0ByIndex = null)
    {
        return GetPrecursorEvidencePeaks(
            peaks,
            precursorMz,
            isolationWindow + 2 * IsotopeEvidenceMzPadding,
            mzTolerancePpm,
            oneOverK0Begin,
            oneOverK0End,
            oneOverK0ByIndex);
    }

    private static List<RaxportPeakRecord> BuildHighIntensityPool(
        IReadOnlyList<RaxportPeakRecord> evidencePeaks,
        int topN,
        double intensityRatio,
        double totalIntensity)
    {
        List<RaxportPeakRecord> pool = new();
        int maxPoolCount = checked(topN * 2);
        double poolIntensity = 0;
        foreach (RaxportPeakRecord peak in evidencePeaks)
        {
            if (pool.Count >= maxPoolCount)
            {
                break;
            }

            pool.Add(peak);
            poolIntensity += peak.Intensity;
            if (poolIntensity / totalIntensity >= intensityRatio)
            {
                break;
            }
        }

        return pool;
    }

    private static IReadOnlyList<int> ResolveIsotopeRemovalCharges(
        RaxportPeakRecord selectedPeak,
        IReadOnlyList<RaxportPeakRecord> evidencePeaks,
        int preferredCharge,
        double mzTolerancePpm)
    {
        if (selectedPeak.Charge > 0)
        {
            return new[] { selectedPeak.Charge };
        }

        if (preferredCharge > 0)
        {
            return new[] { preferredCharge };
        }

        List<int> strongCharges = InferStrongChargesFromIsotopes(selectedPeak, evidencePeaks, mzTolerancePpm);
        if (strongCharges.Count > 0)
        {
            return strongCharges;
        }

        foreach (int charge in ConservativeFallbackCharges)
        {
            if (CountIsotopeMatches(selectedPeak, evidencePeaks, charge, mzTolerancePpm) > 0)
            {
                return new[] { charge };
            }
        }

        return Array.Empty<int>();
    }

    private static List<int> InferStrongChargesFromIsotopes(
        RaxportPeakRecord peak,
        IReadOnlyList<RaxportPeakRecord> evidencePeaks,
        double mzTolerancePpm)
    {
        List<int> charges = new();
        foreach (int charge in PopularChargeOrder)
        {
            if (CountIsotopeMatches(peak, evidencePeaks, charge, mzTolerancePpm) >= StrongIsotopeMatchCount)
            {
                charges.Add(charge);
            }
        }

        return charges;
    }

    private static double RemoveIsotopicPeaks(
        List<RaxportPeakRecord> peaks,
        RaxportPeakRecord selectedPeak,
        int selectedIndex,
        IReadOnlyList<int> isotopeCharges,
        double mzTolerancePpm)
    {
        if (isotopeCharges.Count == 0)
        {
            return 0;
        }

        double removedIntensity = 0;
        removedIntensity += RemoveIsotopicPeaksInDirection(peaks, selectedPeak, selectedIndex, isotopeCharges, mzTolerancePpm, 1);
        removedIntensity += RemoveIsotopicPeaksInDirection(peaks, selectedPeak, selectedIndex, isotopeCharges, mzTolerancePpm, -1);
        return removedIntensity;
    }

    private static double RemoveIsotopicPeaksInDirection(
        List<RaxportPeakRecord> peaks,
        RaxportPeakRecord selectedPeak,
        int selectedIndex,
        IReadOnlyList<int> isotopeCharges,
        double mzTolerancePpm,
        int direction)
    {
        double removedIntensity = 0;
        foreach (int isotopeOffset in IsotopeOffsets)
        {
            bool foundIsotopicPeak = false;
            for (int j = selectedIndex + 1; j < peaks.Count; j++)
            {

                if (CouldBeIsotopicPeak(selectedPeak, peaks[j], isotopeOffset, direction, isotopeCharges, mzTolerancePpm))
                {
                    foundIsotopicPeak = true;
                    removedIntensity += peaks[j].Intensity;
                    peaks.RemoveAt(j);
                    j--;
                }
            }

            if (!foundIsotopicPeak)
            {
                break;
            }
        }

        return removedIntensity;
    }

    public static List<RaxportPrecursorCandidateRecord> ExpandPrecursorCandidates(
        IEnumerable<RaxportPeakRecord> precursorPeaks,
        int maxSelectedPrecursorPeaks,
        int preferredCharge = 0)
    {
        List<RaxportPeakRecord> selectedPeaks = precursorPeaks.ToList();
        return ExpandPrecursorCandidates(
            selectedPeaks,
            selectedPeaks,
            0,
            0,
            maxSelectedPrecursorPeaks,
            mzTolerancePpm: 10,
            preferredCharge);
    }

    public static List<RaxportPrecursorCandidateRecord> ExpandPrecursorCandidates(
        IReadOnlyList<RaxportPeakRecord> precursorPeaks,
        IReadOnlyList<RaxportPeakRecord> evidencePeaks,
        double precursorMz,
        double isolationWindow,
        int maxSelectedPrecursorPeaks,
        double mzTolerancePpm,
        int preferredCharge = 0)
    {
        List<RaxportPrecursorCandidateRecord> candidates = new();
        if (maxSelectedPrecursorPeaks <= 0)
        {
            return candidates;
        }

        int expandedCandidateLimit = GetExpandedCandidateLimit(
            precursorPeaks,
            maxSelectedPrecursorPeaks,
            precursorMz,
            mzTolerancePpm,
            preferredCharge);

        foreach (RaxportPeakRecord peak in precursorPeaks)
        {
            if (IsTrustedPrecursorCharge(peak.Charge))
            {
                AddOrKeepBest(peak, peak.Charge);
            }
            else if (IsPreferredChargeForPeak(peak, precursorMz, mzTolerancePpm, preferredCharge))
            {
                AddOrKeepBest(peak, preferredCharge);
            }
            else
            {
                foreach (int guessedCharge in DefaultGuessedCharges)
                {
                    AddOrKeepBest(peak, guessedCharge);
                }
            }
        }

        return DeduplicateCandidates(candidates, mzTolerancePpm, expandedCandidateLimit);

        void AddOrKeepBest(RaxportPeakRecord peak, int charge)
        {
            if (charge <= 0)
            {
                return;
            }

            for (int i = 0; i < candidates.Count; i++)
            {
                RaxportPrecursorCandidateRecord candidate = candidates[i];
                if (candidate.Charge == charge && WithinMzTolerance(candidate.Mz, peak.Mz, mzTolerancePpm))
                {
                    if (peak.Intensity > candidate.Intensity)
                    {
                        candidates[i] = CreateCandidate(peak, charge);
                    }

                    return;
                }
            }

            candidates.Add(CreateCandidate(peak, charge));
        }
    }

    private static int GetExpandedCandidateLimit(
        IReadOnlyList<RaxportPeakRecord> precursorPeaks,
        int maxSelectedPrecursorPeaks,
        double precursorMz,
        double mzTolerancePpm,
        int preferredCharge)
    {
        int selectedPeakCount = Math.Min(maxSelectedPrecursorPeaks, precursorPeaks.Count);
        int candidateLimit = 0;
        for (int i = 0; i < selectedPeakCount; i++)
        {
            RaxportPeakRecord peak = precursorPeaks[i];
            bool hasTrustedCharge = IsTrustedPrecursorCharge(peak.Charge)
                || IsPreferredChargeForPeak(peak, precursorMz, mzTolerancePpm, preferredCharge);
            candidateLimit = checked(candidateLimit + (hasTrustedCharge ? 1 : DefaultGuessedCharges.Length));
        }

        return candidateLimit;
    }

    private static bool IsPreferredChargeForPeak(
        RaxportPeakRecord peak,
        double precursorMz,
        double mzTolerancePpm,
        int preferredCharge)
    {
        return IsTrustedPrecursorCharge(preferredCharge)
            && precursorMz > 0
            && WithinMzTolerance(peak.Mz, precursorMz, mzTolerancePpm);
    }

    private static List<RaxportPrecursorCandidateRecord> DeduplicateCandidates(
        IReadOnlyList<RaxportPrecursorCandidateRecord> candidates,
        double mzTolerancePpm,
        int maxCandidates)
    {
        List<RaxportPrecursorCandidateRecord> deduplicated = new();
        foreach (RaxportPrecursorCandidateRecord candidate in candidates)
        {
            bool merged = false;
            for (int i = 0; i < deduplicated.Count; i++)
            {
                RaxportPrecursorCandidateRecord existing = deduplicated[i];
                if (existing.Charge == candidate.Charge && WithinMzTolerance(existing.Mz, candidate.Mz, mzTolerancePpm))
                {
                    if (candidate.Intensity > existing.Intensity)
                    {
                        deduplicated[i] = candidate;
                    }

                    merged = true;
                    break;
                }
            }

            if (!merged && deduplicated.Count < maxCandidates)
            {
                deduplicated.Add(candidate);
            }
        }

        return deduplicated;
    }

    private static RaxportPrecursorCandidateRecord CreateCandidate(RaxportPeakRecord peak, int charge)
    {
        return new RaxportPrecursorCandidateRecord(charge, peak.Mz, peak.Intensity, peak.CandidateOneOverK0);
    }

    private static bool IsTrustedPrecursorCharge(int charge)
    {
        return charge >= MinTrustedPrecursorCharge && charge <= MaxTrustedPrecursorCharge;
    }

    private static int InferChargeFromIsotopes(
        RaxportPeakRecord peak,
        IReadOnlyList<RaxportPeakRecord> evidencePeaks,
        double mzTolerancePpm)
    {
        int bestCharge = 0;
        int bestScore = 0;
        foreach (int charge in ChargesInConsideration)
        {
            int score = CountIsotopeMatches(peak, evidencePeaks, charge, mzTolerancePpm);
            if (score > bestScore)
            {
                bestScore = score;
                bestCharge = charge;
            }
        }

        return bestScore > 0 ? bestCharge : 0;
    }

    private static int CountIsotopeMatches(
        RaxportPeakRecord peak,
        IReadOnlyList<RaxportPeakRecord> evidencePeaks,
        int charge,
        double mzTolerancePpm)
    {
        int matches = 0;
        foreach (int isotopeOffset in IsotopeOffsets)
        {
            double expectedMz = peak.Mz + isotopeOffset * NeutronMass / charge;
            bool found = false;
            foreach (RaxportPeakRecord evidencePeak in evidencePeaks)
            {
                if (evidencePeak.Mz <= peak.Mz)
                {
                    continue;
                }

                if (WithinMzTolerance(evidencePeak.Mz, expectedMz, mzTolerancePpm))
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                break;
            }

            matches++;
        }

        return matches;
    }

    private static RaxportPeakRecord? ProjectPeakToMobilityWindow(
        RaxportPeakRecord peak,
        double oneOverK0Begin,
        double oneOverK0End,
        IReadOnlyList<double>? oneOverK0ByIndex)
    {
        RaxportPeakMobilityTrace? trace = peak.MobilityTrace;
        if (trace is null || trace.Count == 0 || oneOverK0ByIndex is null || oneOverK0ByIndex.Count == 0)
        {
            return null;
        }

        double lower = Math.Min(oneOverK0Begin, oneOverK0End);
        double upper = Math.Max(oneOverK0Begin, oneOverK0End);
        double summedIntensity = 0;
        double bestIntensity = double.MinValue;
        double bestOneOverK0 = 0;
        for (int i = 0; i < trace.Count; i++)
        {
            int oneOverK0Index = trace.OneOverK0Indices[i];
            if ((uint)oneOverK0Index >= (uint)oneOverK0ByIndex.Count)
            {
                continue;
            }

            double oneOverK0 = oneOverK0ByIndex[oneOverK0Index];
            if (oneOverK0 < lower || oneOverK0 > upper)
            {
                continue;
            }

            float intensity = trace.Intensities[i];
            summedIntensity += intensity;
            if (intensity > bestIntensity)
            {
                bestIntensity = intensity;
                bestOneOverK0 = oneOverK0;
            }
        }

        if (summedIntensity <= 0)
        {
            return null;
        }

        return peak with { Intensity = summedIntensity, CandidateOneOverK0 = bestOneOverK0 };
    }

    private static List<RaxportPeakRecord> FindPeaksInRange(
        IReadOnlyList<RaxportPeakRecord> peaks,
        double start,
        double end,
        double mzTolerancePpm)
    {
        List<RaxportPeakRecord> peaksInRange = new();
        double lowerBound = start - MzToleranceDa(start, mzTolerancePpm);
        double upperBound = end + MzToleranceDa(end, mzTolerancePpm);
        int low = 0;
        int high = peaks.Count - 1;
        while (low <= high)
        {
            int mid = (low + high) / 2;
            if (peaks[mid].Mz < lowerBound)
            {
                low = mid + 1;
            }
            else
            {
                high = mid - 1;
            }
        }

        for (int i = low; i < peaks.Count; i++)
        {
            if (peaks[i].Mz > upperBound)
            {
                break;
            }

            peaksInRange.Add(peaks[i]);
        }

        return peaksInRange;
    }

    private static bool CouldBeIsotopicPeak(
        RaxportPeakRecord selectedPeak,
        RaxportPeakRecord candidatePeak,
        int isotopeOffset,
        int direction,
        IReadOnlyList<int> isotopeCharges,
        double mzTolerancePpm)
    {
        double expectedSign = Math.Sign(direction);
        if (expectedSign == 0)
        {
            return false;
        }

        double observedMzSpacing = candidatePeak.Mz - selectedPeak.Mz;
        if (Math.Sign(observedMzSpacing) != expectedSign)
        {
            return false;
        }

        foreach (int charge in isotopeCharges)
        {
            double expectedMz = selectedPeak.Mz + expectedSign * isotopeOffset * NeutronMass / charge;
            if (WithinMzTolerance(candidatePeak.Mz, expectedMz, mzTolerancePpm))
            {
                return true;
            }
        }

        return false;
    }

    private static bool WithinMzTolerance(double observedMz, double expectedMz, double mzTolerancePpm)
    {
        return Math.Abs(observedMz - expectedMz) <= MzToleranceDa(expectedMz, mzTolerancePpm);
    }

    private static double MzToleranceDa(double mz, double mzTolerancePpm)
    {
        return Math.Abs(mz) * mzTolerancePpm / 1_000_000.0;
    }
}
