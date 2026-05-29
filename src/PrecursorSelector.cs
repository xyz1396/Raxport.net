namespace Raxport;

internal static class PrecursorSelector
{
    private const double NeutronMass = 1.003355;
    private const int StrongIsotopeMatchCount = 3;
    private static readonly int[] DefaultGuessedCharges = { 2, 3, 4 };
    private static readonly int[] ChargesInConsideration = { 1, 2, 3, 4, 5, 6 };
    private static readonly int[] IsotopeOffsets = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    public static List<Hdf5PeakRecord> FindPrecursorPeaks(
        IReadOnlyList<Hdf5PeakRecord> peaks,
        double precursorMz,
        double isolationWindow,
        int topN,
        double intensityRatio,
        double mzTolerancePpm,
        double? oneOverK0Begin = null,
        double? oneOverK0End = null,
        IReadOnlyList<double>? oneOverK0ByIndex = null)
    {
        List<Hdf5PeakRecord> precursorPeaks = new();
        if (topN <= 0 || peaks.Count == 0)
        {
            return precursorPeaks;
        }

        List<Hdf5PeakRecord> peaksInRange = GetPrecursorEvidencePeaks(
            peaks,
            precursorMz,
            isolationWindow,
            mzTolerancePpm,
            oneOverK0Begin,
            oneOverK0End,
            oneOverK0ByIndex);
        peaksInRange = peaksInRange.OrderByDescending(o => o.Intensity).ToList();

        double totalIntensity = 0.000000001;
        foreach (Hdf5PeakRecord peak in peaksInRange)
        {
            totalIntensity += peak.Intensity;
        }

        double summedIntensity = 0;
        for (int i = 0; i < peaksInRange.Count; i++)
        {
            if (summedIntensity / totalIntensity > intensityRatio)
            {
                break;
            }

            Hdf5PeakRecord selectedPeak = peaksInRange[i];
            precursorPeaks.Add(selectedPeak);
            summedIntensity += selectedPeak.Intensity;
            if (precursorPeaks.Count >= topN)
            {
                break;
            }

            foreach (int isotopeOffset in IsotopeOffsets)
            {
                bool foundIsotopicPeak = false;
                for (int j = i + 1; j < peaksInRange.Count; j++)
                {
                    if (CouldBeIsotopicPeak(selectedPeak, peaksInRange[j], isotopeOffset, precursorMz, mzTolerancePpm))
                    {
                        foundIsotopicPeak = true;
                        summedIntensity += peaksInRange[j].Intensity;
                        peaksInRange.RemoveAt(j);
                        j--;
                    }
                }

                if (!foundIsotopicPeak)
                {
                    break;
                }
            }
        }

        return precursorPeaks;
    }

    public static List<Hdf5PeakRecord> GetPrecursorEvidencePeaks(
        IReadOnlyList<Hdf5PeakRecord> peaks,
        double precursorMz,
        double isolationWindow,
        double mzTolerancePpm,
        double? oneOverK0Begin = null,
        double? oneOverK0End = null,
        IReadOnlyList<double>? oneOverK0ByIndex = null)
    {
        if (peaks.Count == 0)
        {
            return new List<Hdf5PeakRecord>();
        }

        List<Hdf5PeakRecord> peaksInRange = FindPeaksInRange(
            peaks,
            precursorMz - isolationWindow / 2,
            precursorMz + isolationWindow / 2,
            mzTolerancePpm);
        if (oneOverK0Begin.HasValue && oneOverK0End.HasValue)
        {
            peaksInRange = peaksInRange
                .Select(peak => ProjectPeakToMobilityWindow(peak, oneOverK0Begin.Value, oneOverK0End.Value, oneOverK0ByIndex))
                .Where(peak => peak is not null)
                .Cast<Hdf5PeakRecord>()
                .ToList();
        }

        return peaksInRange.OrderBy(peak => peak.Mz).ToList();
    }

    public static List<Hdf5PrecursorCandidateRecord> ExpandPrecursorCandidates(
        IEnumerable<Hdf5PeakRecord> precursorPeaks,
        int maxCandidates,
        int preferredCharge = 0)
    {
        List<Hdf5PeakRecord> selectedPeaks = precursorPeaks.ToList();
        return ExpandPrecursorCandidates(
            selectedPeaks,
            selectedPeaks,
            0,
            0,
            maxCandidates,
            mzTolerancePpm: 10,
            preferredCharge);
    }

    public static List<Hdf5PrecursorCandidateRecord> ExpandPrecursorCandidates(
        IReadOnlyList<Hdf5PeakRecord> precursorPeaks,
        IReadOnlyList<Hdf5PeakRecord> evidencePeaks,
        double precursorMz,
        double isolationWindow,
        int maxCandidates,
        double mzTolerancePpm,
        int preferredCharge = 0)
    {
        List<Hdf5PrecursorCandidateRecord> candidates = new();
        if (maxCandidates <= 0)
        {
            return candidates;
        }

        List<Hdf5PeakRecord> unknownChargePeaks = new();
        foreach (Hdf5PeakRecord peak in precursorPeaks)
        {
            if (peak.Charge > 0)
            {
                AddOrKeepBest(peak, peak.Charge);
            }
            else
            {
                unknownChargePeaks.Add(peak);
                if (preferredCharge > 0)
                {
                    AddOrKeepBest(peak, preferredCharge);
                }
            }

        }

        if (candidates.Count < maxCandidates)
        {
            foreach (Hdf5PeakRecord peak in unknownChargePeaks)
            {
                int inferredCharge = InferChargeFromIsotopes(peak, evidencePeaks, mzTolerancePpm);
                if (inferredCharge > 0)
                {
                    AddOrKeepBest(peak, inferredCharge);
                }
                else
                {
                    foreach (int guessedCharge in DefaultGuessedCharges)
                    {
                        AddOrKeepBest(peak, guessedCharge);
                        if (candidates.Count >= maxCandidates)
                        {
                            break;
                        }
                    }
                }

                if (candidates.Count >= maxCandidates)
                {
                    break;
                }
            }
        }

        return candidates;

        void AddOrKeepBest(Hdf5PeakRecord peak, int charge)
        {
            if (charge <= 0)
            {
                return;
            }

            for (int i = 0; i < candidates.Count; i++)
            {
                Hdf5PrecursorCandidateRecord candidate = candidates[i];
                if (candidate.Charge == charge && WithinMzTolerance(candidate.Mz, peak.Mz, mzTolerancePpm))
                {
                    if (peak.Intensity > candidate.Intensity)
                    {
                        candidates[i] = CreateCandidate(peak, charge);
                    }

                    return;
                }
            }

            if (candidates.Count < maxCandidates)
            {
                candidates.Add(CreateCandidate(peak, charge));
            }
        }
    }

    private static Hdf5PrecursorCandidateRecord CreateCandidate(Hdf5PeakRecord peak, int charge)
    {
        return new Hdf5PrecursorCandidateRecord(charge, peak.Mz, peak.Intensity, peak.CandidateOneOverK0);
    }

    private static int InferChargeFromIsotopes(
        Hdf5PeakRecord peak,
        IReadOnlyList<Hdf5PeakRecord> evidencePeaks,
        double mzTolerancePpm)
    {
        int bestCharge = 0;
        int bestScore = 0;
        foreach (int charge in ChargesInConsideration)
        {
            int score = CountIsotopeMatches(peak, evidencePeaks, charge, mzTolerancePpm);
            if (score >= StrongIsotopeMatchCount)
            {
                return charge;
            }

            if (score > bestScore)
            {
                bestScore = score;
                bestCharge = charge;
            }
        }

        return bestScore > 0 ? bestCharge : 0;
    }

    private static int CountIsotopeMatches(
        Hdf5PeakRecord peak,
        IReadOnlyList<Hdf5PeakRecord> evidencePeaks,
        int charge,
        double mzTolerancePpm)
    {
        int matches = 0;
        foreach (int isotopeOffset in IsotopeOffsets)
        {
            double expectedMz = peak.Mz + isotopeOffset * NeutronMass / charge;
            bool found = false;
            foreach (Hdf5PeakRecord evidencePeak in evidencePeaks)
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

    private static Hdf5PeakRecord? ProjectPeakToMobilityWindow(
        Hdf5PeakRecord peak,
        double oneOverK0Begin,
        double oneOverK0End,
        IReadOnlyList<double>? oneOverK0ByIndex)
    {
        Hdf5PeakMobilityTrace? trace = peak.MobilityTrace;
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

    private static List<Hdf5PeakRecord> FindPeaksInRange(
        IReadOnlyList<Hdf5PeakRecord> peaks,
        double start,
        double end,
        double mzTolerancePpm)
    {
        List<Hdf5PeakRecord> peaksInRange = new();
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
        Hdf5PeakRecord selectedPeak,
        Hdf5PeakRecord candidatePeak,
        int isotopeOffset,
        double isolationCenterMz,
        double mzTolerancePpm)
    {
        double observedMzSpacing = Math.Abs(selectedPeak.Mz - candidatePeak.Mz);
        double mzTolerance = MzToleranceDa(isolationCenterMz, mzTolerancePpm);
        foreach (int charge in CandidateChargesForIsotopeCheck(selectedPeak, candidatePeak))
        {
            double expectedMzSpacing = isotopeOffset * NeutronMass / charge;
            if (Math.Abs(observedMzSpacing - expectedMzSpacing) <= mzTolerance)
            {
                return true;
            }
        }

        return false;
    }

    private static IEnumerable<int> CandidateChargesForIsotopeCheck(Hdf5PeakRecord selectedPeak, Hdf5PeakRecord candidatePeak)
    {
        int selectedCharge = selectedPeak.Charge;
        int candidateCharge = candidatePeak.Charge;
        if (selectedCharge > 0 && candidateCharge > 0)
        {
            if (selectedCharge == candidateCharge)
            {
                yield return selectedCharge;
            }

            yield break;
        }

        if (selectedCharge > 0)
        {
            yield return selectedCharge;
            yield break;
        }

        if (candidateCharge > 0)
        {
            yield return candidateCharge;
            yield break;
        }

        foreach (int charge in ChargesInConsideration)
        {
            yield return charge;
        }
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
