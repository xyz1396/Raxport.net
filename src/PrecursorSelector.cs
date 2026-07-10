namespace Raxport;

internal sealed record RaxportIsotopeChargeEvidence(
    int Charge,
    int MatchCount);

internal sealed record RaxportSelectedPrecursorRecord(
    RaxportPeakRecord ApexPeak,
    double EnvelopeIntensity,
    int ResolvedCharge,
    RaxportPrecursorChargeSource ChargeSource,
    int IsotopeMatchCount,
    IReadOnlyList<RaxportIsotopeChargeEvidence>? FallbackChargeEvidence = null);

internal static class PrecursorSelector
{
    private const double NeutronMass = 1.003355;
    private const int MinimumConfidentIsotopeMatches = 2;
    private const int MinTrustedPrecursorCharge = 1;
    private const int MaxTrustedPrecursorCharge = 7;
    private const int MaxIsotopeOffset = 10;
    private static readonly double IsotopeEvidenceMzPadding = 3 * NeutronMass;
    private static readonly int[] DefaultGuessedCharges = { 2, 3, 4 };
    private static readonly int[] ChargesInConsideration = { 2, 3, 4, 5, 6, 1, 7 };

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

        return SelectPrecursorEnvelopesFromEvidence(
                evidencePeaks,
                isotopeEvidencePeaks,
                precursorMz,
                topN,
                intensityRatio,
                mzTolerancePpm,
                preferredCharge)
            .Select(selection => selection.ApexPeak)
            .ToList();
    }

    public static List<RaxportPeakRecord> FindPrecursorPeaksFromEvidence(
        IReadOnlyList<RaxportPeakRecord> evidencePeaks,
        IReadOnlyList<RaxportPeakRecord> isotopeEvidencePeaks,
        int topN,
        double intensityRatio,
        double mzTolerancePpm,
        int preferredCharge = 0,
        double precursorMz = 0)
    {
        return SelectPrecursorEnvelopesFromEvidence(
                evidencePeaks,
                isotopeEvidencePeaks,
                precursorMz,
                topN,
                intensityRatio,
                mzTolerancePpm,
                preferredCharge)
            .Select(selection => selection.ApexPeak)
            .ToList();
    }

    public static List<RaxportSelectedPrecursorRecord> SelectPrecursorEnvelopesFromEvidence(
        IReadOnlyList<RaxportPeakRecord> evidencePeaks,
        IReadOnlyList<RaxportPeakRecord> isotopeEvidencePeaks,
        double precursorMz,
        int topN,
        double intensityRatio,
        double mzTolerancePpm,
        int reportedTargetCharge = 0)
    {
        List<RaxportSelectedPrecursorRecord> selected = new();
        if (topN <= 0 || evidencePeaks.Count == 0)
        {
            return selected;
        }

        List<RaxportPeakRecord> mzOrderedEvidence = evidencePeaks
            .Where(IsUsableEvidencePeak)
            .OrderBy(peak => peak.Mz)
            .ToList();
        if (mzOrderedEvidence.Count == 0)
        {
            return selected;
        }

        List<RaxportPeakRecord> mzOrderedIsotopeEvidence = isotopeEvidencePeaks
            .Where(IsUsableEvidencePeak)
            .OrderBy(peak => peak.Mz)
            .ToList();

        int[] apexOrder = Enumerable.Range(0, mzOrderedEvidence.Count)
            .OrderByDescending(index => mzOrderedEvidence[index].Intensity)
            .ThenBy(index => mzOrderedEvidence[index].Mz)
            .ToArray();
        bool[] claimed = new bool[mzOrderedEvidence.Count];
        bool[] isotopeClaimed = new bool[mzOrderedIsotopeEvidence.Count];
        List<RaxportSelectedPrecursorRecord> envelopes = new();

        foreach (int apexIndex in apexOrder)
        {
            if (claimed[apexIndex])
            {
                continue;
            }

            RaxportPeakRecord apex = mzOrderedEvidence[apexIndex];
            ChargeResolution resolution = ResolveCharge(
                apex,
                mzOrderedIsotopeEvidence,
                isotopeClaimed,
                precursorMz,
                reportedTargetCharge,
                mzTolerancePpm);
            IReadOnlyList<RaxportIsotopeChargeEvidence> fallbackChargeEvidence =
                resolution.FallbackChargeEvidence ?? Array.Empty<RaxportIsotopeChargeEvidence>();
            int bestFallbackMatchCount = fallbackChargeEvidence.Count > 0
                ? fallbackChargeEvidence.Max(evidence => evidence.MatchCount)
                : 0;
            IEnumerable<int> removalCharges = IsTrustedPrecursorCharge(resolution.Charge)
                ? new[] { resolution.Charge }
                : fallbackChargeEvidence
                    .Where(evidence => evidence.MatchCount == bestFallbackMatchCount)
                    .Select(evidence => evidence.Charge);

            double envelopeIntensity = ClaimMatchingPeaks(
                mzOrderedEvidence,
                claimed,
                apex.Mz,
                mzTolerancePpm);
            ClaimMatchingPeaks(mzOrderedIsotopeEvidence, isotopeClaimed, apex.Mz, mzTolerancePpm);
            List<int> isotopeMemberIndicesToClaim = new();
            foreach (int removalCharge in removalCharges)
            {
                envelopeIntensity += ClaimIsotopeMembers(
                    mzOrderedIsotopeEvidence,
                    isotopeClaimed,
                    mzOrderedEvidence,
                    claimed,
                    apex,
                    removalCharge,
                    mzTolerancePpm,
                    direction: 1);
                envelopeIntensity += ClaimIsotopeMembers(
                    mzOrderedIsotopeEvidence,
                    isotopeClaimed,
                    mzOrderedEvidence,
                    claimed,
                    apex,
                    removalCharge,
                    mzTolerancePpm,
                    direction: -1);
                CollectIsotopeMemberIndices(
                    mzOrderedIsotopeEvidence,
                    isotopeClaimed,
                    isotopeMemberIndicesToClaim,
                    apex,
                    removalCharge,
                    mzTolerancePpm,
                    direction: 1);
                CollectIsotopeMemberIndices(
                    mzOrderedIsotopeEvidence,
                    isotopeClaimed,
                    isotopeMemberIndicesToClaim,
                    apex,
                    removalCharge,
                    mzTolerancePpm,
                    direction: -1);
            }
            foreach (int isotopeIndex in isotopeMemberIndicesToClaim)
            {
                isotopeClaimed[isotopeIndex] = true;
            }

            envelopes.Add(new RaxportSelectedPrecursorRecord(
                apex,
                envelopeIntensity,
                resolution.Charge,
                resolution.Source,
                resolution.IsotopeMatchCount,
                fallbackChargeEvidence));
        }

        envelopes.Sort(static (left, right) =>
        {
            int envelopeComparison = right.EnvelopeIntensity.CompareTo(left.EnvelopeIntensity);
            if (envelopeComparison != 0)
            {
                return envelopeComparison;
            }

            int apexComparison = right.ApexPeak.Intensity.CompareTo(left.ApexPeak.Intensity);
            return apexComparison != 0 ? apexComparison : left.ApexPeak.Mz.CompareTo(right.ApexPeak.Mz);
        });

        double totalIntensity = mzOrderedEvidence.Sum(peak => peak.Intensity);
        double targetFraction = Math.Clamp(intensityRatio, 0, 1);
        double accountedIntensity = 0;
        foreach (RaxportSelectedPrecursorRecord envelope in envelopes)
        {
            selected.Add(envelope);
            accountedIntensity += envelope.EnvelopeIntensity;
            if (selected.Count >= topN ||
                (totalIntensity > 0 && accountedIntensity / totalIntensity >= targetFraction))
            {
                break;
            }
        }

        return selected;
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

        double halfWidth = Math.Abs(isolationWindow) / 2;
        List<RaxportPeakRecord> peaksInRange = FindPeaksInRange(
            peaks,
            precursorMz - halfWidth,
            precursorMz + halfWidth,
            mzTolerancePpm);
        if (oneOverK0Begin.HasValue && oneOverK0End.HasValue)
        {
            peaksInRange = peaksInRange
                .Select(peak => ProjectPeakToMobilityWindow(peak, oneOverK0Begin.Value, oneOverK0End.Value, oneOverK0ByIndex))
                .Where(peak => peak is not null)
                .Cast<RaxportPeakRecord>()
                .ToList();
        }

        return peaksInRange
            .Where(IsUsableEvidencePeak)
            .OrderBy(peak => peak.Mz)
            .ToList();
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
            Math.Abs(isolationWindow) + 2 * IsotopeEvidenceMzPadding,
            mzTolerancePpm,
            oneOverK0Begin,
            oneOverK0End,
            oneOverK0ByIndex);
    }

    public static List<RaxportPrecursorCandidateRecord> ExpandPrecursorCandidates(
        IEnumerable<RaxportPeakRecord> precursorPeaks,
        int maxSelectedPrecursorPeaks,
        int preferredCharge = 0)
    {
        _ = preferredCharge; // A reported charge cannot be scoped safely without a target m/z.
        List<RaxportPeakRecord> selectedPeaks = precursorPeaks.Take(Math.Max(0, maxSelectedPrecursorPeaks)).ToList();
        List<RaxportSelectedPrecursorRecord> selections = new(selectedPeaks.Count);
        foreach (RaxportPeakRecord peak in selectedPeaks)
        {
            int resolvedCharge = IsTrustedPrecursorCharge(peak.Charge) ? peak.Charge : 0;
            RaxportPrecursorChargeSource source = IsTrustedPrecursorCharge(peak.Charge)
                ? RaxportPrecursorChargeSource.Peak
                : RaxportPrecursorChargeSource.Unknown;
            selections.Add(new RaxportSelectedPrecursorRecord(
                peak,
                peak.Intensity,
                resolvedCharge,
                source,
                0));
        }

        return ExpandPrecursorCandidates(selections, maxSelectedPrecursorPeaks, mzTolerancePpm: 10);
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
        _ = isolationWindow;
        if (maxSelectedPrecursorPeaks <= 0)
        {
            return new List<RaxportPrecursorCandidateRecord>();
        }

        List<RaxportPeakRecord> mzOrderedEvidence = evidencePeaks
            .Where(IsUsableEvidencePeak)
            .OrderBy(peak => peak.Mz)
            .ToList();
        List<RaxportSelectedPrecursorRecord> selections = new();
        int selectedCount = Math.Min(maxSelectedPrecursorPeaks, precursorPeaks.Count);
        for (int i = 0; i < selectedCount; i++)
        {
            RaxportPeakRecord peak = precursorPeaks[i];
            bool[] unavailableEvidence = new bool[mzOrderedEvidence.Count];
            for (int otherIndex = 0; otherIndex < selectedCount; otherIndex++)
            {
                if (otherIndex == i)
                {
                    continue;
                }

                ClaimMatchingPeaks(
                    mzOrderedEvidence,
                    unavailableEvidence,
                    precursorPeaks[otherIndex].Mz,
                    mzTolerancePpm);
            }

            ChargeResolution resolution = ResolveCharge(
                peak,
                mzOrderedEvidence,
                unavailableEvidence,
                precursorMz,
                preferredCharge,
                mzTolerancePpm);
            selections.Add(new RaxportSelectedPrecursorRecord(
                peak,
                peak.Intensity,
                resolution.Charge,
                resolution.Source,
                resolution.IsotopeMatchCount,
                resolution.FallbackChargeEvidence));
        }

        return ExpandPrecursorCandidates(selections, maxSelectedPrecursorPeaks, mzTolerancePpm);
    }

    public static List<RaxportPrecursorCandidateRecord> ExpandPrecursorCandidates(
        IReadOnlyList<RaxportSelectedPrecursorRecord> selectedPrecursors,
        int maxSelectedPrecursorPeaks,
        double mzTolerancePpm)
    {
        List<RaxportPrecursorCandidateRecord> candidates = new();
        if (maxSelectedPrecursorPeaks <= 0)
        {
            return candidates;
        }

        int selectedCount = Math.Min(maxSelectedPrecursorPeaks, selectedPrecursors.Count);
        for (int i = 0; i < selectedCount; i++)
        {
            RaxportSelectedPrecursorRecord selection = selectedPrecursors[i];
            if (IsTrustedPrecursorCharge(selection.ResolvedCharge))
            {
                AddOrKeepBest(
                    candidates,
                    selection.ApexPeak,
                    selection.ResolvedCharge,
                    selection.ChargeSource,
                    selection.IsotopeMatchCount,
                    mzTolerancePpm);
                continue;
            }

            IReadOnlyList<RaxportIsotopeChargeEvidence> fallbackEvidence =
                selection.FallbackChargeEvidence ?? Array.Empty<RaxportIsotopeChargeEvidence>();
            HashSet<int> addedCharges = new();
            foreach (RaxportIsotopeChargeEvidence evidence in
                     fallbackEvidence.OrderByDescending(evidence => evidence.MatchCount))
            {
                if (!addedCharges.Add(evidence.Charge))
                {
                    continue;
                }

                AddOrKeepBest(
                    candidates,
                    selection.ApexPeak,
                    evidence.Charge,
                    RaxportPrecursorChargeSource.Fallback,
                    evidence.MatchCount,
                    mzTolerancePpm);
            }
            foreach (int guessedCharge in DefaultGuessedCharges)
            {
                if (!addedCharges.Add(guessedCharge))
                {
                    continue;
                }

                AddOrKeepBest(
                    candidates,
                    selection.ApexPeak,
                    guessedCharge,
                    RaxportPrecursorChargeSource.Fallback,
                    0,
                    mzTolerancePpm);
            }
        }

        return DeduplicateCandidates(candidates, mzTolerancePpm);
    }

    private static ChargeResolution ResolveCharge(
        RaxportPeakRecord apex,
        IReadOnlyList<RaxportPeakRecord> isotopeEvidence,
        IReadOnlyList<bool>? unavailableEvidence,
        double precursorMz,
        int reportedTargetCharge,
        double mzTolerancePpm)
    {
        if (IsTrustedPrecursorCharge(apex.Charge))
        {
            IsotopeHypothesis hypothesis = ScoreIsotopeHypothesis(apex, isotopeEvidence, unavailableEvidence, apex.Charge, mzTolerancePpm);
            return new ChargeResolution(
                apex.Charge,
                RaxportPrecursorChargeSource.Peak,
                hypothesis.MatchCount);
        }

        if (IsTrustedPrecursorCharge(reportedTargetCharge) &&
            IsPeakInReportedEnvelope(apex, isotopeEvidence, unavailableEvidence, precursorMz, reportedTargetCharge, mzTolerancePpm))
        {
            IsotopeHypothesis hypothesis = ScoreIsotopeHypothesis(
                apex,
                isotopeEvidence,
                unavailableEvidence,
                reportedTargetCharge,
                mzTolerancePpm);
            return new ChargeResolution(
                reportedTargetCharge,
                RaxportPrecursorChargeSource.Reported,
                hypothesis.MatchCount);
        }

        IsotopeSearchResult isotopeSearch = FindBestIsotopeHypothesis(apex, isotopeEvidence, unavailableEvidence, mzTolerancePpm);
        IsotopeHypothesis bestHypothesis = isotopeSearch.Best;
        if (bestHypothesis.MatchCount >= MinimumConfidentIsotopeMatchesForCharge(bestHypothesis.Charge) &&
            bestHypothesis.MatchCount > isotopeSearch.RunnerUpMatchCount)
        {
            return new ChargeResolution(
                bestHypothesis.Charge,
                RaxportPrecursorChargeSource.Isotope,
                bestHypothesis.MatchCount);
        }

        return new ChargeResolution(
            0,
            RaxportPrecursorChargeSource.Unknown,
            0,
            isotopeSearch.ChargeEvidence);
    }

    private static bool IsPeakInReportedEnvelope(
        RaxportPeakRecord peak,
        IReadOnlyList<RaxportPeakRecord> isotopeEvidence,
        IReadOnlyList<bool>? unavailableEvidence,
        double precursorMz,
        int charge,
        double mzTolerancePpm)
    {
        if (precursorMz <= 0)
        {
            return false;
        }

        double isotopeSpacing = NeutronMass / charge;
        int isotopeOffset = (int)Math.Round((peak.Mz - precursorMz) / isotopeSpacing);
        if (Math.Abs(isotopeOffset) > MaxIsotopeOffset ||
            !WithinMzTolerance(peak.Mz, precursorMz + isotopeOffset * isotopeSpacing, mzTolerancePpm))
        {
            return false;
        }

        if (isotopeOffset == 0)
        {
            return true;
        }

        int direction = Math.Sign(isotopeOffset);
        for (int offset = 0; offset < Math.Abs(isotopeOffset); offset++)
        {
            double expectedMz = precursorMz + direction * offset * isotopeSpacing;
            if (!FindClosestPeak(isotopeEvidence, expectedMz, mzTolerancePpm, double.NaN, unavailableEvidence).Found)
            {
                return false;
            }
        }

        return true;
    }

    private static IsotopeSearchResult FindBestIsotopeHypothesis(
        RaxportPeakRecord apex,
        IReadOnlyList<RaxportPeakRecord> isotopeEvidence,
        IReadOnlyList<bool>? unavailableEvidence,
        double mzTolerancePpm)
    {
        IsotopeHypothesis best = default;
        int runnerUpMatchCount = 0;
        List<RaxportIsotopeChargeEvidence> chargeEvidence = new();
        foreach (int charge in ChargesInConsideration)
        {
            IsotopeHypothesis current = ScoreIsotopeHypothesis(
                apex,
                isotopeEvidence,
                unavailableEvidence,
                charge,
                mzTolerancePpm);
            if (current.MatchCount > 0)
            {
                chargeEvidence.Add(new RaxportIsotopeChargeEvidence(charge, current.MatchCount));
            }

            if (current.MatchCount > best.MatchCount ||
                (current.MatchCount == best.MatchCount &&
                 current.MatchCount > 0 &&
                 current.MeanAbsolutePpmError < best.MeanAbsolutePpmError))
            {
                runnerUpMatchCount = Math.Max(runnerUpMatchCount, best.MatchCount);
                best = current;
            }
            else
            {
                runnerUpMatchCount = Math.Max(runnerUpMatchCount, current.MatchCount);
            }
        }

        return new IsotopeSearchResult(best, runnerUpMatchCount, chargeEvidence);
    }

    private static IsotopeHypothesis ScoreIsotopeHypothesis(
        RaxportPeakRecord apex,
        IReadOnlyList<RaxportPeakRecord> isotopeEvidence,
        IReadOnlyList<bool>? unavailableEvidence,
        int charge,
        double mzTolerancePpm)
    {
        if (!IsTrustedPrecursorCharge(charge) || isotopeEvidence.Count == 0)
        {
            return new IsotopeHypothesis(charge, 0, double.PositiveInfinity);
        }

        int matchCount = 0;
        double absolutePpmErrorSum = 0;
        ScoreDirection(1);
        ScoreDirection(-1);
        return new IsotopeHypothesis(
            charge,
            matchCount,
            matchCount > 0 ? absolutePpmErrorSum / matchCount : double.PositiveInfinity);

        void ScoreDirection(int direction)
        {
            for (int offset = 1; offset <= MaxIsotopeOffset; offset++)
            {
                double expectedMz = apex.Mz + direction * offset * NeutronMass / charge;
                PeakMatch match = FindClosestPeak(isotopeEvidence, expectedMz, mzTolerancePpm, apex.Mz, unavailableEvidence);
                if (!match.Found)
                {
                    break;
                }

                matchCount++;
                absolutePpmErrorSum += match.AbsolutePpmError;
            }
        }
    }

    private static double ClaimIsotopeMembers(
        IReadOnlyList<RaxportPeakRecord> isotopeEvidence,
        IReadOnlyList<bool>? unavailableEvidence,
        IReadOnlyList<RaxportPeakRecord> targetEvidence,
        bool[] targetClaimed,
        RaxportPeakRecord apex,
        int charge,
        double mzTolerancePpm,
        int direction)
    {
        double claimedIntensity = 0;
        for (int offset = 1; offset <= MaxIsotopeOffset; offset++)
        {
            double expectedMz = apex.Mz + direction * offset * NeutronMass / charge;
            if (!FindClosestPeak(
                    isotopeEvidence,
                    expectedMz,
                    mzTolerancePpm,
                    double.NaN,
                    unavailableEvidence).Found)
            {
                break;
            }

            claimedIntensity += ClaimMatchingPeaks(
                targetEvidence,
                targetClaimed,
                expectedMz,
                mzTolerancePpm);
        }

        return claimedIntensity;
    }

    private static void CollectIsotopeMemberIndices(
        IReadOnlyList<RaxportPeakRecord> isotopeEvidence,
        IReadOnlyList<bool>? unavailableEvidence,
        List<int> destination,
        RaxportPeakRecord apex,
        int charge,
        double mzTolerancePpm,
        int direction)
    {
        for (int offset = 1; offset <= MaxIsotopeOffset; offset++)
        {
            double expectedMz = apex.Mz + direction * offset * NeutronMass / charge;
            List<int> matches = FindMatchingPeakIndices(
                isotopeEvidence,
                expectedMz,
                mzTolerancePpm,
                unavailableEvidence);
            if (matches.Count == 0)
            {
                break;
            }

            destination.AddRange(matches);
        }
    }

    private static double ClaimMatchingPeaks(
        IReadOnlyList<RaxportPeakRecord> mzOrderedPeaks,
        bool[] claimed,
        double expectedMz,
        double mzTolerancePpm)
    {
        if (mzOrderedPeaks.Count != claimed.Length)
        {
            throw new ArgumentException("The evidence and claimed mask must have the same length.");
        }

        double claimedIntensity = 0;
        foreach (int index in FindMatchingPeakIndices(mzOrderedPeaks, expectedMz, mzTolerancePpm))
        {
            if (claimed[index])
            {
                continue;
            }

            claimed[index] = true;
            claimedIntensity += mzOrderedPeaks[index].Intensity;
        }

        return claimedIntensity;
    }

    private static PeakMatch FindClosestPeak(
        IReadOnlyList<RaxportPeakRecord> mzOrderedPeaks,
        double expectedMz,
        double mzTolerancePpm,
        double excludedMz,
        IReadOnlyList<bool>? unavailable = null)
    {
        double tolerance = MzToleranceDa(expectedMz, mzTolerancePpm);
        double lower = expectedMz - tolerance;
        double upper = expectedMz + tolerance;
        int index = LowerBound(mzOrderedPeaks, lower);
        double bestDelta = double.PositiveInfinity;
        bool found = false;
        while (index < mzOrderedPeaks.Count && mzOrderedPeaks[index].Mz <= upper)
        {
            RaxportPeakRecord peak = mzOrderedPeaks[index];
            if ((unavailable is null || !unavailable[index]) &&
                !WithinMzTolerance(peak.Mz, excludedMz, mzTolerancePpm))
            {
                double delta = Math.Abs(peak.Mz - expectedMz);
                if (delta < bestDelta)
                {
                    bestDelta = delta;
                    found = true;
                }
            }
            index++;
        }

        double ppmError = found && expectedMz != 0
            ? bestDelta / Math.Abs(expectedMz) * 1_000_000.0
            : double.PositiveInfinity;
        return new PeakMatch(found, ppmError);
    }

    private static List<int> FindMatchingPeakIndices(
        IReadOnlyList<RaxportPeakRecord> mzOrderedPeaks,
        double expectedMz,
        double mzTolerancePpm,
        IReadOnlyList<bool>? unavailable = null)
    {
        List<int> matches = new();
        double tolerance = MzToleranceDa(expectedMz, mzTolerancePpm);
        double lower = expectedMz - tolerance;
        double upper = expectedMz + tolerance;
        int index = LowerBound(mzOrderedPeaks, lower);
        while (index < mzOrderedPeaks.Count && mzOrderedPeaks[index].Mz <= upper)
        {
            if (unavailable is null || !unavailable[index])
            {
                matches.Add(index);
            }
            index++;
        }

        return matches;
    }

    private static int LowerBound(IReadOnlyList<RaxportPeakRecord> mzOrderedPeaks, double mz)
    {
        int low = 0;
        int high = mzOrderedPeaks.Count;
        while (low < high)
        {
            int mid = low + ((high - low) / 2);
            if (mzOrderedPeaks[mid].Mz < mz)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }

        return low;
    }

    private static void AddOrKeepBest(
        List<RaxportPrecursorCandidateRecord> candidates,
        RaxportPeakRecord peak,
        int charge,
        RaxportPrecursorChargeSource chargeSource,
        int isotopeMatchCount,
        double mzTolerancePpm)
    {
        if (!IsTrustedPrecursorCharge(charge))
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
                    candidates[i] = CreateCandidate(peak, charge, chargeSource, isotopeMatchCount);
                }

                return;
            }
        }

        candidates.Add(CreateCandidate(peak, charge, chargeSource, isotopeMatchCount));
    }

    private static List<RaxportPrecursorCandidateRecord> DeduplicateCandidates(
        IReadOnlyList<RaxportPrecursorCandidateRecord> candidates,
        double mzTolerancePpm)
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

            if (!merged)
            {
                deduplicated.Add(candidate);
            }
        }

        return deduplicated;
    }

    private static RaxportPrecursorCandidateRecord CreateCandidate(
        RaxportPeakRecord peak,
        int charge,
        RaxportPrecursorChargeSource chargeSource,
        int isotopeMatchCount)
    {
        return new RaxportPrecursorCandidateRecord(
            charge,
            peak.Mz,
            peak.Intensity,
            peak.CandidateOneOverK0,
            chargeSource,
            isotopeMatchCount);
    }

    private static int MinimumConfidentIsotopeMatchesForCharge(int charge)
    {
        return charge == 1 ? 3 : MinimumConfidentIsotopeMatches;
    }

    private static bool IsTrustedPrecursorCharge(int charge)
    {
        return charge >= MinTrustedPrecursorCharge && charge <= MaxTrustedPrecursorCharge;
    }

    private static bool IsUsableEvidencePeak(RaxportPeakRecord peak)
    {
        return double.IsFinite(peak.Mz)
            && double.IsFinite(peak.Intensity)
            && peak.Mz > 0
            && peak.Intensity > 0;
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
            if (!float.IsFinite(intensity) || intensity <= 0)
            {
                continue;
            }

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
        double lowerMz = Math.Min(start, end);
        double upperMz = Math.Max(start, end);
        double lowerBound = lowerMz - MzToleranceDa(lowerMz, mzTolerancePpm);
        double upperBound = upperMz + MzToleranceDa(upperMz, mzTolerancePpm);
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

    private static bool WithinMzTolerance(double observedMz, double expectedMz, double mzTolerancePpm)
    {
        return Math.Abs(observedMz - expectedMz) <= MzToleranceDa(expectedMz, mzTolerancePpm);
    }

    private static double MzToleranceDa(double mz, double mzTolerancePpm)
    {
        return Math.Abs(mz) * Math.Max(0, mzTolerancePpm) / 1_000_000.0;
    }

    private readonly record struct ChargeResolution(
        int Charge,
        RaxportPrecursorChargeSource Source,
        int IsotopeMatchCount,
        IReadOnlyList<RaxportIsotopeChargeEvidence>? FallbackChargeEvidence = null);

    private readonly record struct IsotopeSearchResult(
        IsotopeHypothesis Best,
        int RunnerUpMatchCount,
        IReadOnlyList<RaxportIsotopeChargeEvidence> ChargeEvidence);

    private readonly record struct IsotopeHypothesis(
        int Charge,
        int MatchCount,
        double MeanAbsolutePpmError);

    private readonly record struct PeakMatch(bool Found, double AbsolutePpmError);
}
