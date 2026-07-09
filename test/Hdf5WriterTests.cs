#if RAXPORT_TESTS
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Raxport;

[TestClass]
public sealed class Hdf5WriterTests
{
    [TestMethod]
    public void FlushesAcrossConfiguredPeakBoundary()
    {
        string path = Path.Combine(Path.GetTempPath(), $"raxport-boundary-{Guid.NewGuid():N}.h5");
        try
        {
            using Hdf5Writer writer = new(path, "boundary.raw", "test instrument", "test", 3);
            writer.AddScan(CreateScan(1, 1, null));
            Assert.AreEqual(0, writer.FlushCount);
            writer.AddScan(CreateScan(2, 1, null));
            Assert.AreEqual(1, writer.FlushCount);
            writer.AddScan(CreateScan(3, 2, CreateReaction()));
        }
        finally
        {
            AssertHdf5FileCreated(path);
            File.Delete(path);
        }
    }

    [TestMethod]
    public void WritesEmptyAndNonEmptyOffsets()
    {
        string path = Path.Combine(Path.GetTempPath(), $"raxport-offsets-{Guid.NewGuid():N}.h5");
        try
        {
            using Hdf5Writer writer = new(path, "offsets.raw", "test instrument", "test", 20000);
            writer.AddScan(CreateScan(10, 1, null));
            writer.AddScan(CreateScan(11, 3, CreateReaction(1.23, 0.98)));
        }
        finally
        {
            AssertHdf5FileCreated(path);
            AssertReactionMobilityRange(path, 1.23, 0.98);
            File.Delete(path);
        }
    }

    [TestMethod]
    public void PrecursorSelectorSuppressesIsotopicPeaksBeforeNextCandidate()
    {
        RaxportPeakRecord[] peaks =
        {
            new(500.0000, 1000, 0, 0, 0, 2),
            new(500.5017, 800, 0, 0, 0, 2),
            new(501.0034, 600, 0, 0, 0, 2),
            new(501.5000, 500, 0, 0, 0, 0)
        };

        List<RaxportPeakRecord> selected = PrecursorSelector.FindPrecursorPeaks(peaks, 500.75, 3.0, 2, 0.99, 10);
        List<RaxportPeakRecord> evidence = PrecursorSelector.GetPrecursorEvidencePeaks(peaks, 500.75, 3.0, 10);
        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(selected, evidence, 500.75, 3.0, 2, 10);

        Assert.AreEqual(2, selected.Count);
        Assert.AreEqual(500.0000, selected[0].Mz, 0.0001);
        Assert.AreEqual(501.5000, selected[1].Mz, 0.0001);
        Assert.AreEqual(2, candidates[0].Charge);
        Assert.AreEqual(500.0000, candidates[0].Mz, 0.0001);
    }


    [TestMethod]
    public void PrecursorSelectorThermoTrailerChargeDoesNotDriveIsotopeRemoval()
    {
        RaxportPeakRecord[] peaks =
        {
            new(500.000000, 1000, 0, 0, 0, 0),
            new(500.501678, 900, 0, 0, 0, 0),
            new(502.000000, 800, 0, 0, 0, 0)
        };

        int trailerCharge = 4;
        List<RaxportPeakRecord> selected = PrecursorSelector.FindPrecursorPeaks(peaks, 501.0, 4.0, 2, 1.0, 10);
        List<RaxportPeakRecord> evidence = PrecursorSelector.GetPrecursorEvidencePeaks(peaks, 501.0, 4.0, 10);
        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(
            selected,
            evidence,
            501.0,
            4.0,
            2,
            10,
            trailerCharge);

        Assert.AreEqual(2, selected.Count);
        Assert.AreEqual(500.000000, selected[0].Mz, 0.0001);
        Assert.AreEqual(502.000000, selected[1].Mz, 0.0001);
        Assert.AreEqual(2, candidates[0].Charge);
    }

    [TestMethod]
    public void PrecursorSelectorLimitsSelectionPoolByTopNMultiplier()
    {
        RaxportPeakRecord[] peaks =
        {
            new(500.000000, 1000, 0, 0, 0, 2),
            new(500.501678, 900, 0, 0, 0, 0),
            new(501.003355, 800, 0, 0, 0, 0),
            new(501.505033, 700, 0, 0, 0, 0),
            new(503.000000, 600, 0, 0, 0, 0)
        };

        List<RaxportPeakRecord> selected = PrecursorSelector.FindPrecursorPeaks(peaks, 501.5, 5.0, 2, 1.0, 10);

        Assert.AreEqual(1, selected.Count);
        Assert.AreEqual(500.000000, selected[0].Mz, 0.0001);
    }

    [TestMethod]
    public void PrecursorSelectorLimitsSelectionPoolByIntensityRatio()
    {
        RaxportPeakRecord[] peaks =
        {
            new(500.0, 1000, 0, 0, 0, 0),
            new(501.0, 100, 0, 0, 0, 0),
            new(502.0, 100, 0, 0, 0, 0),
            new(503.0, 100, 0, 0, 0, 0)
        };

        List<RaxportPeakRecord> selected = PrecursorSelector.FindPrecursorPeaks(peaks, 501.5, 5.0, 5, 0.5, 10);

        Assert.AreEqual(1, selected.Count);
        Assert.AreEqual(500.0, selected[0].Mz, 0.0001);
    }

    [TestMethod]
    public void PrecursorSelectorCapsSelectedPrecursorCountAtTopN()
    {
        RaxportPeakRecord[] peaks =
        {
            new(500.0, 1000, 0, 0, 0, 0),
            new(501.0, 900, 0, 0, 0, 0),
            new(502.0, 800, 0, 0, 0, 0),
            new(503.0, 700, 0, 0, 0, 0)
        };

        List<RaxportPeakRecord> selected = PrecursorSelector.FindPrecursorPeaks(peaks, 501.5, 5.0, 2, 1.0, 10);

        Assert.AreEqual(2, selected.Count);
        CollectionAssert.AreEqual(new[] { 500.0, 501.0 }, selected.Select(peak => peak.Mz).ToArray());
    }

    [TestMethod]
    public void PrecursorSelectorUsesMs1PeakChargeBeforePreferredChargeForIsotopeRemoval()
    {
        RaxportPeakRecord[] peaks =
        {
            new(500.000000, 1000, 0, 0, 0, 3),
            new(500.334452, 900, 0, 0, 0, 0),
            new(500.501678, 800, 0, 0, 0, 0),
            new(502.000000, 700, 0, 0, 0, 0)
        };

        List<RaxportPeakRecord> selected = PrecursorSelector.FindPrecursorPeaks(peaks, 501.0, 4.0, 2, 1.0, 10, preferredCharge: 2);

        Assert.AreEqual(2, selected.Count);
        Assert.AreEqual(500.000000, selected[0].Mz, 0.0001);
        Assert.AreEqual(500.501678, selected[1].Mz, 0.0001);
    }

    [TestMethod]
    public void PrecursorSelectorUsesPreferredChargeForBrukerLikeIsotopeRemoval()
    {
        RaxportPeakRecord[] peaks =
        {
            new(1102.024258, 1000, 0, 0, 0, 0),
            new(1102.275096, 900, 0, 0, 0, 0),
            new(1102.525935, 800, 0, 0, 0, 0),
            new(1102.776774, 700, 0, 0, 0, 0),
            new(1104.000000, 600, 0, 0, 0, 0)
        };

        List<RaxportPeakRecord> selected = PrecursorSelector.FindPrecursorPeaks(peaks, 1103.0, 5.0, 3, 1.0, 10, preferredCharge: 4);

        Assert.AreEqual(2, selected.Count);
        Assert.AreEqual(1102.024258, selected[0].Mz, 0.0001);
        Assert.AreEqual(1104.000000, selected[1].Mz, 0.0001);
    }

    [TestMethod]
    public void PrecursorSelectorInfersStrongChargeBeforeIsotopeRemoval()
    {
        RaxportPeakRecord[] peaks =
        {
            new(500.000000, 1000, 0, 0, 0, 0),
            new(500.334452, 900, 0, 0, 0, 0),
            new(500.668903, 800, 0, 0, 0, 0),
            new(501.003355, 700, 0, 0, 0, 0),
            new(502.000000, 600, 0, 0, 0, 0)
        };

        List<RaxportPeakRecord> selected = PrecursorSelector.FindPrecursorPeaks(peaks, 501.0, 4.0, 3, 1.0, 10);

        Assert.AreEqual(2, selected.Count);
        Assert.AreEqual(500.000000, selected[0].Mz, 0.0001);
        Assert.AreEqual(502.000000, selected[1].Mz, 0.0001);
    }

    [TestMethod]
    public void PrecursorSelectorUsesDefaultGuessWhenThermoChargeIsMissing()
    {
        RaxportPeakRecord[] peaks =
        {
            new(500.000000, 1000, 0, 0, 0, 0),
            new(500.334452, 900, 0, 0, 0, 0),
            new(500.668903, 800, 0, 0, 0, 0),
            new(501.003355, 700, 0, 0, 0, 0),
            new(502.000000, 600, 0, 0, 0, 0)
        };

        List<RaxportPeakRecord> selected = PrecursorSelector.FindPrecursorPeaks(peaks, 500.0, 0.1, 2, 1.0, 10);
        List<RaxportPeakRecord> isotopeEvidence = PrecursorSelector.GetIsotopeEvidencePeaks(peaks, 500.0, 0.1, 10);
        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(
            selected,
            isotopeEvidence,
            500.0,
            0.1,
            1,
            10);

        Assert.AreEqual(1, selected.Count);
        Assert.AreEqual(500.000000, selected[0].Mz, 0.0001);
        Assert.AreEqual(2, candidates[0].Charge);
    }

    [TestMethod]
    public void PrecursorSelectorUsesConservativeFallbackChargeOrder()
    {
        RaxportPeakRecord[] peaks =
        {
            new(500.000000, 1000, 0, 0, 0, 0),
            new(500.501678, 900, 0, 0, 0, 0),
            new(500.334452, 800, 0, 0, 0, 0)
        };

        List<RaxportPeakRecord> selected = PrecursorSelector.FindPrecursorPeaks(peaks, 500.5, 2.0, 2, 1.0, 10);

        Assert.AreEqual(2, selected.Count);
        Assert.AreEqual(500.000000, selected[0].Mz, 0.0001);
        Assert.AreEqual(500.334452, selected[1].Mz, 0.0001);
    }

    [TestMethod]
    public void PrecursorSelectorStopsForwardRemovalAtFirstMissingOffset()
    {
        RaxportPeakRecord[] peaks =
        {
            new(500.000000, 1000, 0, 0, 0, 2),
            new(501.003355, 900, 0, 0, 0, 0),
            new(502.000000, 800, 0, 0, 0, 0)
        };

        List<RaxportPeakRecord> selected = PrecursorSelector.FindPrecursorPeaks(peaks, 501.0, 4.0, 2, 1.0, 10);

        Assert.AreEqual(2, selected.Count);
        Assert.AreEqual(500.000000, selected[0].Mz, 0.0001);
        Assert.AreEqual(501.003355, selected[1].Mz, 0.0001);
    }

    [TestMethod]
    public void PrecursorSelectorRemovesBackwardIsotopesAndStopsAtFirstMissingOffset()
    {
        RaxportPeakRecord[] withMinusOne =
        {
            new(500.501678, 1000, 0, 0, 0, 2),
            new(500.000000, 900, 0, 0, 0, 0),
            new(502.000000, 800, 0, 0, 0, 0)
        };
        RaxportPeakRecord[] missingMinusOne =
        {
            new(501.003355, 1000, 0, 0, 0, 2),
            new(500.000000, 900, 0, 0, 0, 0),
            new(502.000000, 800, 0, 0, 0, 0)
        };

        List<RaxportPeakRecord> removed = PrecursorSelector.FindPrecursorPeaks(withMinusOne, 501.0, 4.0, 2, 1.0, 10);
        List<RaxportPeakRecord> stopped = PrecursorSelector.FindPrecursorPeaks(missingMinusOne, 501.0, 4.0, 2, 1.0, 10);

        Assert.AreEqual(2, removed.Count);
        Assert.AreEqual(500.501678, removed[0].Mz, 0.0001);
        Assert.AreEqual(502.000000, removed[1].Mz, 0.0001);
        Assert.AreEqual(2, stopped.Count);
        Assert.AreEqual(501.003355, stopped[0].Mz, 0.0001);
        Assert.AreEqual(500.000000, stopped[1].Mz, 0.0001);
    }

    [TestMethod]
    public void PrecursorSelectorExpandsUnknownChargeUsingRawDefaults()
    {
        RaxportPeakRecord[] peaks =
        {
            new(700.0, 1000, 0, 0, 0, 0)
        };

        List<RaxportPeakRecord> selected = PrecursorSelector.FindPrecursorPeaks(peaks, 700.0, 1.0, 3, 0.99, 10);
        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(selected, 3);

        CollectionAssert.AreEqual(new[] { 2, 3, 4 }, candidates.Select(candidate => candidate.Charge).ToArray());
        CollectionAssert.AreEqual(new[] { 700.0, 700.0, 700.0 }, candidates.Select(candidate => candidate.Mz).ToArray());
    }


    [TestMethod]
    public void PrecursorSelectorExpandsSelectedMzCountIntoGuessedCharges()
    {
        RaxportPeakRecord[] selected =
        {
            new(600.0, 1000, 0, 0, 0, 0),
            new(601.0, 900, 0, 0, 0, 0),
            new(602.0, 800, 0, 0, 0, 0),
            new(603.0, 700, 0, 0, 0, 0),
            new(604.0, 600, 0, 0, 0, 0),
            new(605.0, 500, 0, 0, 0, 0)
        };

        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(selected, 6);

        Assert.AreEqual(18, candidates.Count);
        CollectionAssert.AreEqual(
            new[] { 600.0, 601.0, 602.0, 603.0, 604.0, 605.0 },
            candidates.Select(candidate => candidate.Mz).Distinct().ToArray());
        CollectionAssert.AreEqual(new[] { 2, 3, 4 }, candidates.Take(3).Select(candidate => candidate.Charge).ToArray());
    }

    [TestMethod]
    public void PrecursorSelectorUsesPreferredChargeInsteadOfGuessesAtTrailerMz()
    {
        RaxportPeakRecord[] selected =
        {
            new(600.0, 1000, 0, 0, 0, 0),
            new(601.0, 900, 0, 0, 0, 0)
        };

        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(
            selected,
            Array.Empty<RaxportPeakRecord>(),
            600.0,
            2.0,
            5,
            10,
            preferredCharge: 2);

        CollectionAssert.AreEqual(new[] { 2, 2, 3, 4 }, candidates.Select(candidate => candidate.Charge).ToArray());
        CollectionAssert.AreEqual(new[] { 600.0, 601.0, 601.0, 601.0 }, candidates.Select(candidate => candidate.Mz).ToArray());
    }

    [TestMethod]
    public void PrecursorSelectorRemovesDuplicateCandidates()
    {
        RaxportPeakRecord[] selected =
        {
            new(600.0, 1000, 0, 0, 0, 0),
            new(600.0, 900, 0, 0, 0, 0),
            new(601.0, 800, 0, 0, 0, 0)
        };

        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(
            selected,
            Array.Empty<RaxportPeakRecord>(),
            600.0,
            2.0,
            6,
            10,
            preferredCharge: 2);

        CollectionAssert.AreEqual(new[] { 2, 2, 3, 4 }, candidates.Select(candidate => candidate.Charge).ToArray());
        CollectionAssert.AreEqual(new[] { 600.0, 601.0, 601.0, 601.0 }, candidates.Select(candidate => candidate.Mz).ToArray());
        Assert.AreEqual(1000, candidates[0].Intensity, 0.0001);
    }

    [TestMethod]
    public void PrecursorSelectorDeduplicatesSameChargeWithinMzTolerance()
    {
        RaxportPeakRecord[] selected =
        {
            new(430.88861083984375, 100, 0, 0, 0, 3),
            new(430.8926696777344, 200, 0, 0, 0, 3),
            new(431.02, 50, 0, 0, 0, 3)
        };

        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(
            selected,
            Array.Empty<RaxportPeakRecord>(),
            431.0,
            2.0,
            5,
            10);

        Assert.AreEqual(2, candidates.Count);
        CollectionAssert.AreEqual(new[] { 3, 3 }, candidates.Select(candidate => candidate.Charge).ToArray());
        Assert.AreEqual(430.8926696777344, candidates[0].Mz, 0.0000001);
        Assert.AreEqual(200, candidates[0].Intensity, 0.0001);
        Assert.AreEqual(431.02, candidates[1].Mz, 0.0000001);
    }

    [TestMethod]
    public void PrecursorSelectorUsesRealPeakChargeBeforePreferredCharge()
    {
        RaxportPeakRecord[] selected =
        {
            new(600.0, 1000, 0, 0, 0, 5)
        };

        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(
            selected,
            selected,
            600.0,
            1.0,
            1,
            10,
            preferredCharge: 2);

        CollectionAssert.AreEqual(new[] { 5 }, candidates.Select(candidate => candidate.Charge).ToArray());
    }


    [TestMethod]
    public void PrecursorSelectorAcceptsPeakChargeSeven()
    {
        RaxportPeakRecord[] selected =
        {
            new(600.0, 1000, 0, 0, 0, 7)
        };

        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(
            selected,
            selected,
            600.0,
            1.0,
            1,
            10,
            preferredCharge: 2);

        CollectionAssert.AreEqual(new[] { 7 }, candidates.Select(candidate => candidate.Charge).ToArray());
    }

    [TestMethod]
    public void PrecursorSelectorUsesPreferredChargeOnlyForTrailerMz()
    {
        RaxportPeakRecord[] selected =
        {
            new(600.0000, 1000, 0, 0, 0, 20),
            new(601.0000, 900, 0, 0, 0, 0)
        };

        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(
            selected,
            selected,
            600.0005,
            2.0,
            2,
            10,
            preferredCharge: 6);

        CollectionAssert.AreEqual(new[] { 6, 2, 3, 4 }, candidates.Select(candidate => candidate.Charge).ToArray());
        CollectionAssert.AreEqual(new[] { 600.0, 601.0, 601.0, 601.0 }, candidates.Select(candidate => candidate.Mz).ToArray());
    }

    [TestMethod]
    public void PrecursorSelectorGuessesWhenThermoChargesAreOutOfRange()
    {
        RaxportPeakRecord[] selected =
        {
            new(600.0, 1000, 0, 0, 0, 20)
        };

        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(
            selected,
            selected,
            600.0,
            1.0,
            3,
            10,
            preferredCharge: 15);

        CollectionAssert.AreEqual(new[] { 2, 3, 4 }, candidates.Select(candidate => candidate.Charge).ToArray());
    }

    [TestMethod]
    public void PrecursorSelectorUsesDefaultGuessInsteadOfStrongIsotopeCharge()
    {
        RaxportPeakRecord[] evidence =
        {
            new(500.000000, 1000, 0, 0, 0, 0),
            new(500.334452, 700, 0, 0, 0, 0),
            new(500.668903, 500, 0, 0, 0, 0),
            new(501.003355, 300, 0, 0, 0, 0)
        };
        RaxportPeakRecord[] selected = { evidence[0] };

        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(
            selected,
            evidence,
            500.0,
            2.0,
            1,
            10);

        Assert.AreEqual(2, candidates[0].Charge);
    }

    [TestMethod]
    public void PrecursorSelectorUsesHighestIsotopeScoreBeforeChargePrior()
    {
        RaxportPeakRecord[] evidence =
        {
            new(500.000000, 1000, 0, 0, 0, 0),
            new(500.501678, 900, 0, 0, 0, 0),
            new(501.003355, 800, 0, 0, 0, 0),
            new(501.505033, 700, 0, 0, 0, 0),
            new(502.006710, 600, 0, 0, 0, 0),
            new(503.010065, 500, 0, 0, 0, 0)
        };
        RaxportPeakRecord[] selected = { evidence[0] };

        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(
            selected,
            evidence,
            500.0,
            4.0,
            1,
            10);

        Assert.AreEqual(2, candidates[0].Charge);
    }

    [TestMethod]
    public void PrecursorSelectorUsesDefaultGuessInsteadOfWeakIsotopeCharge()
    {
        RaxportPeakRecord[] evidence =
        {
            new(500.000000, 1000, 0, 0, 0, 0),
            new(500.200671, 700, 0, 0, 0, 0)
        };
        RaxportPeakRecord[] selected = { evidence[0] };

        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(
            selected,
            evidence,
            500.0,
            2.0,
            1,
            10);

        Assert.AreEqual(2, candidates[0].Charge);
    }

    [TestMethod]
    public void PrecursorSelectorKeepsDifferentChargesAtSameMz()
    {
        RaxportPeakRecord[] selected =
        {
            new(700.0, 1000, 0, 0, 0, 0)
        };

        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(selected, 3);

        CollectionAssert.AreEqual(new[] { 2, 3, 4 }, candidates.Select(candidate => candidate.Charge).ToArray());
        Assert.IsTrue(candidates.All(candidate => Math.Abs(candidate.Mz - 700.0) < 0.0001));
    }

    [TestMethod]
    public void PrecursorSelectorDeduplicatesByMzToleranceAndKeepsStrongestEvidence()
    {
        RaxportPeakRecord[] selected =
        {
            new(600.0000, 100, 0, 0, 0, 0, null, 1.10),
            new(600.0030, 500, 0, 0, 0, 0, null, 1.30)
        };

        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(
            selected,
            selected,
            600.0,
            1.0,
            1,
            10,
            preferredCharge: 2);

        Assert.AreEqual(1, candidates.Count);
        Assert.AreEqual(600.0030, candidates[0].Mz, 0.0001);
        Assert.AreEqual(500, candidates[0].Intensity, 0.0001);
        Assert.AreEqual(1.30, candidates[0].OneOverK0, 0.0001);
    }

    [TestMethod]
    public void WritesPeakMobilityTraceOffsets()
    {
        string path = Path.Combine(Path.GetTempPath(), $"raxport-mobility-traces-{Guid.NewGuid():N}.h5");
        try
        {
            using Hdf5Writer writer = new(path, "traces.d", "timsTOF", "test", 20000);
            writer.AddScan(new RaxportScanRecord(
                1,
                1,
                0.5,
                1234,
                "Bruker MS1 frame=1",
                string.Empty,
                0,
                null,
                new[]
                {
                    new RaxportPeakRecord(500.1, 1000, 0, 0, 0, 0, new RaxportPeakMobilityTrace(
                        new[] { 10, 12 },
                        new[] { 20f, 30f })),
                    new RaxportPeakRecord(600.2, 2000, 0, 0, 0, 0)
                }));
        }
        finally
        {
            AssertHdf5FileCreated(path);
            AssertPeakMobilityTraceSchema(path);
            File.Delete(path);
        }
    }

    [TestMethod]
    public void BrukerRawScanBufferParserReadsCountsIndicesAndIntensities()
    {
        uint[] buffer = { 2, 1, 10, 11, 7, 20, 30, 40 };

        IReadOnlyList<BrukerRawScan> scans = BrukerTimsReader.ParseRawScanBuffer(buffer, 5, 7);

        Assert.AreEqual(2, scans.Count);
        Assert.AreEqual(5, scans[0].ScanNumber);
        CollectionAssert.AreEqual(new uint[] { 10, 11 }, scans[0].Indices);
        CollectionAssert.AreEqual(new uint[] { 7, 20 }, scans[0].Intensities);
        Assert.AreEqual(6, scans[1].ScanNumber);
        CollectionAssert.AreEqual(new uint[] { 30 }, scans[1].Indices);
        CollectionAssert.AreEqual(new uint[] { 40 }, scans[1].Intensities);
    }

    [TestMethod]
    public void BrukerMobilityTraceMatchingAggregatesNearestCentroidWithinPpm()
    {
        RaxportPeakRecord[] centroids =
        {
            new(500.0000, 1000, 0, 0, 0, 0),
            new(501.0000, 2000, 0, 0, 0, 0),
            new(600.0000, 3000, 0, 0, 0, 0)
        };
        BrukerRawScan[] rawScans =
        {
            new(10, new uint[] { 1, 2, 3 }, new uint[] { 10, 15, 99 }),
            new(11, new uint[] { 4 }, new uint[] { 40 }),
            new(12, new uint[] { 5 }, new uint[] { 0 })
        };
        double[][] mzByScan =
        {
            new[] { 500.0010, 500.0030, 700.0 },
            new[] { 501.0005 },
            new[] { 500.0005 }
        };
        double[] oneOverK0 = new double[13];
        oneOverK0[10] = 1.10;
        oneOverK0[11] = 1.20;
        oneOverK0[12] = 1.30;

        List<RaxportPeakRecord> peaks = BrukerTimsReader.BuildMobilityTracePeaks(
            centroids,
            rawScans,
            mzByScan,
            oneOverK0,
            10,
            out int omittedCentroids);

        Assert.AreEqual(1, omittedCentroids);
        Assert.AreEqual(2, peaks.Count);
        Assert.AreEqual(1000, peaks[0].Intensity, 0.0001, "Peak row intensity should remain the collapsed v3 area.");
        Assert.AreEqual(1, peaks[0].MobilityTrace!.Count, "Zero-intensity matched points should not be stored.");
        Assert.AreEqual(10, peaks[0].MobilityTrace.OneOverK0Indices[0]);
        Assert.AreEqual(25, peaks[0].MobilityTrace.Intensities[0], 0.0001);
        Assert.AreEqual(11, peaks[1].MobilityTrace!.OneOverK0Indices[0]);
        Assert.AreEqual(40, peaks[1].MobilityTrace.Intensities[0], 0.0001);
    }

    [TestMethod]
    public void PrecursorSelectorUsesMobilityWindowTraceIntensityAndCandidateMobility()
    {
        RaxportPeakRecord[] peaks =
        {
            new(500.0, 1000, 0, 0, 0, 0, new RaxportPeakMobilityTrace(
                new[] { 0, 20 },
                new[] { 10f, 100f })),
            new(501.0, 900, 0, 0, 0, 0, new RaxportPeakMobilityTrace(
                new[] { 0 },
                new[] { 50f }))
        };

        double[] oneOverK0Axis = new double[21];
        oneOverK0Axis[0] = 1.00;
        oneOverK0Axis[20] = 1.20;
        List<RaxportPeakRecord> selected = PrecursorSelector.FindPrecursorPeaks(peaks, 500.0, 3.0, 1, 0.99, 10, 1.15, 1.25, oneOverK0Axis);
        List<RaxportPrecursorCandidateRecord> candidates = PrecursorSelector.ExpandPrecursorCandidates(selected, 3);

        Assert.AreEqual(1, selected.Count);
        Assert.AreEqual(500.0, selected[0].Mz, 0.0001);
        Assert.AreEqual(100, selected[0].Intensity, 0.0001);
        CollectionAssert.AreEqual(new[] { 2, 3, 4 }, candidates.Select(candidate => candidate.Charge).ToArray());
        Assert.IsTrue(candidates.All(candidate => Math.Abs(candidate.Intensity - 100) < 0.0001));
        Assert.IsTrue(candidates.All(candidate => Math.Abs(candidate.OneOverK0 - 1.20) < 0.0001));
    }

    private static RaxportScanRecord CreateScan(int scanNumber, int msOrder, RaxportReactionRecord? reaction)
    {
        return new RaxportScanRecord(
            scanNumber,
            msOrder,
            scanNumber / 10.0,
            1000 + scanNumber,
            $"filter {scanNumber}",
            msOrder == 1 ? string.Empty : "HCD",
            msOrder == 1 ? 0 : scanNumber - 1,
            reaction,
            new[]
            {
                new RaxportPeakRecord(100 + scanNumber, 200, 300, 0, 10, 2),
                new RaxportPeakRecord(101 + scanNumber, 201, 301, 0, 11, 3)
            });
    }

    private static RaxportReactionRecord CreateReaction(double oneOverK0Begin = 0, double oneOverK0End = 0)
    {
        return new RaxportReactionRecord(
            500.2,
            1.6,
            2,
            30,
            true,
            "HCD",
            false,
            false,
            0,
            0,
            0,
            new[]
            {
                new RaxportPrecursorCandidateRecord(2, 500.2, 123.4),
                new RaxportPrecursorCandidateRecord(3, 501.2, 567.8)
            },
            oneOverK0Begin,
            oneOverK0End);
    }

    private static void AssertHdf5FileCreated(string path)
    {
        Assert.IsTrue(File.Exists(path), "HDF5 file was not created.");
        byte[] signature = File.ReadAllBytes(path).Take(8).ToArray();
        CollectionAssert.AreEqual(new byte[] { 0x89, 0x48, 0x44, 0x46, 0x0d, 0x0a, 0x1a, 0x0a }, signature);
    }


    private static void AssertPeakMobilityTraceSchema(string path)
    {
        using Process process = new();
        process.StartInfo.FileName = "python";
        process.StartInfo.ArgumentList.Add("-c");
        process.StartInfo.ArgumentList.Add(
            @"import h5py, math, sys
with h5py.File(sys.argv[1], 'r') as f:
    if int(f.attrs['schema_version']) != 5:
        raise SystemExit('unexpected schema_version: %s' % f.attrs['schema_version'])
    if '/peaks/one_over_k0' in f:
        raise SystemExit('peaks/one_over_k0 should not exist')
    starts = f['/peaks/mobility_trace_start'][:].astype(int).tolist()
    counts = f['/peaks/mobility_trace_count'][:].astype(int).tolist()
    if starts != [0, -1]:
        raise SystemExit(f'unexpected trace starts: {starts}')
    if counts != [2, 0]:
        raise SystemExit(f'unexpected trace counts: {counts}')
    if '/peak_mobility_traces/one_over_k0' in f:
        raise SystemExit('peak_mobility_traces/one_over_k0 should not exist')
    index = f['/peak_mobility_traces/one_over_k0_index'][:].astype(int).tolist()
    intensity_ds = f['/peak_mobility_traces/intensity']
    intensity = intensity_ds[:].tolist()
    if index != [10, 12]:
        raise SystemExit(f'unexpected trace one_over_k0_index: {index}')
    if str(intensity_ds.dtype) != 'float32':
        raise SystemExit(f'unexpected trace intensity dtype: {intensity_ds.dtype}')
    if any(abs(a - b) > 1e-6 for a, b in zip(intensity, [20.0, 30.0])):
        raise SystemExit(f'unexpected trace intensity: {intensity}')
");
        process.StartInfo.ArgumentList.Add(path);
        process.StartInfo.RedirectStandardError = true;
        process.StartInfo.UseShellExecute = false;

        process.Start();
        Assert.IsTrue(process.WaitForExit(10000), "Timed out while reading peak mobility trace datasets.");
        Assert.AreEqual(0, process.ExitCode, process.StandardError.ReadToEnd());
    }

    private static void AssertReactionMobilityRange(string path, double expectedBegin, double expectedEnd)
    {
        using Process process = new();
        process.StartInfo.FileName = "python";
        process.StartInfo.ArgumentList.Add("-c");
        process.StartInfo.ArgumentList.Add(
            @"import h5py, math, sys
with h5py.File(sys.argv[1], 'r') as f:
    begin = f['/reactions/one_over_k0_begin'][0]
    end = f['/reactions/one_over_k0_end'][0]
    if not math.isclose(begin, float(sys.argv[2]), rel_tol=0, abs_tol=1e-9):
        raise SystemExit(f'unexpected one_over_k0_begin: {begin}')
    if not math.isclose(end, float(sys.argv[3]), rel_tol=0, abs_tol=1e-9):
        raise SystemExit(f'unexpected one_over_k0_end: {end}')
    intensity = f['/precursor_candidates/intensity'][:].tolist()
    if any(abs(a - b) > 1e-6 for a, b in zip(intensity, [123.4, 567.8])):
        raise SystemExit(f'unexpected precursor candidate intensity: {intensity}')
");
        process.StartInfo.ArgumentList.Add(path);
        process.StartInfo.ArgumentList.Add(expectedBegin.ToString(System.Globalization.CultureInfo.InvariantCulture));
        process.StartInfo.ArgumentList.Add(expectedEnd.ToString(System.Globalization.CultureInfo.InvariantCulture));
        process.StartInfo.RedirectStandardError = true;
        process.StartInfo.UseShellExecute = false;

        process.Start();
        Assert.IsTrue(process.WaitForExit(10000), "Timed out while reading reaction 1/K0 datasets.");
        Assert.AreEqual(0, process.ExitCode, process.StandardError.ReadToEnd());
    }
}
#endif
