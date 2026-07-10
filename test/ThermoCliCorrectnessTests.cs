#if RAXPORT_TESTS
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Raxport;

[TestClass]
public sealed class ThermoCliCorrectnessTests
{
    [TestMethod]
    public void ThermoCollectedPeaksAreFilteredAndSortedByMz()
    {
        List<RaxportPeakRecord> peaks = new()
        {
            new(501.0, 20.0, 0, 0, 0, 0),
            new(double.NaN, 10.0, 0, 0, 0, 0),
            new(499.0, double.PositiveInfinity, 0, 0, 0, 0),
            new(0.0, 10.0, 0, 0, 0, 0),
            new(502.0, 0.0, 0, 0, 0, 0),
            new(500.0, 10.0, 0, 0, 0, 0),
            new(double.PositiveInfinity, 10.0, 0, 0, 0, 0),
            new(503.0, -1.0, 0, 0, 0, 0)
        };

        List<RaxportPeakRecord> normalized = ThermoRawFileConverter.NormalizeCollectedPeaks(peaks);

        Assert.AreSame(peaks, normalized);
        CollectionAssert.AreEqual(new[] { 500.0, 501.0 }, normalized.Select(peak => peak.Mz).ToArray());
        Assert.IsTrue(normalized.All(peak => double.IsFinite(peak.Mz) && peak.Mz > 0));
        Assert.IsTrue(normalized.All(peak => double.IsFinite(peak.Intensity) && peak.Intensity > 0));
    }

    [TestMethod]
    public void ThermoProfileNormalizationPreservesFiniteNonPositiveSamples()
    {
        List<RaxportPeakRecord> peaks = new()
        {
            new(501.0, 0.0, 0, 0, 0, 0),
            new(500.0, -2.0, 0, 0, 0, 0),
            new(502.0, double.NaN, 0, 0, 0, 0)
        };

        List<RaxportPeakRecord> normalized = ThermoRawFileConverter.NormalizeCollectedPeaks(
            peaks,
            preserveNonPositiveIntensity: true);

        CollectionAssert.AreEqual(new[] { 500.0, 501.0 }, normalized.Select(peak => peak.Mz).ToArray());
        CollectionAssert.AreEqual(new[] { -2.0, 0.0 }, normalized.Select(peak => peak.Intensity).ToArray());
    }

    [TestMethod]
    public void MzToleranceMustBeNonNegativeAndFinite()
    {
        Assert.IsTrue(Raxport.TryParseNonNegativeFinite("0", out _));
        Assert.IsTrue(Raxport.TryParseNonNegativeFinite("10.5", out double value));
        Assert.AreEqual(10.5, value, 0.000001);
        Assert.IsFalse(Raxport.TryParseNonNegativeFinite("-1", out _));
        Assert.IsFalse(Raxport.TryParseNonNegativeFinite("Infinity", out _));
        Assert.IsFalse(Raxport.TryParseNonNegativeFinite("not-a-number", out _));
    }

    [TestMethod]
    public void PrecursorIntensityFractionMustBeInUnitInterval()
    {
        Assert.IsTrue(Raxport.TryParseUnitFraction("0.99", out double value));
        Assert.AreEqual(0.99, value, 0.000001);
        Assert.IsTrue(Raxport.TryParseUnitFraction("1", out _));
        Assert.IsFalse(Raxport.TryParseUnitFraction("0", out _));
        Assert.IsFalse(Raxport.TryParseUnitFraction("-0.1", out _));
        Assert.IsFalse(Raxport.TryParseUnitFraction("1.01", out _));
        Assert.IsFalse(Raxport.TryParseUnitFraction("NaN", out _));
        Assert.IsFalse(Raxport.TryParseUnitFraction("not-a-number", out _));
    }

    [TestMethod]
    public void TopNRequiresAPositiveInteger()
    {
        Assert.IsTrue(Raxport.TryParseTopN("1", out int one));
        Assert.AreEqual(1, one);
        Assert.IsTrue(Raxport.TryParseTopN("15", out int fifteen));
        Assert.AreEqual(15, fifteen);

        Assert.IsFalse(Raxport.TryParseTopN("0", out _));
        Assert.IsFalse(Raxport.TryParseTopN("-1", out _));
        Assert.IsFalse(Raxport.TryParseTopN("1.5", out _));
        Assert.IsFalse(Raxport.TryParseTopN("not-a-number", out _));
        Assert.IsFalse(Raxport.TryParseTopN(string.Empty, out _));
    }
}
#endif
