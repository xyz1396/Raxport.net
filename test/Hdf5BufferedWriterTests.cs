#if RAXPORT_TESTS
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Raxport;

[TestClass]
public sealed class Hdf5BufferedWriterTests
{
    [TestMethod]
    public void FlushesAcrossConfiguredBoundary()
    {
        string path = Path.Combine(Path.GetTempPath(), $"raxport-boundary-{Guid.NewGuid():N}.h5");
        try
        {
            using Hdf5BufferedWriter writer = new(path, "boundary.raw", "test instrument", "test", 2);
            writer.AddScan(CreateScan(1, 1, null));
            writer.AddScan(CreateScan(2, 1, null));
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
            using Hdf5BufferedWriter writer = new(path, "offsets.raw", "test instrument", "test", 20000);
            writer.AddScan(CreateScan(10, 1, null));
            writer.AddScan(CreateScan(11, 3, CreateReaction()));
        }
        finally
        {
            AssertHdf5FileCreated(path);
            File.Delete(path);
        }
    }

    private static Hdf5ScanRecord CreateScan(int scanNumber, int msOrder, Hdf5ReactionRecord? reaction)
    {
        return new Hdf5ScanRecord(
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
                new Hdf5PeakRecord(100 + scanNumber, 200, 300, 0, 10, 2),
                new Hdf5PeakRecord(101 + scanNumber, 201, 301, 0, 11, 3)
            });
    }

    private static Hdf5ReactionRecord CreateReaction()
    {
        return new Hdf5ReactionRecord(
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
                new Hdf5PrecursorCandidateRecord(2, 500.2),
                new Hdf5PrecursorCandidateRecord(3, 501.2)
            });
    }

    private static void AssertHdf5FileCreated(string path)
    {
        Assert.IsTrue(File.Exists(path), "HDF5 file was not created.");
        byte[] signature = File.ReadAllBytes(path).Take(8).ToArray();
        CollectionAssert.AreEqual(new byte[] { 0x89, 0x48, 0x44, 0x46, 0x0d, 0x0a, 0x1a, 0x0a }, signature);
    }
}
#endif
