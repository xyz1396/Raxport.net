#if RAXPORT_TESTS
using System.IO.Compression;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Raxport;

[TestClass]
public sealed class MzMlWriterTests
{
    private static readonly XNamespace Ns = "http://psi.hupo.org/ms/mzml";

    [TestMethod]
    public void WritesIndexedMzMlWithOffsetsAndDecodedBinaryArrays()
    {
        string path = Path.Combine(Path.GetTempPath(), $"raxport-mzml-{Guid.NewGuid():N}.mzML");
        try
        {
            using (MzMlWriter writer = new(path, "sample.raw", "Orbitrap", "test-version"))
            {
                writer.AddScan(new RaxportScanRecord(
                    10,
                    1,
                    1.25,
                    1500,
                    "FTMS + p NSI Full ms [100.0000-1000.0000]",
                    string.Empty,
                    0,
                    null,
                    new[]
                    {
                        new RaxportPeakRecord(100.5, 20.0, 60000.0, 1.0, 2.0, 2, new RaxportPeakMobilityTrace(new[] { 10, 11 }, new[] { 100.0f, 200.0f })),
                        new RaxportPeakRecord(200.5, 50.0, 65000.0, 1.5, 3.0, 3, new RaxportPeakMobilityTrace(new[] { 12 }, new[] { 300.0f }))
                    },
                    true,
                    100,
                    1000,
                    "Positive"));

                writer.AddScan(new RaxportScanRecord(
                    11,
                    2,
                    1.50,
                    800,
                    "FTMS + p NSI d Full ms2 500.2000@hcd35.00 [100.0000-1500.0000]",
                    "HigherEnergyCollisionalDissociation",
                    10,
                    new RaxportReactionRecord(
                        500.2,
                        1.6,
                        3,
                        35,
                        true,
                        "HigherEnergyCollisionalDissociation",
                        false,
                        false,
                        0,
                        0,
                        0,
                        new[]
                        {
                            new RaxportPrecursorCandidateRecord(3, 500.2, 12345.0, 1.11),
                            new RaxportPrecursorCandidateRecord(2, 501.2, 23456.0, 1.22)
                        },
                        1.05,
                        1.15),
                    new[]
                    {
                        new RaxportPeakRecord(150.25, 10.0, 0, 0, 0, 0),
                        new RaxportPeakRecord(250.25, 40.0, 0, 0, 0, 0)
                    },
                    true,
                    100,
                    1500,
                    "Positive"));
            }

            string xml = File.ReadAllText(path, Encoding.UTF8);
            Assert.IsTrue(xml.Contains("<indexedmzML", StringComparison.Ordinal));
            Assert.IsTrue(xml.Contains("<index name=\"spectrum\">", StringComparison.Ordinal));
            Assert.IsTrue(xml.Contains("<index name=\"chromatogram\">", StringComparison.Ordinal));
            Assert.IsTrue(xml.Contains("<indexListOffset>", StringComparison.Ordinal));
            Assert.IsTrue(Regex.IsMatch(xml, "<fileChecksum>[0-9a-f]{40}</fileChecksum>"));
            Assert.IsTrue(xml.Contains("one_over_k0_begin", StringComparison.Ordinal));
            Assert.IsTrue(xml.Contains("beam-type collision-induced dissociation", StringComparison.Ordinal));
            Assert.IsTrue(xml.Contains("Raxport signal-to-noise array", StringComparison.Ordinal));
            Assert.IsTrue(xml.Contains("Raxport mobility trace start array", StringComparison.Ordinal));
            Assert.IsTrue(xml.Contains("Raxport mobility trace one_over_k0_index array", StringComparison.Ordinal));
            Assert.IsTrue(xml.Contains("Raxport precursor candidate count", StringComparison.Ordinal));
            Assert.IsTrue(xml.Contains("Raxport precursor candidate 1 one_over_k0", StringComparison.Ordinal));

            byte[] bytes = File.ReadAllBytes(path);
            foreach (Match match in Regex.Matches(xml, "<offset idRef=\"([^\"]+)\">([0-9]+)</offset>"))
            {
                string idRef = match.Groups[1].Value;
                int offset = int.Parse(match.Groups[2].Value);
                string atOffset = Encoding.UTF8.GetString(bytes, offset, Math.Min(32, bytes.Length - offset));
                if (idRef.StartsWith("controllerType=", StringComparison.Ordinal))
                {
                    Assert.IsTrue(atOffset.StartsWith("<spectrum", StringComparison.Ordinal), idRef);
                }
                else if (idRef == "BasePeak_0")
                {
                    Assert.IsTrue(atOffset.StartsWith("<chromatogram", StringComparison.Ordinal), idRef);
                }
            }

            XDocument document = XDocument.Parse(xml);
            XElement mzMl = document.Root!.Element(Ns + "mzML")!;
            XElement spectrumList = mzMl.Element(Ns + "run")!.Element(Ns + "spectrumList")!;
            Assert.AreEqual("2", spectrumList.Attribute("count")!.Value);

            XElement firstSpectrum = spectrumList.Elements(Ns + "spectrum").First();
            Assert.AreEqual("10", firstSpectrum.Attribute("id")!.Value.Split("scan=")[1]);
            CollectionAssert.AreEqual(new[] { 100.5, 200.5 }, DecodeBinaryArray(firstSpectrum, "MS:1000514"));
            CollectionAssert.AreEqual(new[] { 20.0, 50.0 }, DecodeBinaryArray(firstSpectrum, "MS:1000515"));
            CollectionAssert.AreEqual(new[] { 2.0, 3.0 }, DecodeBinaryArray(firstSpectrum, "MS:1000516"));
            CollectionAssert.AreEqual(new[] { 60000.0, 65000.0 }, DecodeBinaryArray(firstSpectrum, "MS:1002529"));
            CollectionAssert.AreEqual(new[] { 2.0, 3.0 }, DecodeUserBinaryArray(firstSpectrum, "Raxport signal-to-noise array"));
            CollectionAssert.AreEqual(new[] { 0.0, 2.0 }, DecodeUserBinaryArray(firstSpectrum, "Raxport mobility trace start array"));
            CollectionAssert.AreEqual(new[] { 2.0, 1.0 }, DecodeUserBinaryArray(firstSpectrum, "Raxport mobility trace count array"));
            CollectionAssert.AreEqual(new[] { 10.0, 11.0, 12.0 }, DecodeUserBinaryArray(firstSpectrum, "Raxport mobility trace one_over_k0_index array"));
            CollectionAssert.AreEqual(new[] { 100.0, 200.0, 300.0 }, DecodeUserBinaryArray(firstSpectrum, "Raxport mobility trace intensity array"));

            XElement secondSpectrum = spectrumList.Elements(Ns + "spectrum").Skip(1).First();
            Assert.IsNotNull(secondSpectrum.Element(Ns + "precursorList"));
            Assert.IsTrue(secondSpectrum.ToString().Contains("spectrumRef=\"controllerType=0 controllerNumber=1 scan=10\"", StringComparison.Ordinal));

            XElement selectedIon = secondSpectrum
                .Element(Ns + "precursorList")!
                .Element(Ns + "precursor")!
                .Element(Ns + "selectedIonList")!
                .Element(Ns + "selectedIon")!;
            AssertUserParam(selectedIon, "Raxport precursor candidate count", "2");
            AssertUserParam(selectedIon, "Raxport precursor candidate 0 charge", "3");
            AssertUserParam(selectedIon, "Raxport precursor candidate 0 mz", "500.2");
            AssertUserParam(selectedIon, "Raxport precursor candidate 0 intensity", "12345");
            AssertUserParam(selectedIon, "Raxport precursor candidate 0 one_over_k0", "1.11");
            AssertUserParam(selectedIon, "Raxport precursor candidate 1 charge", "2");
            AssertUserParam(selectedIon, "Raxport precursor candidate 1 mz", "501.2");
            AssertUserParam(selectedIon, "Raxport precursor candidate 1 intensity", "23456");
            AssertUserParam(selectedIon, "Raxport precursor candidate 1 one_over_k0", "1.22");
        }
        finally
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }

    private static void AssertUserParam(XElement element, string name, string value)
    {
        XElement userParam = element
            .Elements(Ns + "userParam")
            .Single(param => (string?)param.Attribute("name") == name);
        Assert.AreEqual(value, userParam.Attribute("value")!.Value);
    }

    private static double[] DecodeUserBinaryArray(XElement spectrum, string name)
    {
        XElement binaryDataArray = spectrum
            .Descendants(Ns + "binaryDataArray")
            .Single(array => array.Elements(Ns + "userParam").Any(param => (string?)param.Attribute("name") == name));
        return DecodeBinaryDataArray(binaryDataArray);
    }

    private static double[] DecodeBinaryArray(XElement spectrum, string accession)
    {
        XElement binaryDataArray = spectrum
            .Descendants(Ns + "binaryDataArray")
            .Single(array => array.Elements(Ns + "cvParam").Any(param => (string?)param.Attribute("accession") == accession));
        return DecodeBinaryDataArray(binaryDataArray);
    }
    private static double[] DecodeBinaryDataArray(XElement binaryDataArray)
    {
        string encoded = binaryDataArray.Element(Ns + "binary")!.Value;
        byte[] compressed = Convert.FromBase64String(encoded);
        using MemoryStream input = new(compressed);
        using ZLibStream zlib = new(input, CompressionMode.Decompress);
        using MemoryStream output = new();
        zlib.CopyTo(output);
        byte[] bytes = output.ToArray();
        double[] values = new double[bytes.Length / sizeof(double)];
        Buffer.BlockCopy(bytes, 0, values, 0, bytes.Length);
        return values;
    }

}
#endif
