using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using Raxport;
using System.Linq;
using ThermoFisher.CommonCore.Data;
using ThermoFisher.CommonCore.Data.Business;
using ThermoFisher.CommonCore.Data.Interfaces;
using ThermoFisher.CommonCore.RawFileReader;
using System.Collections.Generic;

namespace TestRaxport
{
    [TestClass]
    public class TestRaxport
    {
        [TestMethod]
        public void TestReadMS1scanNumbers()
        {
            FTwriter writer = new("D:/work/202208/AMD raw files/AMD_StandardEnrichment_SampleAlpha_98Percent15N_Velos_OrbiMS2_Run2_013110_12.raw",
                "D:\\work\\202401\\FTtest", false);
            int i;
            foreach (var x in writer!.MS1scanNumbers!)
            { i = x; };
            i = writer.MS1scanNumbers.Count();
            Assert.AreEqual(writer.MS1scanNumbers.Count(), 2278, 0.001, "MS1scanNumbers not debited correctly");
        }
        [TestMethod]
        public void TestGetScanNumberRanges()
        {
            FTwriter writer = new("D:/work/202208/AMD raw files/AMD_StandardEnrichment_SampleAlpha_98Percent15N_Velos_OrbiMS2_Run2_013110_12.raw",
                "D:\\work\\202401\\FTtest", false);
            writer.InitScanNumberRanges(5000);
            Assert.AreEqual(writer!.scanNumberRanges!.Count, 2, 0.001, "MS1scanNumbers not debited correctly");
        }
        [TestMethod]
        public void Test2SplitedWrite()
        {
            FTwriter writer = new("D:\\work\\202402\\X13N_1Da_overlap_ID111456_01_OA10034_10328_012424.raw",
            "D:\\work\\202402\\FTtest", false);
            writer.InitScanNumberRanges(20000);
            writer.SplitedWrite(7);
        }
        [TestMethod]
        public void TestBinarySearchMzRange()
        {
            FTwriter Iwriter = new();
            IRawDataPlus rawFile = RawFileReaderAdapter.FileFactory("D:/work/202208/AMD raw files/AMD_StandardEnrichment_SampleAlpha_98Percent15N_Velos_OrbiMS2_Run2_013110_12.raw"); ;
            rawFile.SelectInstrument(Device.MS, 1);
            Scan currentScan = Scan.FromFile(rawFile, 3815);
            LabelPeak[] peaks = currentScan.CentroidScan.GetLabelPeaks();
            //List<LabelPeak> peaksInRange = Iwriter.BinarySearchMzRange(peaks, 854, 862);
            //List<LabelPeak> peaksInRange = Iwriter.BinarySearchMzRange(peaks, 635, 640);
            List<LabelPeak> peaksInRange = Iwriter.BinarySearchMzRange(peaks, 528, 535);
            foreach (LabelPeak peak in peaksInRange)
            {
                Console.WriteLine(peak.Mass);
            }
        }
        [TestMethod]
        public void TestFindPrecursorPeaks()
        {
            FTwriter Iwriter = new();
            //IRawDataPlus rawFile = RawFileReaderAdapter.FileFactory("D:/work/202208/AMD raw files/AMD_StandardEnrichment_SampleAlpha_98Percent15N_Velos_OrbiMS2_Run2_013110_12.raw");
            IRawDataPlus rawFile = RawFileReaderAdapter.FileFactory("F:\\Astral\\X32_ID110716_01_OA10034_10314_121923.raw");
            rawFile.SelectInstrument(Device.MS, 1);
            //Scan currentScan = Scan.FromFile(rawFile, 3815);
            Scan currentScan = Scan.FromFile(rawFile, 98711);
            LabelPeak[] peaks = currentScan.CentroidScan.GetLabelPeaks();
            Iwriter.currentPrecusorScan = currentScan;
            //Iwriter.findPrecursorPeaks(655, 10, 5, 0.95);
            //Iwriter.findPrecursorPeaks(740, 10, 5, 0.95);
            //Iwriter.findPrecursorPeaks(662, 4, 5, 0.95);
            //Iwriter.findPrecursorPeaks(566, 4, 5, 0.95);
            //Iwriter.findPrecursorPeaks(382, 4, 5, 0.95);
            // for no peak region at right
            //Iwriter.findPrecursorPeaks(978, 4, 5, 0.95);
            //Iwriter.findPrecursorPeaks(962, 4, 5, 0.95);
            Iwriter.FindPrecursorPeaks(2000, 4, 5, 0.95);
            foreach (LabelPeak peak in Iwriter!.currentPrecurosrPeaks!)
            {
                Console.WriteLine(peak.Mass);
            }
        }
    }
}