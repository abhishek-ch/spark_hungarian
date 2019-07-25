package com.abc.hungarian.scanner

import com.abc.hungarian.utils.AbstractDatasetSuitSpec

class DFScannerTest extends AbstractDatasetSuitSpec {

  describe(".testScanner") {
    it("test") {
      val rowDF = spark.createDataFrame(Seq(
        (2, 4, 7, 2, 0),
        (0, 9, 7, 5, 4),
        (6, 7, 0, 0, 2),
        (2, 0, 1, 1, 0),
        (0, 5, 7, 1, 7)
      )).toDF


      val output_1 = DFScanner.step3_rowLines(rowDF)
      println(
        s"""Row Scanned - $output_1
           |Column Scanned - ${DFScanner.step4_columnLines(rowDF, output_1)}""".stripMargin)


      val df2 = spark.createDataFrame(Seq(
        (2, 4, 6, 1, 0),
        (0, 9, 6, 4, 4),
        (7, 8, 0, 0, 3),
        (2, 0, 0, 0, 0),
        (0, 5, 6, 0, 7)
      )).toDF

      val output_2 = DFScanner.step3_rowLines(df2)
      println(
        s"""Row Scanned - $output_2
           |Column Scanned - ${DFScanner.step4_columnLines(df2, output_2)}""".stripMargin)
    }
  }

}
