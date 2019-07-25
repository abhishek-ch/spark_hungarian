package com.abc.hungarian.scanner

import com.abc.hungarian.utils.AbstractDatasetSuitSpec

class DataOptimizationTest extends AbstractDatasetSuitSpec {

  describe(".findMinUndeletedCellValues") {
    it("test me") {
      val df = spark
        .createDataFrame(
          Seq(
            (9L, 11L, 14L, 11L, 7L),
            (6L, 15L, 13L, 13L, 10L),
            (12L, 13L, 6L, 8L, 8L),
            (11L, 9L, 10L, 12L, 9L),
            (7L, 12L, 14L, 10L, 14L)
          ))
        .toDF

      val rowDF = RowColReduction.step1_RowReduction(df)
      val colDF = RowColReduction.step2_ColReduction(rowDF)

      val rowSeq = DFScanner.step3_rowLines(colDF)
      val colSeq = DFScanner.step4_columnLines(colDF, rowSeq)

      println(
        s"Row Seqeuence ${rowSeq.mkString(",")} Column Sequence ${colSeq.mkString(",")}")

      colDF.show()

      val resultDF =
        DataOptimization.step5_addAndDeleteGlobalMinFromDataFrame(colDF,
                                                                  rowSeq,
                                                                  colSeq)
      resultDF.show()

    }

  }

}
