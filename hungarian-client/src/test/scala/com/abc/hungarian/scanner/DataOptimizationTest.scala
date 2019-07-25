package com.abc.hungarian.scanner

import com.abc.hungarian.utils.AbstractDatasetSuitSpec

class DataOptimizationTest extends AbstractDatasetSuitSpec {

  describe(".testDataOptimization") {
    it("must return final global minima along with crossed rows columns") {
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
      assert(rowSeq == Seq[Long](0, 1, 4))
      assert(colSeq == Seq[Long](2))

      val expectedDF = spark
        .createDataFrame(
          Seq(
            (2L, 4L, 6L, 1L, 0L),
            (0L, 9L, 6L, 4L, 4L),
            (7L, 8L, 0L, 0L, 3L),
            (2L, 0L, 0L, 0L, 0L),
            (0L, 5L, 6L, 0L, 7L)
          )
        )
        .toDF

      val resultDF =
        DataOptimization.step5_addAndDeleteGlobalMinFromDataFrame(colDF,
                                                                  rowSeq,
                                                                  colSeq)
      assertDataFrameEquals(expectedDF, resultDF)

    }

  }

}
