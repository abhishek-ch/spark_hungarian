package com.abc.hungarian.scanner

import com.abc.hungarian.utils.AbstractDatasetSuitSpec

class RowColReductionTest extends AbstractDatasetSuitSpec {

  describe(".rowColumnReduction") {
    it("test row") {
      val inputSeq = Seq(
        (9, 11, 14, 11, 7),
        (6, 15, 13, 13, 10),
        (12, 13, 6, 8, 8),
        (11, 9, 10, 12, 9),
        (7, 12, 14, 10, 14)
      ).map(
        eachTuple =>
          (eachTuple._1.toLong,
           eachTuple._2.toLong,
           eachTuple._3.toLong,
           eachTuple._4.toLong,
           eachTuple._5.toLong))

      val df = spark.createDataFrame(inputSeq).toDF

      val rowDF = RowColReduction.step1_RowReduction(df)
      val expectedRowDF = spark
        .createDataFrame(
          Seq(
            (2L, 4L, 7L, 4L, 0L),
            (0L, 9L, 7L, 7L, 4L),
            (6L, 7L, 0L, 2L, 2L),
            (2L, 0L, 1L, 3L, 0L),
            (0L, 5L, 7l, 3L, 7L)
          )
        )
        .toDF

      assertDataFrameEquals(expectedRowDF, rowDF)

      val colDF = RowColReduction.step2_ColReduction(rowDF)
      val expectedColDF = spark
        .createDataFrame(
          Seq(
            (2L, 4L, 7L, 2L, 0L),
            (0L, 9L, 7L, 5L, 4L),
            (6L, 7L, 0L, 0L, 2L),
            (2L, 0L, 1L, 1L, 0L),
            (0L, 5L, 7l, 1L, 7L)
          )
        )
        .toDF

      assertDataFrameEquals(expectedColDF, colDF)

    }
  }

}
