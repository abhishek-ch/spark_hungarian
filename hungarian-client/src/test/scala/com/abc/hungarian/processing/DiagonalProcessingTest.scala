package com.abc.hungarian.processing

import com.abc.hungarian.utils.AbstractDatasetSuitSpec

class DiagonalProcessingTest extends AbstractDatasetSuitSpec {
  describe(".findNumberOfZeros") {
    it("must return valid unmarked zeroes") {

      val df = spark
        .createDataFrame(
          Seq(
            (40L, 0L, 5L, 0L),
            (0L, 45L, 0L, 0L),
            (55L, 0L, 0L, 5L),
            (0L, 60L, 50L, 60L)
          ))
        .toDF

      assert(
        DiagonalProcessing
          .findNumberOfZeros(df, Seq[Long](0), Seq[Long]())
          .length == 6)
      assert(
        DiagonalProcessing
          .findNumberOfZeros(df, Seq[Long](0), Seq[Long](0, 1))
          .length == 2)

      assert(
        DiagonalProcessing
          .findNumberOfZeros(df, Seq[Long](0), Seq[Long](0, 1, 2))
          .length == 0)
    }
  }

  describe(".testDiagonalRule") {
    it("diagonal rule must be able to optimize") {

      val df = spark
        .createDataFrame(
          Seq(
            (40L, 0L, 5L, 0L),
            (0L, 45L, 0L, 0L),
            (55L, 0L, 0L, 5L),
            (0L, 60L, 50L, 60L)
          ))
        .toDF

      DiagonalProcessing.stepX_diagonalTriggerLines(df,
                                                    Seq[Long](0),
                                                    Seq[Long]())

    }
  }

}
