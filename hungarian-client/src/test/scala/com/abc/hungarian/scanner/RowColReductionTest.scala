package com.abc.hungarian.scanner

import com.abc.hungarian.utils.AbstractDatasetSuitSpec

class RowColReductionTest extends AbstractDatasetSuitSpec {

  describe(".rowColumnReduction") {
    it("test row") {
      val df = spark.createDataFrame(Seq(
        (9, 11, 14, 11, 7),
        (6, 15, 13, 13, 10),
        (12, 13, 6, 8, 8),
        (11, 9, 10, 12, 9),
        (7, 12, 14, 10, 14)
      )).toDF

      val rowDF = RowColReduction.step1_RowReduction(df)
      rowDF.show()

      val colDF = RowColReduction.step2_ColReduction(rowDF)
      colDF.show()
    }
  }

}
