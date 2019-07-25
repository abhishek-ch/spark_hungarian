package com.abc.hungarian.runner

import com.abc.hungarian.utils.AbstractDatasetSuitSpec

class MainRunnerTest extends AbstractDatasetSuitSpec {
  describe(".runEndToEnd") {
    it("run end to end and get the final optimized dataframe") {
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

      val resultList = MainRunner.run(df)
      assert(resultList.length > 0)
      assert(resultList(0) === List[Long](5, 1, 3, 2, 4))

    }

    it("single iteration gave optimized result") {
      val df = spark
        .createDataFrame(
          Seq(
            (250L, 400L, 350L),
            (400L, 600L, 350L),
            (200L, 400L, 250L)
          ))
        .toDF

      val resultList = MainRunner.run(df)
      println(resultList)
      assert(resultList.length > 0)
      assert(resultList(0) === List[Long](2, 3, 1))
    }

    it("end to end another trial") {
      val df = spark
        .createDataFrame(
          Seq(
            (90L, 75L, 75L, 80L),
            (35L, 85L, 55L, 65L),
            (125L, 95L, 90L, 105L),
            (45L, 110L, 95L, 115L)
          ))
        .toDF

      val resultList = MainRunner.run(df)
      println(resultList)

    }
  }

}
