package com.abc.hungarian

import com.abc.hungarian.utils.SparkSessionImplicits

object TestMainSpark extends SparkSessionImplicits {
  def main(args: Array[String]): Unit = {
    spark
      .createDataFrame(
        Seq((2029851929L, 2.01, 1250878652L), (2029851980L, 1.77, 1250878652L)))
      .toDF("strategyID", "troasValue", "accountid")
      .show()
  }
}
