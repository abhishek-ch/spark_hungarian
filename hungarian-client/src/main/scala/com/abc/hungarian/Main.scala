package com.abc.hungarian

import com.abc.hungarian.runner.MainRunner
import com.abc.hungarian.utils.SparkSessionImplicits

import scala.util.Try

object Main extends SparkSessionImplicits {
  def main(args: Array[String]): Unit = {

    require(args.length > 0, "Arguments must be passed for dataframe input")

    // TODO add support of spark table and any other data type
    val inputDF = spark.read.parquet(args(0))
    val maxCycle = Try(args(1).toInt).getOrElse(3)

    MainRunner.run(inputDF, maxCycle)
  }
}
