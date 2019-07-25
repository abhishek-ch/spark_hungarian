package com.abc.hungarian.utils

import org.apache.spark.sql.SparkSession

trait SparkSessionImplicits {

  lazy implicit val spark: SparkSession = SparkSession
    .builder()
    .getOrCreate()

}
