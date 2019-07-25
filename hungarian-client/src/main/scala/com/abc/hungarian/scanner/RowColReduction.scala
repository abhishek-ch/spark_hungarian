package com.abc.hungarian.scanner

import com.abc.hungarian.utils.SparkSessionImplicits
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer

object RowColReduction extends SparkSessionImplicits {

  def step1_RowReduction(sourceDF: DataFrame): DataFrame = {

    val rdd = sourceDF.rdd.map(row => {
      val allValues = row.toSeq.map(_.toString.toLong)
      val minVal = allValues.reduceLeft(_ min _)
      // Row.fromSeq converts the List to Tuple format, easiest way to Row
      Row.fromSeq(allValues.map(each => (each - minVal)))
    })

    spark.createDataFrame(rdd, sourceDF.schema)

  }

  def step2_ColReduction(rowScannedDF: DataFrame): DataFrame = {
    val minIndices = ArrayBuffer.empty[Long]
    for (eachCol <- rowScannedDF.columns) {
      val minValue = rowScannedDF
        .select(min(eachCol))
        .first()
        .toSeq
        .map(_.toString.toLong)
        .head
      minIndices += minValue
    }

    val rdd = rowScannedDF.rdd.map(row => {
      val allValues = row.toSeq.map(_.toString.toLong)
      Row.fromSeq(allValues.zipWithIndex.map {
        case (element, idx) =>
          element - minIndices(idx)
      })
    })

    spark.createDataFrame(rdd, rowScannedDF.schema)
  }

}
