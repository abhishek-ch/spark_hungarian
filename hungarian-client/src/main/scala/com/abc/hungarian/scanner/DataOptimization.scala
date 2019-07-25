package com.abc.hungarian.scanner

import com.abc.hungarian.utils.SparkSessionImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

object DataOptimization extends SparkSessionImplicits {

  def findMinUndeletedCellValue(sourceDF: DataFrame,
                                rowCrossedIndices: Seq[Long],
                                colCrossedIndices: Seq[Long]): Long = {

    import spark.implicits._

    val colScannedDF: DataFrame =
      DFScanner.getUncrossedDataframe(sourceDF, rowCrossedIndices)
    val dfColumns = colScannedDF.columns

    val minDF = colScannedDF
      .withColumn("minRowValue", array_min(array(dfColumns.map(col): _*)))
      .select("minRowValue")

    val minArr = minDF.as[Long].collect()
    var globalMin = Long.MaxValue
    minArr.zipWithIndex.foreach {
      case (elem, idx) => {
        if (!colCrossedIndices.contains(idx))
          globalMin = math.min(globalMin, elem)
      }
    }

    println(s"Global Minimum Value from Undeleted Cells $globalMin")
    globalMin
  }

  def addRowNumberIndexInDataFrame(sourceDF: DataFrame): DataFrame = {
    val rdd = sourceDF.rdd.zipWithIndex.map {
      case (row, index) => Row.fromSeq(row.toSeq :+ index)
    }
    spark.createDataFrame(
      rdd,
      // Create schema for index column
      StructType(
        sourceDF.schema.fields :+ StructField("index", LongType, false)))
  }

  def step5_addAndDeleteGlobalMinFromDataFrame(
      sourceDF: DataFrame,
      rowCrossedIndices: Seq[Long],
      colCrossedIndices: Seq[Long]): DataFrame = {

    val globalMinima = spark.sparkContext.broadcast(
      findMinUndeletedCellValue(sourceDF, rowCrossedIndices, colCrossedIndices))
    val bcRowIndices = spark.sparkContext.broadcast(rowCrossedIndices)
    val bcColumnIndices = spark.sparkContext.broadcast(colCrossedIndices)

    val rdd = sourceDF.rdd.zipWithIndex.map {
      case (row, rowIndex) => {
        val columnValues = row.toSeq.map(_.toString.toLong)
        // iterate through columns and check for intersection points
        Row.fromSeq(columnValues.zipWithIndex.map {
          case (each, colIndex) => {
            // 1. find intersection points and add minimum value to it
            if (bcColumnIndices.value.contains(rowIndex) && bcRowIndices.value
                  .contains(colIndex))
              each + globalMinima.value
            // 2. find no intersection value and reduce globalMinima from it
            else if (!bcColumnIndices.value
                       .contains(rowIndex) && !bcRowIndices.value.contains(
                       colIndex))
              each - globalMinima.value
            // 3. find just crossed points and keep the same value
            else
              each
          }
        })
      }
    }

    spark.createDataFrame(rdd, sourceDF.schema)
  }

}
