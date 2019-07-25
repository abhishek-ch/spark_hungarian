package com.abc.hungarian.scanner

import com.abc.hungarian.utils.{SparkSessionImplicits, UtilityFunctions}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.{ArrayBuffer, Set}

object DFScanner extends SparkSessionImplicits {

  def step3_rowLines(rowDF: DataFrame,
                     whitelistCols: Seq[Long] = Seq[Long]()): Seq[Long] = {
    val rowColumns = rowDF.columns
    extractRowScannedCrossedColumnIndices(
      rowDF.withColumn(
        "zeroitIndex",
        UtilityFunctions.findZeroUdf(struct(rowColumns.map(col): _*),
                                     typedLit(whitelistCols))))
  }

  def extractRowScannedCrossedColumnIndices(
      rowScannedDF: DataFrame): Seq[Long] = {
    import spark.implicits._

    val colRow = rowScannedDF.select("zeroitIndex").as[Seq[Long]].collect()
    val validSortedCrossedIndices = colRow.sortWith {
      _.length < _.length
    }

    processIndices(validSortedCrossedIndices)
  }

  private def processIndices(
      validSortedCrossedIndices: Array[Seq[Long]]): Seq[Long] = {
    val validIndices = Set.empty[Long]
    for (indexSeq <- validSortedCrossedIndices) {
      if (indexSeq.length == 1 && !validIndices.contains(indexSeq(0))) {
        validIndices += indexSeq(0)
      } else if (indexSeq.length > 1) {
        val leftIndices = indexSeq.diff(validIndices.toSeq)
        if (leftIndices.length == 1) {
          validIndices += leftIndices(0)
        }
      }
    }

    validIndices.toSeq
  }

  def step4_columnLines(columnDF: DataFrame,
                        rowCrossedIndices: Seq[Long]): Seq[Long] = {

    val colScannedDF: DataFrame =
      getUncrossedDataframe(columnDF, rowCrossedIndices)

    val validIndices = ArrayBuffer.empty[Seq[Long]]
    for (eachCol <- colScannedDF.columns)
      validIndices += colScannedDF
        .select(eachCol)
        .collect()
        .map(each => each(0))
        .zipWithIndex
        .filter(_._1 == 0)
        .map(_._2.toLong)

    val validSortedCrossedIndices = validIndices.sortWith {
      _.length < _.length
    }
    processIndices(validSortedCrossedIndices.toArray)
  }

  def getUncrossedDataframe(columnDF: DataFrame,
                            rowCrossedIndices: Seq[Long]): DataFrame = {
    val colRange = columnDF.columns.length
    val columnRange = List.range(1, colRange + 1)

    val validColumns =
      columnRange.diff(rowCrossedIndices.map(_ + 1L)).map(tag => s"_$tag")
    if (validColumns.length > 0)
      columnDF.select(validColumns.head, validColumns.tail: _*)
    else
      spark.emptyDataFrame
  }

}
