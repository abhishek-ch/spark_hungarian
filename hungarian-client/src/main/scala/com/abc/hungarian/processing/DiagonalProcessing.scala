package com.abc.hungarian.processing

import com.abc.hungarian.scanner.DFScanner
import com.abc.hungarian.utils.SparkSessionImplicits
import org.apache.spark.sql.{DataFrame, Row}
import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object DiagonalProcessing extends SparkSessionImplicits {

  def findNumberOfZeros(dataFrame: DataFrame,
                        rowSeq: Seq[Long],
                        colSeq: Seq[Long]): List[Long] = {

    //    val onlyZerosAccum = spark.sparkContext.longAccumulator("numzeros")
    val onlyZerosCollAccum =
      spark.sparkContext.collectionAccumulator[Long]("zerosaccum")

    // Why Count in following transformation ???
    // Read about accumulator truth https://stackoverflow.com/questions/29494452/when-are-accumulators-truly-reliable
    dataFrame.rdd.zipWithIndex
      .map {
        case (row, rowIndex) => {
          val columnValues = row.toSeq.map(_.toString.toLong)
          // iterate through columns and check for intersection points
          Row.fromSeq(columnValues.zipWithIndex.map {
            case (each, colIndex) => {
              // 2. find no intersection value and reduce globalMinima from it
              if (!colSeq.contains(rowIndex)
                  && !rowSeq.contains(colIndex)
                  && each == 0) {
                //                onlyZerosAccum.add(1)
                onlyZerosCollAccum.add(colIndex)
              } else
                each
            }
          })
        }
      }
      .count()

    println(
      "Total Number of Zeroes Found " + onlyZerosCollAccum.value.toList
        .mkString(","))

    onlyZerosCollAccum.value.toList

  }

  /**
    * https://www.youtube.com/watch?v=WmPX0XbKaCc
    *
    * @param dataFrame
    * @param rowSeq
    * @param colSeq
    * @return
    */
  def stepX_diagonalTriggerLines(
      dataFrame: DataFrame,
      rowSeq: Seq[Long],
      colSeq: Seq[Long]): (DataFrame, ListBuffer[Long], ListBuffer[Long]) = {
    //val cleanedDF = DFScanner.getUncrossedDataframe(dataFrame, rowSeq)

    //    Main list to hold all the indices during diagonal rule processing
    val mainRowIndicesList = ListBuffer[Long]()
    mainRowIndicesList ++= rowSeq

    val mainColIndicesList = ListBuffer[Long]()
    mainColIndicesList ++= colSeq

    dataFrame.cache()
    val rowsCount = dataFrame.count()

    var zerosList = findNumberOfZeros(dataFrame, rowSeq, colSeq).distinct
    while (zerosList.length > 0 && (mainColIndicesList.length + mainRowIndicesList.length < rowsCount)) {
      val randomColumnSelection = Random.shuffle(zerosList).head
      println(
        s"Random Row Selected Column to be crossed as $randomColumnSelection")
      mainRowIndicesList += randomColumnSelection

      mainRowIndicesList ++= DFScanner.step3_rowLines(dataFrame,
                                                      mainRowIndicesList)
      mainColIndicesList ++= DFScanner.step4_columnLines(dataFrame,
                                                         mainRowIndicesList)

      zerosList = findNumberOfZeros(dataFrame,
                                    mainRowIndicesList,
                                    mainColIndicesList).distinct

    }

    println(s"Diagonal Rule optimization with row ${mainRowIndicesList.mkString(
      ", ")} and columns ${mainColIndicesList.mkString(", ")}")

    (dataFrame, mainRowIndicesList, mainColIndicesList)
  }

}
