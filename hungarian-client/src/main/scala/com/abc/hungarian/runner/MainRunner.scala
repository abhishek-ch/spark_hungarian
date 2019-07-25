package com.abc.hungarian.runner

import com.abc.hungarian.processing.{DiagonalProcessing, ResultPostProcessing}
import com.abc.hungarian.scanner.{DFScanner, DataOptimization, RowColReduction}
import com.abc.hungarian.utils.SparkSessionImplicits
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.LongType

object MainRunner extends SparkSessionImplicits {

  def run(inputDF: DataFrame, cycle: Int = 3): Seq[List[Long]] = {
    println(
      s"To find an optimized cycle, the hungarian flow will retry $cycle times")

    val currSchema = inputDF.schema
    currSchema.foreach(field => {
      assert(field.dataType == LongType, "Each Field must be Long Type")
    })

    val finalDF = runCycle(inputDF, cycle, 1)
    if (finalDF.rdd.isEmpty())
      println(s"Unable to find optimized solution withing $cycle Cycles")
    else {
      println(s"Hurray !!! Hungarian found the optimized Dataframe")
      finalDF.show()
    }

    ResultPostProcessing.extractIndicesFromDataframe(
      ResultPostProcessing.extractValidOptionsFromDataframe(finalDF))
  }

  private def runCycle(inputDF: DataFrame,
                       maxCycle: Int,
                       currentCycle: Int): DataFrame = {
    if (currentCycle > maxCycle)
      return spark.emptyDataFrame
    println(s"Running cycle $currentCycle of maximum cycle $maxCycle")
    //    inputDF.show()
    inputDF.cache()
    val rowsCount = inputDF.count()

    val rowDF = RowColReduction.step1_RowReduction(inputDF)
    val colDF = RowColReduction.step2_ColReduction(rowDF)

    val rowSeq = DFScanner.step3_rowLines(colDF)
    val colSeq = DFScanner.step4_columnLines(colDF, rowSeq)
    println(
      s"""Total number of row counts $rowsCount and Crossed Count ${rowSeq.length + colSeq.length}
         |Row crossed ${rowSeq.mkString(", ")}
         |Column Crossed ${colSeq.mkString(", ")}
       """.stripMargin)

    if (rowSeq.length + colSeq.length >= rowsCount) {
      println("Optimized Dataframe found ...")
      return colDF
    } else {
      if (DiagonalProcessing
            .findNumberOfZeros(colDF, rowSeq, colSeq)
            .length > 1) {

        colDF.show()
        println(s"Trying to process Diagonal Rule now !!!")
        return DiagonalProcessing
          .stepX_diagonalTriggerLines(colDF, rowSeq, colSeq)
          ._1
      }

      val resultDF = DataOptimization.step5_addAndDeleteGlobalMinFromDataFrame(
        colDF,
        rowSeq,
        colSeq)
      println(s"Processing Normal Cycle ${currentCycle + 1}")
      runCycle(resultDF, maxCycle, currentCycle + 1)
      //  if resultDF has more than 1 unmarked zeroes, then we need to do diagonal processing

    }

  }
}
