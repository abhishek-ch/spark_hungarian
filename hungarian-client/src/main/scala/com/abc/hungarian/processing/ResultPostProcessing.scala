package com.abc.hungarian.processing

import com.abc.hungarian.utils.{SparkSessionImplicits, UtilityFunctions}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object ResultPostProcessing extends SparkSessionImplicits {

  def extractValidOptionsFromDataframe(resultDF: DataFrame): DataFrame = {
    if (!resultDF.rdd.isEmpty())
      resultDF
        .withColumn(
          "zeroCol",
          UtilityFunctions.findZeroUdf(struct(resultDF.columns.map(col): _*),
                                       typedLit(Seq[Long]())))
        .select("zeroCol")
    else
      resultDF

  }

  implicit def enhanceWithContainsDuplicates[T](s: List[T]) = new {
    def containsDuplicates = (s.distinct.size != s.size)
  }

  def extractIndicesFromDataframe(finalDF: DataFrame): Seq[List[Long]] = {
    import spark.implicits._

    val resultBuffer = ListBuffer.empty[List[Long]]

    val resultList = finalDF
      .select("zeroCol")
      .as[List[Long]]
      .collect()
      .toList

    println(s"The Optimized position of each entry ${resultList.mkString(",")}")
    val resultCombinations =
      UtilityFunctions.listOfListsCombinations(resultList)

    resultCombinations.foreach(result => {
      if (!result.containsDuplicates) {
        val indexedRes = result.map(_ + 1)
        println(s"Valid Combination is ${result.map(_ + 1).mkString(", ")}")
        resultBuffer += indexedRes
      }
    })

    resultBuffer.toList

  }

}
