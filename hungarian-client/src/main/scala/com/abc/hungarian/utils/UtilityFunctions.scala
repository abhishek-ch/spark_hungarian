package com.abc.hungarian.utils

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{lit, udf}

object UtilityFunctions {

  def findZeroUdf: UserDefinedFunction =
    udf((row: Row, whitelistCols: Seq[Long]) => {
      row.toSeq
        .map(_.toString.toLong)
        .zipWithIndex
        .filter(_._1 == 0)
        .map(_._2)
        .diff(whitelistCols)
    })

  def listOfListsCombinations[A](source: List[List[A]]): List[List[A]] =
    source match {
      case Nil => List(Nil)
      case ls :: sublist =>
        for (each <- ls; combs <- listOfListsCombinations(sublist))
          yield each :: combs
    }

  def enhanceWithContainsDuplicates[T](s: List[T]) = new {
    def containsDuplicates = (s.distinct.size != s.size)
  }

}
