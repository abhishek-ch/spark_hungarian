package com.abc.hungarian.utils

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec

abstract class AbstractDatasetSuitSpec extends FunSpec
  with DataFrameSuiteBase{

  override implicit def reuseContextIfPossible: Boolean = true

}
