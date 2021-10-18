package com.amazonaws.services.glue.marketplace.connector.tpcds

import org.apache.spark.sql.connector.expressions.Transform

import org.scalatest.FunSuite

import java.util
import collection.JavaConverters._


class TPCDSSourceSuite extends FunSuite {
  test("'table' parameter validation") {
    val errorMessage = "requirement failed: You need to specify 'table' parameter."
    val options = Map(
      "scale" -> "1",
      "numPartitions" -> "1"
    )
    val javaProps: util.Map[String, String] = options.asJava
    val iae = intercept[IllegalArgumentException] {
      new TPCDSSource().getTable(null, Array.empty[Transform], javaProps)
    }
    assert(iae.getMessage == errorMessage)
  }
}
