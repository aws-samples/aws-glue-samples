package com.amazonaws.services.glue.marketplace.connector.tpcds


import com.teradata.tpcds.Table
import org.apache.spark.sql.nonExistingUDTTest
import org.apache.spark.sql.types.{DateType, DecimalType, IntegerType, LongType, StringType}
import org.scalatest.FunSuite

import collection.JavaConverters._

class TypeCastSuite extends FunSuite {
  test("Convert value types from Spark to Scala types - StringType") {
    val value = "This is a test"
    val dataType = StringType
    val valueType = TPCDSUtils.convertValueType(value, dataType)
    assert(valueType.isInstanceOf[java.lang.String])
  }

  test("Convert value types from Spark to Scala types - LongType") {
    val value = 819667L.toString
    val dataType = LongType
    val valueType = TPCDSUtils.convertValueType(value, dataType)
    assert(valueType.isInstanceOf[java.lang.Long])
  }

  test("Convert value types from Spark to Scala types - IntegerType") {
    val value = 1234.toString
    val dataType = IntegerType
    val valueType = TPCDSUtils.convertValueType(value, dataType)
    assert(valueType.isInstanceOf[java.lang.Integer])
  }

  test("Convert value types from Spark to Scala types - DecimalType") {
    val value = 12.34567.toString
    val dataType = new DecimalType()
    val valueType = TPCDSUtils.convertValueType(value, dataType)
    assert(valueType.isInstanceOf[java.math.BigDecimal])
  }

  test("Convert value types from Spark to Scala types - DateType") {
    val value = 1234.toString
    val dataType = IntegerType
    val valueType = TPCDSUtils.convertValueType(value, dataType)
    assert(valueType.isInstanceOf[java.lang.Integer])
  }

  test("Convert value types from Spark to Scala types - non-existing type throwing 'IllegalArgumentException'") {
    val errorMessage = "The specified type: nonexistingudttest is not defined."
    val value = 1234.toString

    val nonExistingType = new nonExistingUDTTest

    val iae = intercept[IllegalArgumentException] {
      TPCDSUtils.convertValueType(value, nonExistingType)
    }
    assert(iae.getMessage == errorMessage)
  }

  test("Convert column types from TPCDS to Spark types - IDENTIFIER to LongType") {
    val table = Table.getBaseTables.asScala.filter(t => t.toString == "CUSTOMER").head
    val column = table.getColumn("c_customer_sk")
    val dataType = TPCDSUtils.convertColumnType(column)
    assert(dataType == LongType)
  }

  test("Convert column types from TPCDS to Spark types - INTEGER to IntegerType") {
    val table = Table.getBaseTables.asScala.filter(t => t.toString == "CUSTOMER").head
    val column = table.getColumn("c_birth_day")
    val dataType = TPCDSUtils.convertColumnType(column)
    assert(dataType == IntegerType)
  }

  test("Convert column types from TPCDS to Spark types - DATE to DateType") {
    val table = Table.getBaseTables.asScala.filter(t => t.toString == "CALL_CENTER").head
    val column = table.getColumn("cc_rec_start_date")
    val dataType = TPCDSUtils.convertColumnType(column)
    assert(dataType == DateType)
  }

  test("Convert column types from TPCDS to Spark types - DECIMAL to DecimalType") {
    val table = Table.getBaseTables.asScala.filter(t => t.toString == "CALL_CENTER").head
    val column = table.getColumn("cc_gmt_offset")
    val dataType = TPCDSUtils.convertColumnType(column)
    assert(dataType == new DecimalType(5, 2))
  }

  test("Convert column types from TPCDS to Spark types - CHAR to StringType") {
    val table = Table.getBaseTables.asScala.filter(t => t.toString == "CALL_CENTER").head
    val column = table.getColumn("cc_call_center_id")
    val dataType = TPCDSUtils.convertColumnType(column)
    assert(dataType == StringType)
  }

  test("Convert column types from TPCDS to Spark types - VARCHAR to StringType") {
    val table = Table.getBaseTables.asScala.filter(t => t.toString == "CALL_CENTER").head
    val column = table.getColumn("cc_name")
    val dataType = TPCDSUtils.convertColumnType(column)
    assert(dataType == StringType)
  }

  test("Convert column types from TPCDS to Spark types - TIME to StringType") {
    val table = Table.getBaseTables.asScala.filter(t => t.toString == "DBGEN_VERSION").head
    val column = table.getColumn("dv_create_time")
    val dataType = TPCDSUtils.convertColumnType(column)
    assert(dataType == StringType)
  }
}
