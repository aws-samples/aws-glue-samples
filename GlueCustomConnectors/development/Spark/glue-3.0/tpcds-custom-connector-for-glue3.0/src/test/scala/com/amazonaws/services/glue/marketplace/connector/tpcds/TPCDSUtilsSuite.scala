package com.amazonaws.services.glue.marketplace.connector.tpcds

import org.scalatest.FunSuite
import com.teradata.tpcds.Table
import org.apache.spark.sql.types._

import collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


class TPCDSUtilsSuite extends FunSuite {
  val customerSchema: StructType = {
    val columns = new ArrayBuffer[StructField]()
    columns += StructField("c_customer_sk", LongType)
    columns += StructField("c_customer_id", StringType)
    columns += StructField("c_current_cdemo_sk", LongType)
    columns += StructField("c_current_hdemo_sk", LongType)
    columns += StructField("c_current_addr_sk", LongType)
    columns += StructField("c_first_shipto_date_sk", LongType)
    columns += StructField("c_first_sales_date_sk", LongType)
    columns += StructField("c_salutation", StringType)
    columns += StructField("c_first_name", StringType)
    columns += StructField("c_last_name", StringType)
    columns += StructField("c_preferred_cust_flag", StringType)
    columns += StructField("c_birth_day", IntegerType)
    columns += StructField("c_birth_month", IntegerType)
    columns += StructField("c_birth_year", IntegerType)
    columns += StructField("c_birth_country", StringType)
    columns += StructField("c_login", StringType)
    columns += StructField("c_email_address", StringType)
    columns += StructField("c_last_review_date_sk", LongType)

    new StructType(columns.toArray)
  }

  test("Test extracting output table name") {
    val tableName = "customer"
    assert(TPCDSUtils.extractTable(tableName).toString == "CUSTOMER")
  }

  test("Test extracting non exist table name") {
    val tableName = "non exist table"
    val errorMessage = "next on empty iterator"
    val nse = intercept[NoSuchElementException] {
      TPCDSUtils.extractTable(tableName)
    }
    assert(nse.getMessage == errorMessage)
  }

  test("Check the number of rows in the whole of generated data") {
    val data = TPCDSUtils.generateChunkIterator(
      Table.getBaseTables.asScala.filter(t => t.toString == "CUSTOMER").head,
      1,
      1,
      1
    )
    assert(data.length == 100000)
  }

  test("Check the number of rows in chunked data") {
    val chunk = TPCDSUtils.generateChunkIterator(
      Table.getBaseTables.asScala.filter(t => t.toString == "CUSTOMER").head,
      1000,
      10000,
      1
    )
    assert(chunk.length == 1200)
  }
}
