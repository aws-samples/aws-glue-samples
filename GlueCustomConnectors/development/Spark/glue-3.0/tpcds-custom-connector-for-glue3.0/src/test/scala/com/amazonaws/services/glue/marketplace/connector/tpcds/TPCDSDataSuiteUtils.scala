package com.amazonaws.services.glue.marketplace.connector.tpcds

import com.teradata.tpcds
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object TPCDSDataSuiteUtils {
  // TPC-DS 'customer' table
  val customerTable: tpcds.Table = TPCDSUtils.extractTable("customer")

  val customerTableSchema: StructType = {
    val columns = new ArrayBuffer[StructField]()
    for(c <- customerTable.getColumns) columns += StructField(c.getName, TPCDSUtils.convertColumnType(c))
    new StructType(columns.toArray)
  }

  val customerManualSchema: StructType = {
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

  // TPC-DS 'dbgen_version' table
  val dbgenVersionTable: tpcds.Table = TPCDSUtils.extractTable("dbgen_version")

  val dbgenVersionTableSchema: StructType = {
    val columns = new ArrayBuffer[StructField]()
    for(c <- dbgenVersionTable.getColumns) columns += StructField(c.getName, TPCDSUtils.convertColumnType(c))
    new StructType(columns.toArray)
  }

}
