/*
 * Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
package test.scala

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.schema.{Schema, TypeCode}
import com.amazonaws.services.glue.schema.builders.SchemaBuilder
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class DbtableQueryTest extends FunSuite with BeforeAndAfterEach {
  val conf = new SparkConf().setAppName("LocalTest").setMaster("local")
  var spark: SparkContext = _
  var glueContext: GlueContext = _

  val rootLogger = Logger.getRootLogger() // Remove verbose log
  rootLogger.setLevel(Level.ERROR)

  override def beforeEach(): Unit = {
    spark = new SparkContext(conf)
    glueContext = new GlueContext(spark)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    spark.stop()
  }


  test("test basic dbTable/query") {
    // set up common connection options
    val optionsMap = Map(
      "className" -> "partner.jdbc.salesforce.SalesforceDriver",
      "url" -> "jdbc:salesforce:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
      "secretId"-> "secret"
    )

    // read data with query option
    val optionMapWithQuery = optionsMap + ("query" -> "SELECT * FROM Account")
    val customSource0 = glueContext.getSource(
      connectionType = "custom.jdbc",
      connectionOptions = JsonOptions(optionMapWithQuery),
      transformationContext = "customSource0")
    val dyf0 = customSource0.getDynamicFrame()

    // read data with dbTable option
    val optionMapWithDbTable = optionsMap + ("dbTable" -> "Account")
    val customSource1 = glueContext.getSource(
      connectionType = "custom.jdbc",
      connectionOptions = JsonOptions(optionMapWithQuery),
      transformationContext = "customSource1")
    val dyf1 = customSource1.getDynamicFrame()

    // verify data
    assert(dyf0.schema == dyf1.schema)
    assert(dyf0.count === dyf1.count)
  }

  test("test query - select columns") {
    // set up connection options with query
    val optionsMap = Map(
      "query" -> "SELECT NumberOfEmployees, CreatedDate FROM Account",

      "className" -> "partner.jdbc.salesforce.SalesforceDriver",
      "url" -> "jdbc:salesforce:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
      "secretId"-> "secret"
    )

    // create DataSource and read data
    val customSource = glueContext.getSource(
      connectionType = "custom.jdbc",
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "customSource")
    val dyf = customSource.getDynamicFrame()

    // verify data schema
    var expectedSchema = new Schema(new SchemaBuilder()
      .beginStruct()
      .atomicField("NumberOfEmployees", TypeCode.INT)
      .atomicField("CreatedDate", TypeCode.TIMESTAMP)
      .endStruct().build())

    assert(dyf.schema === expectedSchema)
    val expectedRowCount = 13
    assert(dyf.count === expectedRowCount)
  }

  test("test query - filterPredicate provided") {
    // set up connection options with filterPredicate
    val optionsMap = Map(
      "query" -> "SELECT NumberOfEmployees, CreatedDate FROM Account WHERE",

      "className" -> "partner.jdbc.salesforce.SalesforceDriver",
      "url" -> "jdbc:salesforce:user=${user};Password=${Password};SecurityToken=${SecurityToken};RTK=${RTK}",
      "secretId"-> "secret",

      "filterPredicate" -> "BillingCity='Mountain View'"
    )

    // create DataSource and read data
    val customSource = glueContext.getSource(
      connectionType = "custom.jdbc",
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "customSource")
    val dyf = customSource.getDynamicFrame()

    // verify data schema
    var expectedSchema = new Schema(new SchemaBuilder()
      .beginStruct()
      .atomicField("NumberOfEmployees", TypeCode.INT)
      .atomicField("CreatedDate", TypeCode.TIMESTAMP)
      .endStruct().build())

    assert(dyf.schema === expectedSchema)
    val expectedRowCount = 1
    assert(dyf.count === expectedRowCount)
  }

  test("test query - partitionColumn provided") {
    // set up connection options with partitionColumn
    val optionsMap = Map(
      "query" -> "SELECT NumberOfEmployees, CreatedDate FROM Account WHERE",

      "className" -> "partner.jdbc.salesforce.SalesforceDriver",
      "url" -> "jdbc:salesforce:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
      "secretId"-> "secret",

      "partitionColumn" -> "RecordId__c",
      "lowerBound" -> "0",
      "upperBound" -> "13",
      "numPartitions" -> "2"
    )

    // create DataSource and read data
    val customSource = glueContext.getSource(
      connectionType = "custom.jdbc",
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "customSource")
    val dyf = customSource.getDynamicFrame()

    // verify data schema
    var expectedSchema = new Schema(new SchemaBuilder()
      .beginStruct()
      .atomicField("NumberOfEmployees", TypeCode.INT)
      .atomicField("CreatedDate", TypeCode.TIMESTAMP)
      .endStruct().build())

    assert(dyf.schema === expectedSchema)
    val expectedRowCount = 13
    assert(dyf.count === expectedRowCount)
  }
}