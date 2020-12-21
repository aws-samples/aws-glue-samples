/*
 * Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
package test.scala

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.schema.builders.SchemaBuilder
import com.amazonaws.services.glue.schema.{Schema, TypeCode}
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class CatalogConnectionTest extends FunSuite with BeforeAndAfterEach {
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

  /*
  please set up the catalog connection for testing.
  A valid custom connection for this test should contain the following connection fields:
    1. ConnectionType:  CUSTOM
    2. CONNECTOR_TYPE: Jdbc | Spark | Athena
    3. JDBC_CONNECTION_URL (for JDBC only): e.g. "jdbc:salesforce:user=${user};Password=${Password};SecurityToken=${SecurityToken};"
    4. CONNECTOR_CLASS_NAME: e.g. "partner.jdbc.salesforce.SalesforceDriver",
    5. USERNAME and PASSWORD (for JDBC only) or SECRET_ID,
    6. CONNECTOR_URL: e.g. s3://path/to/my/connector.jar.
  */
  test("test catalog connection integration") {
    val database = "connector"
    val tableName = "testsalesforce"

    // set up additional connection options
    val optionsMap = Map(
      "query" -> "SELECT NumberOfEmployees, CreatedDate FROM Account WHERE",

      "partitionColumn" -> "RecordId__c",
      "lowerBound" -> "0",
      "upperBound" -> "13",
      "numPartitions" -> "2",

      "connectionName" -> "connection"
    )

    // create data source and read data
    val customSource = glueContext.getSource(
      connectionType = "custom.jdbc",
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "customSource")
    val dyf = customSource.getDynamicFrame()

    // verify data
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