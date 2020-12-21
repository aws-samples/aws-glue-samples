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

class JDBCUrlTest extends FunSuite with BeforeAndAfterEach {
  val conf = new SparkConf().setAppName("LocalTest").setMaster("local")
  var spark: SparkContext = _
  var glueContext: GlueContext = _


  override def beforeEach(): Unit = {
    spark = new SparkContext(conf)
    glueContext = new GlueContext(spark)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    spark.stop()
  }


  test("test JDBC extra jdbc url parameters") {
    val rootLogger = Logger.getRootLogger() // Remove verbose log if needed
    rootLogger.setLevel(Level.ERROR)

    // set up connection options
    val SecurityToken = "securitytoken" // for Salesforce remote access
    val user = "user" // for Salesforce remote access
    val password = "password" // for Salesforce remote access
    val extraJDBCParameters = "UseBulkAPI=true"
    val url = s"jdbc:salesforce:SecurityToken=${SecurityToken};" + extraJDBCParameters
    val optionsMap = Map(
      "query" -> "SELECT NumberOfEmployees, CreatedDate FROM Account",
      "className" -> "partner.jdbc.salesforce.SalesforceDriver",

      "url" -> url,
      "user" -> user,
      "password" -> password
    )

    // 2. Create DataSource
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
