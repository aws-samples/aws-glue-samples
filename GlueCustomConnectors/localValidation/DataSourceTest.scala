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

class DataSourceTest extends FunSuite with BeforeAndAfterEach {
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


  test("test data source connection") {
    // remove verbose log if needed
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // set up connection options
    val SecurityToken = "securitytoken" // for Salesforce remote access
    val user = "user"
    val password = "password"
    val optionsMap = Map(
      "dbTable" -> "Account",
      "className" -> "partner.jdbc.salesforce.SalesforceDriver",
      "url" -> s"jdbc:salesforce:SecurityToken=${SecurityToken};", //dollar sign
      "user" -> user,
      "password" -> password
    )

    // create DataSource
    val customSource = glueContext.getSource(
      connectionType = "custom.jdbc", // spark: "custom.spark", athena: "custom.athena"
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "customSource")
    val dyf = customSource.getDynamicFrame()

    // verify data
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
