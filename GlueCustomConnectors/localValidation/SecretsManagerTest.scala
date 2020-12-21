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

class SecretsManagerTest extends FunSuite with BeforeAndAfterEach {
  val conf = new SparkConf().setAppName("SimpleTest").setMaster("local")
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

  test("Test secrets manager integration") {
    // remove verbose log if needed
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // set up secret Id
    val secretId = "secret"

    // set up connection options
    val optionsMap = Map(
      "query" -> "SELECT NumberOfEmployees, CreatedDate FROM Account",
      "className" -> "partner.jdbc.salesforce.SalesforceDriver",
      // dollar signed placeholders will be replaced by the values of corresponding keys from secret
      "url" -> "jdbc:salesforce:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
      "secretId"-> secretId
    )

    // create DataSource and read data
    val customSource = glueContext.getSource(
      connectionType = "custom.jdbc", // spark: "custom.spark"
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