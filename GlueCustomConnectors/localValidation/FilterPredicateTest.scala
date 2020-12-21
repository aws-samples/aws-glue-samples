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

class FilterPredicateTest extends FunSuite with BeforeAndAfterEach {
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


  test("Test partner jdbc filter predicate") {
    // 1. Set filterPredicate
    val filterPredicate = "BillingCity='Mountain View'"

    // 2. Set up connection options
    val optionsMap = Map(
      "dbTable" -> "Account",
      "className" -> "partner.jdbc.salesforce.SalesforceDriver",
      "url" -> "jdbc:salesforce:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
      "secretId"-> "secret",

      "filterPredicate" -> filterPredicate
    )

    // 3. Create DataSource and read data
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

    val expectedRowCount = 1
    assert(dyf.count === expectedRowCount)
  }
}