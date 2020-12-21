/*
 * Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
package test.scala

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.schema.{Schema, TypeCode}
import com.amazonaws.services.glue.schema.builders.SchemaBuilder
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class ColumnPartitioningTest extends FunSuite with BeforeAndAfterEach {
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


  test("test partner jdbc partition column") {
    // set partitionColumn Information
    val partitionColumn = "RecordId__c"
    val lowerBound = "0"
    val upperBound = "13"
    val numPartitions = "2"

    // set up connection options
    val optionsMap = Map(
      "query" -> "SELECT NumberOfEmployees, CreatedDate FROM Account WHERE",
      "className" -> "partner.jdbc.salesforce.SalesforceDriver",
      "url" -> "jdbc:salesforce:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
      "secretId"-> "secret",

      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound,
      "upperBound" -> upperBound,
      "numPartitions" -> numPartitions
    )

    // create DataSource and read data
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