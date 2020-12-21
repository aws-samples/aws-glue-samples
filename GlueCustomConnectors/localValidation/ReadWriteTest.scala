/*
 * Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
package test.scala

import java.util.UUID

import com.amazonaws.services.glue.{DynamicFrame, GlueContext, MappingSpec}
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class ReadWriteTest extends FunSuite with BeforeAndAfterEach {
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


  test("test read and write") {
    // remove verbose log if needed
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // set up connection options
    val optionsMap = Map(
      "query" -> "SELECT NumberOfEmployees, CreatedDate FROM Account",
      "className" -> "partner.jdbc.salesforce.SalesforceDriver",
      "url" -> "jdbc:salesforce:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
      "secretId"-> "secret"
    )

    // create DataSource
    val customSource = glueContext.getSource(
      connectionType = "custom.jdbc", // spark: "custom.spark", athena: "custom.athena"
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "customSource")

    // transform both columns to string type
    val mappings = Seq(
      MappingSpec("NumberOfEmployees", "int", "NumberOfEmployees", "string"),
      MappingSpec("CreatedDate", "timestamp", "CreatedDate", "string")
    )
    val dyf = customSource.getDynamicFrame().applyMapping(mappings)

    // write to S3
    val writePath = "s3://output/readwritetest/" + UUID.randomUUID()
    glueContext.getSinkWithFormat("s3", new JsonOptions(s"""{"path": "$writePath"}"""),
      format = "csv", formatOptions = new JsonOptions("""{"writeHeader": true}""")).writeDynamicFrame(dyf)
    val formatOptions = JsonOptions(Map("separator" -> ",", "withHeader" -> true))
    val dyf1: DynamicFrame = glueContext
      .getSource("s3", JsonOptions(Map("paths" -> List(writePath))))
      .withFormat("csv", formatOptions)
      .getDynamicFrame()

    // verify data
    assert(dyf1.schema === dyf.schema)
    assert(dyf1.count === dyf.count)

  }
}

