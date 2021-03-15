/*
 * Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MinimalSparkConnectorTest {
  def main(sysArgs: Array[String]) {

    val conf = new SparkConf().setAppName("SimpleTest").setMaster("local")
    val spark: SparkContext = new SparkContext(conf)
    val glueContext: GlueContext = new GlueContext(spark)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val optionsMap = Map(
      "connectionName" -> "test-with-connection"
    )
    val datasource = glueContext.getSource(
      connectionType = "custom.spark",
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "datasource")
    val dyf = datasource.getDynamicFrame()
    dyf.show()
    dyf.printSchema()

    val datasink = glueContext.getSink(
      connectionType = "custom.spark",
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "datasink")
    datasink.writeDynamicFrame(dyf)
  }
}