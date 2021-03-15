/*
 * Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
object SparkSnowflake {
  def main(sysArgs: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSnowflake").setMaster("local")
    val spark: SparkContext = new SparkContext(conf)
    val glueContext: GlueContext = new GlueContext(spark)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    // Please update the values in the optionsMap to connect to your own data source
    val optionsMap = Map(
      "sfDatabase" -> "snowflake_sample_data",
      "sfSchema" -> "PUBLIC",
      "sfWarehouse" -> "WORKSHOP_123",
      "dbtable" -> "lineitem",
      "connectionName" -> "my-connection" // please refer to Glue Studio Create Custom Connector doc to create a connection
    )
    val  customSource = glueContext.getSource(
      connectionType = "custom.spark", // for marketplace workflow, use marketplace.spark
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "")
    val dyf = customSource.getDynamicFrame()
    dyf.printSchema()
    dyf.show()

    val customSink = glueContext.getSink(
      connectionType = "custom.spark",
      connectionOptions = JsonOptions(optionsMap))
    customSink.writeDynamicFrame(dyf)
  }
}
