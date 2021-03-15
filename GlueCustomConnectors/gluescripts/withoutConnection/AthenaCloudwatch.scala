/*
 * Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
object AthenaCloudwatch {
  def main(sysArgs: Array[String]) {
    val conf = new SparkConf().setAppName("AthenaCloudwatch").setMaster("local")
    val spark: SparkContext = new SparkContext(conf)
    val glueContext: GlueContext = new GlueContext(spark)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    // Please update the values in the optionsMap to connect to your own data source
    val optionsMap = Map(
      "tableName" -> "all_log_streams",
      "schemaName" -> "/aws-glue/jobs/output",
      "className" -> "com.amazonaws.athena.connectors.Cloudwatch"
    )
    val customSource = glueContext.getSource(
      connectionType = "custom.athena", // for marketplace workflow, use marketplace.athena
      connectionOptions = JsonOptions(optionsMap)
    )
    val dyf = customSource.getDynamicFrame()
    dyf.printSchema()
    dyf.show()
  }
}
