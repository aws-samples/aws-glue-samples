/*
 * Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
object Salesforcejdbc {
  def main(sysArgs: Array[String]) {
    val conf = new SparkConf().setAppName("JDBCSalesforce").setMaster("local")
    val spark: SparkContext = new SparkContext(conf)
    val glueContext: GlueContext = new GlueContext(spark)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    // Please update the values in the optionsMap to connect to your own data source
    val optionsMap = Map(
      "dbTable" -> "Account",
      "partitionColumn" -> "RecordId__c",
      "lowerBound" -> "0",
      "upperBound" -> "123",
      "numPartitions" -> "6",
      "url" -> "jdbc:partner:salesforce:user=${user};Password=${Password};SecurityToken=${SecurityToken}",
      "secretId" -> "my-secret", // the secret stores values of user, Password and SecurityToken
      "dataSource" -> "salesforce",
      "className" -> "partner.jdbc.salesforce.SalesforceDriver"
    )
    val customSource = glueContext.getSource(
      connectionType = "custom.jdbc", // for marketplace workflow, use marketplace.jdbc
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "")
    val dyf = customSource.getDynamicFrame()
    dyf.printSchema()
    dyf.show()
  }
}
