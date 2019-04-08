/*
 * Copyright 2016-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

import com.amazonaws.services.glue.util.JsonOptions
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import org.apache.spark.SparkContext

object ResolveChoice {
  def main(sysArgs: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sc)
    val spark = glueContext.getSparkSession

    // catalog: database and table name
    val dbName = "medicare"
    val tblName = "medicare"

    // s3 output directories
    val baseOutputDir = "s3://glue-sample-target/output-dir"
    val medicareCast = s"$baseOutputDir/medicare_json_cast"
    val medicareProject = s"$baseOutputDir/medicare_json_project"
    val medicareCols = s"$baseOutputDir/medicare_json_make_cols"
    val medicareStruct = s"$baseOutputDir/medicare_json_make_struct"
    val medicareSql = s"$baseOutputDir/medicare_json_sql"

    // Read data into a dynamic frame

    val medicareDyf = glueContext.getCatalogSource(database = dbName, tableName = tblName).getDynamicFrame()

    // The `provider id` field will be choice between long and string

    // Cast choices into integers, those values that cannot cast result in null
    val medicareResCast = medicareDyf.resolveChoice(specs = Seq(("provider id","cast:long")))
    val medicareResProject = medicareDyf.resolveChoice(specs = Seq(("provider id","project:long")))
    val medicareResMakeCols = medicareDyf.resolveChoice(specs = Seq(("provider id","make_cols")))
    val medicareResMakeStruct = medicareDyf.resolveChoice(specs = Seq(("provider id","make_struct")))

    // Spark SQL on a Spark dataframe
    val medicareDf = medicareDyf.toDF()
    medicareDf.createOrReplaceTempView("medicareTable")
    val medicareSqlDf = spark.sql("SELECT * FROM medicareTable WHERE `total discharges` > 30")
    val medicareSqlDyf = DynamicFrame(medicareSqlDf, glueContext).withName("medicare_sql_dyf")

    // Write it out in Json
    glueContext.getSinkWithFormat(connectionType = "s3", options = JsonOptions(Map("path" -> medicareCast)), format = "json").writeDynamicFrame(medicareResCast)
    glueContext.getSinkWithFormat(connectionType = "s3", options = JsonOptions(Map("path" -> medicareProject)), format = "json").writeDynamicFrame(medicareResProject)
    glueContext.getSinkWithFormat(connectionType = "s3", options = JsonOptions(Map("path" -> medicareCols)), format = "json").writeDynamicFrame(medicareResMakeCols)
    glueContext.getSinkWithFormat(connectionType = "s3", options = JsonOptions(Map("path" -> medicareStruct)), format = "json").writeDynamicFrame(medicareResMakeStruct)
    glueContext.getSinkWithFormat(connectionType = "s3", options = JsonOptions(Map("path" -> medicareSql)), format = "json").writeDynamicFrame(medicareSqlDyf)
  }
}
