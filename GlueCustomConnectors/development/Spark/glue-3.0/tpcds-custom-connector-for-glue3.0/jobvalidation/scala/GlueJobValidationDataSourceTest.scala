/*
 * Copyright 2016-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._

object GlueJobValidationDataSourceTest {
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)

    val optionsMap = Map(
      "table" -> "customer",
      "scale" -> "1",
      "numPartitions" -> "1",
      "connectionName" -> "GlueTPCDSConnection"
    )

    // create DataSource
    val customSource = glueContext.getSource(
      connectionType = "marketplace.spark",
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "customSource")
    val dyf = customSource.getDynamicFrame()

    // verify data
    val expectedRowCount = 100000
    assert(dyf.count == expectedRowCount)
  }
}