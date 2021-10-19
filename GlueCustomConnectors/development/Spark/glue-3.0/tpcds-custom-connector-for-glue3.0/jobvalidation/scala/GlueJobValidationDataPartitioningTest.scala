/*
 * Copyright 2016-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.schema.{Schema, TypeCode}
import com.amazonaws.services.glue.schema.builders.SchemaBuilder
import com.amazonaws.services.glue.schema.types.DecimalType
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

object GlueJobValidationDataPartitioningTest {
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark, 1, 20) // Change these values for preventing from repartition by GlueContext default config.

    val connectionName = "GlueTPCDSConnection"
    val connectionType = "marketplace.spark"

    // 1. Partition count - generated single chunk data and row count is more than numPartitions
    val options_1 = Map(
      "table" -> "customer",
      "scale" -> "1",
      "numPartitions" -> "5",
      "connectionName" -> connectionName
    )

    val dyf_1 = glueContext.getSource(
      connectionType = connectionType,
      connectionOptions = JsonOptions(options_1),
      transformationContext = "dyf"
    ).getDynamicFrame()

    assert(dyf_1.getNumPartitions == 5)
    assert(dyf_1.count == 100000)


    // 2. Partition count - generated single chunk data and row count is less than numPartitions
    val options_2 = Map(
      "table" -> "call_center",
      "scale" -> "1",
      "numPartitions" -> "100",
      "connectionName" -> connectionName
    )

    val dyf_2 = glueContext.getSource(
      connectionType = connectionType,
      connectionOptions = JsonOptions(options_2),
      transformationContext = "dyf"
    ).getDynamicFrame()

    assert(dyf_2.getNumPartitions == 1)
    assert(dyf_2.count == 6)


    // 3. Partition count - generated multiple chunk data - in parallel
    val options_3 = Map(
      "table" -> "customer",
      "scale" -> "100",
      "numPartitions" -> "100",
      "connectionName" -> connectionName
    )

    val dyf_3 = glueContext.getSource(
      connectionType = connectionType,
      connectionOptions = JsonOptions(options_3),
      transformationContext = "dyf"
    ).getDynamicFrame()

    assert(dyf_3.getNumPartitions == 100)
    assert(dyf_3.count == 2000000)
  }
}