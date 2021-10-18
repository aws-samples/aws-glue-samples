package com.amazonaws.services.glue.marketplace.connector.tpcds

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SharedSparkContext
import org.scalatest.FunSuite

class TPCDSSourceIntegrationSuite extends FunSuite with SharedSparkContext {
  val className = "tpcds"

  test("Partition count - generated single chunk data and row count is more than numPartitions") {
    val glueContext: GlueContext = new GlueContext(sparkContext, 1, 20)
    val options = Map(
      "table" -> "customer",
      "scale" -> "1",
      "numPartitions" -> "5",
      "className" -> className
    )

    val dyf = glueContext.getSource(
      connectionType = "custom.spark", // for marketplace workflow, use marketplace.spark
      connectionOptions = JsonOptions(options),
      transformationContext = "dyf"
    ).getDynamicFrame()

    assert(dyf.getNumPartitions == 5)
    assert(dyf.count == 100000)
  }

  test("Partition count - generated single chunk data and row count is less than numPartitions") {
    val glueContext: GlueContext = new GlueContext(sparkContext, 1, 20)
    val options = Map(
      "table" -> "call_center",
      "scale" -> "1",
      "numPartitions" -> "100",
      "className" -> className
    )

    val dyf = glueContext.getSource(
      connectionType = "custom.spark", // for marketplace workflow, use marketplace.spark
      connectionOptions = JsonOptions(options),
      transformationContext = "dyf"
    ).getDynamicFrame()

    assert(dyf.getNumPartitions == 1)
    assert(dyf.count == 6)
  }


  test("Partition count - generated multiple chunk data - in parallel") {
    val glueContext: GlueContext = new GlueContext(sparkContext, 1, 20)
    val options = Map(
      "table" -> "customer",
      "scale" -> "100",
      "numPartitions" -> "100",
      "className" -> className
    )

    val dyf = glueContext.getSource(
      connectionType = "custom.spark", // for marketplace workflow, use marketplace.spark
      connectionOptions = JsonOptions(options),
      transformationContext = "dyf"
    ).getDynamicFrame()

    assert(dyf.getNumPartitions == 100)
    assert(dyf.count == 2000000)
  }


}
