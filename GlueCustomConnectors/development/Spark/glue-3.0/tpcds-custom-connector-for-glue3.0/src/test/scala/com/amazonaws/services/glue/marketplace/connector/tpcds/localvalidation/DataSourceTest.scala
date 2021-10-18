package com.amazonaws.services.glue.marketplace.connector.tpcds.localvalidation

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SharedSparkContext
import org.scalatest.{BeforeAndAfterEach, FunSuite}


class DataSourceTest extends FunSuite with BeforeAndAfterEach with SharedSparkContext {
  test("test data source connection") {
    val glueContext: GlueContext = new GlueContext(sparkContext, 1, 20)

    val optionsMap = Map(
      "table" -> "customer",
      "scale" -> "1",
      "numPartitions" -> "1",
      "className" -> "tpcds"
    )

    // create DataSource
    val customSource = glueContext.getSource(
      connectionType = "custom.spark",
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "customSource")
    val dyf = customSource.getDynamicFrame()

    // verify data
    val expectedRowCount = 100000
    assert(dyf.count === expectedRowCount)
  }

}
