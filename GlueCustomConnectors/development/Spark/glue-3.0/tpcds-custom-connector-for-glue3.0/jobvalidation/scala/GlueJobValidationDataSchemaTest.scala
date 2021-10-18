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

object GlueJobValidationDataSchemaTest {
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)

    val optionsMap = Map(
      "table" -> "call_center",
      "scale" -> "1",
      "numPartitions" -> "1",
      "connectionName" -> "GlueTPCDSConnection"
    )

    // create DataSource and read data
    val customSource = glueContext.getSource(
      connectionType = "marketplace.spark",
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "customSource")
    val dyf = customSource.getDynamicFrame()

    // verify schema of 'customer' table
    val expectedSchema = new Schema(new SchemaBuilder()
      .beginStruct()
      .atomicField("cc_call_center_sk", TypeCode.LONG)
      .atomicField("cc_call_center_id", TypeCode.STRING)
      .atomicField("cc_rec_start_date", TypeCode.DATE)
      .atomicField("cc_rec_end_date", TypeCode.DATE)
      .atomicField("cc_closed_date_sk", TypeCode.INT)
      .atomicField("cc_open_date_sk", TypeCode.INT)
      .atomicField("cc_name", TypeCode.STRING)
      .atomicField("cc_class", TypeCode.STRING)
      .atomicField("cc_employees", TypeCode.INT)
      .atomicField("cc_sq_ft", TypeCode.INT)
      .atomicField("cc_hours", TypeCode.STRING)
      .atomicField("cc_manager", TypeCode.STRING)
      .atomicField("cc_mkt_id", TypeCode.INT)
      .atomicField("cc_mkt_class", TypeCode.STRING)
      .atomicField("cc_mkt_desc", TypeCode.STRING)
      .atomicField("cc_market_manager", TypeCode.STRING)
      .atomicField("cc_division", TypeCode.INT)
      .atomicField("cc_division_name", TypeCode.STRING)
      .atomicField("cc_company", TypeCode.INT)
      .atomicField("cc_company_name", TypeCode.STRING)
      .atomicField("cc_street_number", TypeCode.STRING)
      .atomicField("cc_street_name", TypeCode.STRING)
      .atomicField("cc_street_type", TypeCode.STRING)
      .atomicField("cc_suite_number", TypeCode.STRING)
      .atomicField("cc_city", TypeCode.STRING)
      .atomicField("cc_county", TypeCode.STRING)
      .atomicField("cc_state", TypeCode.STRING)
      .atomicField("cc_zip", TypeCode.STRING)
      .atomicField("cc_country", TypeCode.STRING)
      .atomicField("cc_gmt_offset", new DecimalType(5, 2))
      .atomicField("cc_tax_percentage", new DecimalType(5, 2))
      .endStruct().build())

    assert(dyf.schema == expectedSchema)
  }
}