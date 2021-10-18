package com.amazonaws.services.glue.marketplace.connector.tpcds.localvalidation

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.schema.{Schema, TypeCode}
import com.amazonaws.services.glue.schema.builders.SchemaBuilder
import com.amazonaws.services.glue.schema.types.DecimalType
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SharedSparkContext
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class DataSchemaTest extends FunSuite with BeforeAndAfterEach with SharedSparkContext {
  // skipped row count test because the test is conducted on `DataSourceTest`

  // call_center schema includes customer's. Therefore, this table is used for this schema test.
  test("test data source data schema - 'call_center' table which has long, string, date, int and decimal types in its schema") {
    val glueContext: GlueContext = new GlueContext(sparkContext, 1, 20)

    // set up connection options
    val optionsMap = Map(
      "table" -> "call_center",
      "scale" -> "1",
      "numPartitions" -> "1",
      "className" -> "tpcds"
    )

    // create DataSource and read data
    val customSource = glueContext.getSource(
      connectionType = "custom.spark",
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "customSource")
    val dyf = customSource.getDynamicFrame()

    // verify schema of 'call_center' table
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

    assert(dyf.schema === expectedSchema)
  }
}
