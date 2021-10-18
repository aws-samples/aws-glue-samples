#  Copyright 2016-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT-0

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.gluetypes import Field, IntegerType, LongType, StructType, StringType, DecimalType, DateType


glue_context = GlueContext(SparkContext())
logger = glue_context.get_logger()

connection_options = {
    "table": "call_center",
    "scale": "1",
    "numPartitions": "1",
    "connectionName" : "GlueTPCDSConnection"
}

# read data from data source
datasource0 = glue_context.create_dynamic_frame_from_options(
    connection_type="marketplace.spark", # use `custom.spark` when running job validation tests by self-build custom connector
    connection_options=connection_options)

# validate data schema
expected_schema = StructType([
    Field("cc_call_center_sk", LongType()),
    Field("cc_call_center_id", StringType()),
    Field("cc_rec_start_date", DateType()),
    Field("cc_rec_end_date", DateType()),
    Field("cc_closed_date_sk", IntegerType()),
    Field("cc_open_date_sk", IntegerType()),
    Field("cc_name", StringType()),
    Field("cc_class", StringType()),
    Field("cc_employees", IntegerType()),
    Field("cc_sq_ft", IntegerType()),
    Field("cc_hours", StringType()),
    Field("cc_manager", StringType()),
    Field("cc_mkt_id", IntegerType()),
    Field("cc_mkt_class", StringType()),
    Field("cc_mkt_desc", StringType()),
    Field("cc_market_manager", StringType()),
    Field("cc_division", IntegerType()),
    Field("cc_division_name", StringType()),
    Field("cc_company", IntegerType()),
    Field("cc_company_name", StringType()),
    Field("cc_street_number", StringType()),
    Field("cc_street_name", StringType()),
    Field("cc_street_type", StringType()),
    Field("cc_suite_number", StringType()),
    Field("cc_city", StringType()),
    Field("cc_county", StringType()),
    Field("cc_state", StringType()),
    Field("cc_zip", StringType()),
    Field("cc_country", StringType()),
    Field("cc_gmt_offset", DecimalType(precision=5, scale=2)),
    Field("cc_tax_percentage", DecimalType(precision=5, scale=2))
])

result_schema = datasource0.schema()
assert result_schema == expected_schema
logger.info(f'Expected schema: {expected_schema.jsonValue()}')
logger.info(f'Result schema: {result_schema.jsonValue()}')
logger.info("Result schema in tree structure: ")
datasource0.printSchema()