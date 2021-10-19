#  Copyright 2016-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT-0

from pyspark.context import SparkContext
from awsglue.context import GlueContext


glue_context = GlueContext(SparkContext())
logger = glue_context.get_logger()

connection_options = {
    "table": "customer",
    "scale": "1",
    "numPartitions": "1",
    "connectionName" : "GlueTPCDSConnection"
}

# read data from data source
datasource0 = glue_context.create_dynamic_frame_from_options(
    connection_type="marketplace.spark",
    connection_options=connection_options)

# validate data reading and row count
expected_count = 100000
result_count = datasource0.count()
assert result_count == expected_count
logger.info(f'Expected record count: {expected_count}, result record count: {result_count}')