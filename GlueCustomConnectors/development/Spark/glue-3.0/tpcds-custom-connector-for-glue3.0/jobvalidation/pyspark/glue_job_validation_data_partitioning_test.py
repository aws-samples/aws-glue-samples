#  Copyright 2016-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT-0

from pyspark.context import SparkContext
from awsglue.context import GlueContext


glue_context = GlueContext(SparkContext(), minPartitions=1, targetPartitions=20)
logger = glue_context.get_logger()
connection_name = "GlueTPCDSConnection"
connection_type = "marketplace.spark" # use `custom.spark` when running job validation tests by self-build custom connector

'''
1. Partition count - generated single chunk data and row count is more than numPartitions
'''
connection_options_1 = {
    "table": "customer",
    "scale": "1",
    "numPartitions": "5",
    "connectionName" : connection_name
}

# read data from data source
dyf_1 = glue_context.create_dynamic_frame_from_options(
    connection_type=connection_type,
    connection_options=connection_options_1)

# validate number of partitions and row count
expected_partitions = 5
result_partitions = dyf_1.getNumPartitions()
assert result_partitions == expected_partitions
logger.info(f'Expected partition count: {expected_partitions}, result partition count: {result_partitions}')

expected_count = 100000
result_count = dyf_1.count()
assert dyf_1.count() == expected_count
logger.info(f'Expected record count: {expected_count}, result record count: {result_count}')

'''
2. Partition count - generated single chunk data and row count is less than numPartitions
'''
connection_options_2 = {
    "table": "call_center",
    "scale": "1",
    "numPartitions": "100",
    "connectionName" : connection_name
}

# read data from data source
dyf_2 = glue_context.create_dynamic_frame_from_options(
    connection_type=connection_type,
    connection_options=connection_options_2)

# validate number of partitions and row count
expected_partitions = 1
result_partitions = dyf_2.getNumPartitions()
assert result_partitions == expected_partitions
logger.info(f'Expected partition count: {expected_partitions}, result partition count: {result_partitions}')

expected_count = 6
result_count = dyf_2.count()
assert result_count == expected_count
logger.info(f'Expected record count: {expected_count}, result record count: {result_count}')


'''
3. Partition count - generated multiple chunk data - in parallel
'''
connection_options_3 = {
    "table": "customer",
    "scale": "100",
    "numPartitions": "100",
    "connectionName" : connection_name
}

# read data from data source
dyf_3 = glue_context.create_dynamic_frame_from_options(
    connection_type=connection_type,
    connection_options=connection_options_3)

# validate number of partitions and row count
expected_partitions = 100
result_partitions = dyf_3.getNumPartitions()
assert result_partitions == expected_partitions
logger.info(f'Expected partition count: {expected_partitions}, result partition count: {result_partitions}')

expected_count = 2000000
result_count = dyf_3.count()
assert result_count == expected_count
logger.info(f'Expected record count: {expected_count}, result record count: {result_count}')
