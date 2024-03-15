import sys
from unittest.mock import MagicMock

import pytest
from awsglue import DynamicFrame
from awsglue.dynamicframe import DynamicFrameReader, DynamicFrameWriter
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from awsglue.context import GlueContext

import logging

logger = logging.getLogger("py4j")
logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_context():
    if SparkContext._active_spark_context is None:
        return SparkContext('local[*]', 'GlueUnitTests', conf=SparkConf())
    return SparkContext._active_spark_context


@pytest.fixture(scope="session")
def spark(spark_context):
    return SparkSession(spark_context)


@pytest.fixture
def glue_context(spark_context):
    return GlueContext(spark_context, minPartitions=2, targetPartitions=2)


@pytest.fixture
def read_from_catalog_responses(spark):
    responses = {}
    DynamicFrameReader.from_catalog_real = DynamicFrameReader.from_catalog

    def mock_create_dynamic_frame_from_catalog(self, database, table_name, *args, **kwargs):
        key = f"{database}.{table_name}"
        if key not in responses:
            raise ValueError(f"Data not mocked for table: {key}")
        df = spark.createDataFrame(responses.get(key))
        return DynamicFrame.fromDF(df, self._glue_context, "mock")

    DynamicFrameReader.from_catalog = mock_create_dynamic_frame_from_catalog
    return responses


@pytest.fixture
def read_from_options_responses(spark):
    responses = []
    response_index = 0
    DynamicFrameReader.from_options_real = DynamicFrameReader.from_options

    def mock_create_dynamic_frame_from_options(self, *args, **kwargs):
        nonlocal response_index
        if response_index >= len(responses):
            raise ValueError("More calls to from_catalog has been made than responses provided")
        response = responses[response_index]
        response_index += 1
        if response is None:
            DynamicFrameReader.from_catalog_real(self, *args, **kwargs)
        else:
            df = spark.createDataFrame(response)
            return DynamicFrame.fromDF(df, self._glue_context, "mock")

    DynamicFrameReader.from_options = mock_create_dynamic_frame_from_options
    return responses


@pytest.fixture
def write_from_options_mock(spark):
    DynamicFrameWriter.from_options = MagicMock()
    return DynamicFrameWriter.from_options


@pytest.fixture
def write_from_catalog_mock(spark):
    DynamicFrameWriter.from_catalog = MagicMock()
    return DynamicFrameWriter.from_catalog


@pytest.fixture
def purge_s3_mock():
    GlueContext.purge_s3_path_real = GlueContext.purge_s3_path
    GlueContext.purge_s3_path = MagicMock()
    return GlueContext.purge_s3_path


@pytest.fixture
def purge_table_mock():
    GlueContext.purge_table_real = GlueContext.purge_table
    GlueContext.purge_table_path = MagicMock()
    return GlueContext.purge_table_path
