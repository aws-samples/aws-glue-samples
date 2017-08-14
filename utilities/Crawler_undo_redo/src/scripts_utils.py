# Copyright 2016-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Licensed under the Amazon Software License (the "License"). You may not use
# this file except in compliance with the License. A copy of the License is
# located at
#
#  http://aws.amazon.com/asl/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express
# or implied. See the License for the specific language governing
# permissions and limitations under the License.

import os
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import get_transform
from pyspark.sql.types import *
from pyspark.sql.functions import *

COLLECT_RESULT_NAME = "collect_list(named_struct(NamePlaceholder(), unresolvedstar()))"
DEFAULT_CATALOG_ENDPOINT = 'datacatalog'
DEFAULT_GLUE_ENDPOINT = 'glue'
DEFAULT_REGION = 'us-east-1'

def write_backup(data, database_name, backup_location, glue_context):
    nested_tables = nest_data_frame(_order_columns_for_backup(data['table']), database_name, 'table')
    nested_partitions = nest_data_frame(_order_columns_for_backup(data['partition']), database_name, 'partition')
    write_df_to_s3(
            glue_context,
            nested_tables.withColumn("table",lit("empty")).select(col("table"),("items"),("database"),("type")).union(nested_partitions),
            backup_location
    )

def _order_columns_for_backup(dataframe):
    return dataframe.select(
        col('name'),
        col('description'),
        col('owner'),
        col('createTime'),
        col('updateTime'),
        col('lastAccessTime'),
        col('lastAnalyzedTime'),
        col('retention'),
        col('storageDescriptor'),
        col('partitionKeys'),
        col('tableType'),
        col('parameters'),
        col('createdBy'),
        col('values'),
        col('namespaceName'),
        col('tableName'),
        col('table')
    )

def nest_data_frame(data_frame, database_name, entity_type):
    if entity_type.startswith("table"):
        # Entity is a table
        return data_frame.agg(collect_list(struct("*"))).withColumnRenamed(COLLECT_RESULT_NAME, "items").withColumn("database",lit(database_name)).withColumn("type", lit(entity_type))
    elif entity_type.startswith("partition"):
        # Entity is a partition
        return data_frame.groupBy('tableName').agg(collect_list(struct("*"))).withColumnRenamed(COLLECT_RESULT_NAME, "items").withColumn("database",lit(database_name)).withColumn("type", lit(entity_type)).withColumnRenamed("tableName","table")
    elif entity_type.startswith("database"):
        return data_frame.groupBy().agg(collect_list(struct("*"))).withColumnRenamed(COLLECT_RESULT_NAME, "items").withColumn("type", lit(entity_type))
    else:
        raise Exception("entity_type %s is not recognized, your backup data may be corrupted..." % entity_type)

def write_df_to_catalog(data_frame, entity_type, glue_context, options):
    # Check if data frame is empty. There is no "empty" method for data frame, this is the closest we get.
    if data_frame.rdd.isEmpty():
        return # nothing to do
    database_name = options['catalog.database']
    nested_data_frame = nest_data_frame(data_frame, database_name, entity_type)
    dynamic_frame = DynamicFrame.fromDF(nested_data_frame, glue_context, entity_type)
    sink = glue_context.getSink('catalog', **options)
    sink.write(dynamic_frame)

def catalog_dict(data_frame):
    databases = data_frame.filter("type = 'database'").select(explode(data_frame['items'])).select(col("col.*"))
    tables = data_frame.filter("type = 'table'").select(explode(data_frame['items'])).select(col("col.*"))
    table_versions = data_frame.filter("type = 'tableVersion'").select(explode(data_frame['items'])).select(col("col.*"))
    partitions = data_frame.filter("type = 'partition'").select(explode(data_frame['items'])).select(col("col.*"))
    tables_to_delete = data_frame.filter("type = 'tableToDelete'").select(explode(data_frame['items'])).select(col("col.*"))
    partitions_to_delete = data_frame.filter("type = 'partitionToDelete'").select(explode(data_frame['items'])).select(col("col.*"))
    return {
        'database' : databases,
        'table' : tables,
        'tableVersion' : table_versions,
        'partition' : partitions,
        'tableToDelete' : tables_to_delete,
        'partitionToDelete' : partitions_to_delete
    }    

def read_from_catalog(glue_context, options):
    return catalog_dict(glue_context.create_dynamic_frame_from_options(
    connection_type="com.amazonaws.services.glue.connections.DataCatalogConnection", connection_options=options).toDF())

def write_df_to_s3(glue_context, data_frame, backup_location):
    dynamic_frame = DynamicFrame.fromDF(data_frame, glue_context, "toS3")
    sink = glue_context.getSink("s3", path=backup_location)
    sink.setFormat("json")
    sink.write(dynamic_frame)

def read_from_s3(glue_context, backup_location):
    src = glue_context.getSource("file", paths=[backup_location])
    src.setFormat('json')
    return catalog_dict(src.getFrame().toDF())
