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

from __future__ import print_function

import sys
import argparse
from awsglue.context import GlueContext
from pyspark.context import SparkContext

from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import get_transform
from pyspark.sql.types import *
from scripts_utils import *
from pyspark.sql.functions import *

def crawler_backup(glue_context, data, options):
    crawler_name = options['crawler.name']
    backup_location = options['s3.backup_location']
    database_name = options['catalog.database']

    # Only get data for this crawler
    data['table'] = data['table'].filter("parameters.UPDATED_BY_CRAWLER = '%s'" % crawler_name)
    data['partition'] = data['partition'].join(data['table'].withColumn('tableName', col('name')), 'tableName', 'leftsemi')

    if backup_location is not None:
        # Backup the contents of the catalog at an s3 location
        write_backup(data, database_name, backup_location, glue_context)

def crawler_undo(glue_context, **options):
    spark_ctxt = glue_context._instantiatedContext
    crawler_name = options['crawler.name']
    database_name = options['catalog.database']
    timestamp = options['timestamp']
    options["catalog.tableVersions"] = True
    
    data = read_from_catalog(glue_context, options)

    crawler_backup(glue_context, data, options)

    # Find all the table versions for this crawler
    crawler_tables = data['tableVersion'].select(col("table.updateTime").alias("updateTime"), col("table"), col('table.parameters.UPDATED_BY_CRAWLER')).filter("UPDATED_BY_CRAWLER = '%s'" % crawler_name)
    
    # Find the latest previous version of tables for this crawler that were updated or deleted since the last timestamp.
    filtered = crawler_tables.filter("updateTime <= %d" % timestamp).withColumn("filtered_name", col("table.name"))
    update_times = filtered.groupBy("table.name").max("table.updateTime").withColumnRenamed("max(table.updateTime AS `updateTime`)","time") 
    joined = filtered.join(update_times, (col("filtered_name") == col("name")) & (col("updateTime") == col("time")), 'inner')
    tables_to_write = joined.select(col("table.*"))
    
    # Find the tables that were created since the last timestamp
    names = crawler_tables.select(col("table.name")).distinct()
    present_before_timestamp = joined.select(col("table.name"))
    tables_to_delete = names.subtract(present_before_timestamp)

    # Find the partitions that were created since the last timestamp
    partitions_to_delete = data['partition'].withColumn('name', col('tableName')).join(crawler_tables.withColumn('name', col('table.name')), 'name', 'leftsemi').filter("creationTime < %d" % timestamp)

    # Write to Catalog
    write_df_to_catalog(tables_to_write, "table", glue_context, options)
    write_df_to_catalog(tables_to_delete, "tableToDelete", glue_context, options)
    write_df_to_catalog(partitions_to_delete, "partitionToDelete", glue_context, options)

def crawler_undo_options(args):
    # arguments
    parser = argparse.ArgumentParser(description='This script allows you to rollback the effects of a crawler.')
    parser.add_argument('-c', '--crawler-name', required=True, help='Name of the crawler to rollback.')
    parser.add_argument('-b', '--backup-location', required=False, help='Location of the backup to use. If not specified, no backup is used.')
    parser.add_argument('-d', '--database-name', required=False, help='Database to roll back. If not specified, '
                                                                     'the database target of the crawler is used instead.')
    parser.add_argument('-t', '--timestamp', required=False, help='Timestamp to rollback to, in milliseconds since epoch. If not specified, '
                                                                  'the start timestamp of the crawler is used instead.')
    parser.add_argument('-r', '--region', required=False, default=DEFAULT_REGION, help='Optional DataCatalog service endpoint region.')

    options, unknown = parser.parse_known_args(args)

    if not (options.database_name is not None and options.timestamp is not None):
        import boto3 # Import is done here to ensure script does not fail in case boto3 is not required.
        glue_endpoint = DEFAULT_GLUE_ENDPOINT
        glue = boto3.client('glue', endpoint_url="https://%s.%s.amazonaws.com" % (glue_endpoint, options.region))
        crawler = glue.get_crawler(Name=options.crawler_name)['Crawler']

    if options.database_name is not None:
        database_name = options.database_name
    else:
        database_name = crawler['DatabaseName']

    if options.timestamp is not None:
        timestamp = options.timestamp
    else:
        timestamp = crawler['LastCrawlInfo']['StartTime']

    return {
        "catalog.name": DEFAULT_CATALOG_ENDPOINT,
        "catalog.region": options.region,
        "catalog.database": database_name,
        "crawler.name" : options.crawler_name,
        "s3.backup_location" : options.backup_location,
        "timestamp": int(timestamp)
    }

def main():

    # spark env
    sc = SparkContext()
    glue_context = GlueContext(sc)

    crawler_undo(
        glue_context,
        **crawler_undo_options(sys.argv[1:]))

if __name__ == '__main__':
    main()
