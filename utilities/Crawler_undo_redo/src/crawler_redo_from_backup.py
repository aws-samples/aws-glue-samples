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

def crawler_redo_from_backup(glue_context, **options):
    spark_ctxt = glue_context._instantiatedContext
    backup_location = options['s3.backup_location']

    # Read from s3
    data = read_from_s3(glue_context, backup_location)

    # Write to Catalog
    for entity_type in ['table', 'tableToDelete', 'partition', 'partitionToDelete']:
        write_df_to_catalog(data[entity_type], entity_type, glue_context, options)

def crawler_redo_from_backup_options(args):
    # arguments
    parser = argparse.ArgumentParser(description='This script allows you to restore a namespace to a specific backup.')
    parser.add_argument('-c', '--crawler-name', required=True, help='Name of the crawler to restore.')
    parser.add_argument('-b', '--backup-location', required=True, help='Location of the backup to use.')
    parser.add_argument('-d', '--database-name', required=False, help='Database to back up. If not specified, '
                                                                     'the database target of the crawler is used instead.')
    parser.add_argument('-r', '--region', required=False, default=DEFAULT_REGION, help='Optional service endpoint region.')


    options, unknown = parser.parse_known_args(args)

    if options.database_name is not None:
        database_name = options.database_name
    else:
        import boto3
        glue_endpoint = DEFAULT_GLUE_ENDPOINT
        glue = boto3.client('glue', endpoint_url="https://%s.%s.amazonaws.com" % (glue_endpoint, options.region))
        crawler = glue.get_crawler(Name=options.crawler_name)['Crawler']
        database_name = crawler['DatabaseName']

    return {
        "catalog.name": DEFAULT_CATALOG_ENDPOINT,
        "catalog.region": options.region,
        "catalog.database": database_name,
        "crawler.name" : options.crawler_name,
        "s3.backup_location" : options.backup_location
    }

def main():

    # spark env
    sc = SparkContext()
    glue_context = GlueContext(sc)

    crawler_redo_from_backup(
        glue_context,
        **crawler_redo_from_backup_options(sys.argv[1:]))

if __name__ == '__main__':
    main()
