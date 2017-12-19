#  Copyright 2016-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  Licensed under the Amazon Software License (the "License"). You may not use
#  this file except in compliance with the License. A copy of the License is
#  located at
#
#    http://aws.amazon.com/asl/
#
#  and in the "LICENSE" file accompanying this file. This file is distributed
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.


from __future__ import print_function

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

from hive_metastore_migration import *


def transform_df_to_catalog_import_schema(sql_context, glue_context, df_databases, df_tables, df_partitions):
    df_databases_array = df_databases.select(df_databases['type'], array(df_databases['item']).alias('items'))
    df_tables_array = df_tables.select(df_tables['type'], df_tables['database'],
                                       array(df_tables['item']).alias('items'))
    df_partitions_array_batched = batch_metastore_partitions(sql_context=sql_context, df_parts=df_partitions)
    dyf_databases = DynamicFrame.fromDF(
        dataframe=df_databases_array, glue_ctx=glue_context, name='dyf_databases')
    dyf_tables = DynamicFrame.fromDF(
        dataframe=df_tables_array, glue_ctx=glue_context, name='dyf_tables')
    dyf_partitions = DynamicFrame.fromDF(
        dataframe=df_partitions_array_batched, glue_ctx=glue_context, name='dyf_partitions')
    return dyf_databases, dyf_tables, dyf_partitions


def import_datacatalog(sql_context, glue_context, datacatalog_name, databases, tables, partitions, region):

    # TEMP: get around datacatalog writer performance issue
    limited_partitions = partitions.limit(10)

    (dyf_databases, dyf_tables, dyf_partitions) = transform_df_to_catalog_import_schema(
        sql_context, glue_context, databases, tables, limited_partitions)

    # load
    glue_context.write_dynamic_frame.from_options(
        frame=dyf_databases, connection_type='catalog',
        connection_options={'catalog.name': datacatalog_name, 'catalog.region': region})
    glue_context.write_dynamic_frame.from_options(
        frame=dyf_tables, connection_type='catalog',
        connection_options={'catalog.name': datacatalog_name, 'catalog.region': region})
    glue_context.write_dynamic_frame.from_options(
        frame=dyf_partitions, connection_type='catalog',
        connection_options={'catalog.name': datacatalog_name, 'catalog.region': region})


def metastore_full_migration(sc, sql_context, glue_context, connection, datacatalog_name, db_prefix, table_prefix
                             , region):
    # extract
    hive_metastore = HiveMetastore(connection, sql_context)
    hive_metastore.extract_metastore()

    # transform
    (databases, tables, partitions) = HiveMetastoreTransformer(
        sc, sql_context, db_prefix, table_prefix).transform(hive_metastore)

    #load
    import_datacatalog(sql_context, glue_context, datacatalog_name, databases, tables, partitions, region)


def metastore_import_from_s3(sql_context, glue_context,db_input_dir, tbl_input_dir, parts_input_dir,
                             datacatalog_name, region):

    # extract
    databases = sql_context.read.json(path=db_input_dir, schema=METASTORE_DATABASE_SCHEMA)
    tables = sql_context.read.json(path=tbl_input_dir, schema=METASTORE_TABLE_SCHEMA)
    partitions = sql_context.read.json(path=parts_input_dir, schema=METASTORE_PARTITION_SCHEMA)

    # load
    import_datacatalog(sql_context, glue_context, datacatalog_name, databases, tables, partitions, region)


def main():
    # arguments
    from_s3 = 'from-s3'
    from_jdbc = 'from-jdbc'
    parser = argparse.ArgumentParser(prog=sys.argv[0])
    parser.add_argument('-m', '--mode', required=True, choices=[from_s3, from_jdbc], help='Choose to migrate metastore either from JDBC or from S3')
    parser.add_argument('-c', '--connection-name', required=False, help='Glue Connection name for Hive metastore JDBC connection')
    parser.add_argument('-R', '--region', required=False, help='AWS region of target Glue DataCatalog, default to "us-east-1"')
    parser.add_argument('-d', '--database-prefix', required=False, help='Optional prefix for database names in Glue DataCatalog')
    parser.add_argument('-t', '--table-prefix', required=False, help='Optional prefix for table name in Glue DataCatalog')
    parser.add_argument('-D', '--database-input-path', required=False, help='An S3 path containing json files of metastore database entities')
    parser.add_argument('-T', '--table-input-path', required=False, help='An S3 path containing json files of metastore table entities')
    parser.add_argument('-P', '--partition-input-path', required=False, help='An S3 path containing json files of metastore partition entities')

    options = get_options(parser, sys.argv)
    if options['mode'] == from_s3:
        validate_options_in_mode(
            options=options, mode=from_s3,
            required_options=['database_input_path', 'table_input_path', 'partition_input_path'],
            not_allowed_options=['database_prefix', 'table_prefix']
        )
    elif options['mode'] == from_jdbc:
        validate_options_in_mode(
            options=options, mode=from_jdbc,
            required_options=['connection_name'],
            not_allowed_options=['database_input_path', 'table_input_path', 'partition_input_path']
        )
    else:
        raise AssertionError('unknown mode ' + options['mode'])

    validate_aws_regions(options['region'])

    # spark env
    (conf, sc, sql_context) = get_spark_env()
    glue_context = GlueContext(sc)

    # launch job
    if options['mode'] == from_s3:
        metastore_import_from_s3(
            sql_context=sql_context,
            glue_context=glue_context,
            db_input_dir=options['database_input_path'],
            tbl_input_dir=options['table_input_path'],
            parts_input_dir=options['partition_input_path'],
            datacatalog_name='datacatalog',
            region=options.get('region') or 'us-east-1'
        )
    elif options['mode'] == from_jdbc:
        glue_context.extract_jdbc_conf(options['connection_name'])
        metastore_full_migration(
            sc=sc,
            sql_context=sql_context,
            glue_context=glue_context,
            connection=glue_context.extract_jdbc_conf(options['connection_name']),
            db_prefix=options.get('database_prefix') or '',
            table_prefix=options.get('table_prefix') or '',
            datacatalog_name='datacatalog',
            region=options.get('region') or 'us-east-1'
        )

if __name__ == '__main__':
    main()
