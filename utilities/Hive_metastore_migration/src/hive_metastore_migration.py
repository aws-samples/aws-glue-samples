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

# This script avoids adding any external dependencies
# except for python 2.7 standard library and Spark 2.1
import sys
import argparse
import re
import logging
from time import localtime, strftime
from types import MethodType
from datetime import tzinfo, datetime, timedelta

from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SQLContext, DataFrame, Row
from pyspark.sql.functions import lit, struct, array, col, UserDefinedFunction, concat, monotonically_increasing_id, explode
from pyspark.sql.types import StringType, StructField, StructType, LongType, ArrayType, MapType, IntegerType, \
    FloatType, BooleanType


MYSQL_DRIVER_CLASS = 'com.mysql.jdbc.Driver'

# flags for migration direction
FROM_METASTORE = 'from-metastore'
TO_METASTORE = 'to-metastore'

DATACATALOG_STORAGE_DESCRIPTOR_SCHEMA = \
    StructType([
        StructField('inputFormat', StringType(), True),
        StructField('compressed', BooleanType(), False),
        StructField('storedAsSubDirectories', BooleanType(), False),
        StructField('location', StringType(), True),
        StructField('numberOfBuckets', IntegerType(), False),
        StructField('outputFormat', StringType(), True),
        StructField('bucketColumns', ArrayType(StringType(), True), True),
        StructField('columns', ArrayType(StructType([
            StructField('name', StringType(), True),
            StructField('type', StringType(), True),
            StructField('comment', StringType(), True)
        ]), True), True),
        StructField('parameters', MapType(StringType(), StringType(), True), True),
        StructField('serdeInfo', StructType([
            StructField('name', StringType(), True),
            StructField('serializationLibrary', StringType(), True),
            StructField('parameters', MapType(StringType(), StringType(), True), True)
        ]), True),
        StructField('skewedInfo', StructType([
            StructField('skewedColumnNames', ArrayType(StringType(), True), True),
            StructField('skewedColumnValueLocationMaps', MapType(StringType(), StringType(), True), True),
            StructField('skewedColumnValues', ArrayType(StringType(), True), True)
        ]), True),
        StructField('sortColumns', ArrayType(StructType([
            StructField('column', StringType(), True),
            StructField('order', IntegerType(), True)
        ]), True), True)
    ])

DATACATALOG_DATABASE_ITEM_SCHEMA = \
    StructType([
        StructField('description', StringType(), True),
        StructField('locationUri', StringType(), True),
        StructField('name', StringType(), False),
        StructField('parameters', MapType(StringType(), StringType(), True), True)
    ])

DATACATALOG_TABLE_ITEM_SCHEMA = \
    StructType([
        StructField('createTime', StringType(), True),
        StructField('lastAccessTime', StringType(), True),
        StructField('owner', StringType(), True),
        StructField('retention', IntegerType(), True),
        StructField('name', StringType(), False),
        StructField('tableType', StringType(), True),
        StructField('viewExpandedText', StringType(), True),
        StructField('viewOriginalText', StringType(), True),
        StructField('parameters', MapType(StringType(), StringType(), True), True),
        StructField('partitionKeys', ArrayType(StructType([
            StructField('name', StringType(), True),
            StructField('type', StringType(), True),
            StructField('comment', StringType(), True)
        ]), True), True),
        StructField('storageDescriptor', DATACATALOG_STORAGE_DESCRIPTOR_SCHEMA, True)
    ])

DATACATALOG_PARTITION_ITEM_SCHEMA = \
    StructType([
        StructField('creationTime', StringType(), True),
        StructField('lastAccessTime', StringType(), True),
        StructField('namespaceName', StringType(), True),
        StructField('tableName', StringType(), True),
        StructField('parameters', MapType(StringType(), StringType(), True), True),
        StructField('storageDescriptor', DATACATALOG_STORAGE_DESCRIPTOR_SCHEMA, True),
        StructField('values', ArrayType(StringType(), False), False)
    ])

DATACATALOG_DATABASE_SCHEMA = \
    StructType([
        StructField('items', ArrayType(
            DATACATALOG_DATABASE_ITEM_SCHEMA, False),
                    True),
        StructField('type', StringType(), False)
    ])

DATACATALOG_TABLE_SCHEMA = \
    StructType([
        StructField('database', StringType(), False),
        StructField('type', StringType(), False),
        StructField('items', ArrayType(DATACATALOG_TABLE_ITEM_SCHEMA, False), True)
    ])

DATACATALOG_PARTITION_SCHEMA = \
    StructType([
        StructField('database', StringType(), False),
        StructField('table', StringType(), False),
        StructField('items', ArrayType(DATACATALOG_PARTITION_ITEM_SCHEMA, False), True),
        StructField('type', StringType(), False)
    ])

METASTORE_PARTITION_SCHEMA = \
    StructType([
        StructField('database', StringType(), False),
        StructField('table', StringType(), False),
        StructField('item', DATACATALOG_PARTITION_ITEM_SCHEMA, True),
        StructField('type', StringType(), False)
    ])

METASTORE_DATABASE_SCHEMA = \
    StructType([
        StructField('item', DATACATALOG_DATABASE_ITEM_SCHEMA, True),
        StructField('type', StringType(), False)
    ])

METASTORE_TABLE_SCHEMA = \
    StructType([
        StructField('database', StringType(), False),
        StructField('type', StringType(), False),
        StructField('item', DATACATALOG_TABLE_ITEM_SCHEMA, True)
    ])


def append(l, elem):
    """Append list with element and return the list modified"""
    if elem is not None:
        l.append(elem)
    return l


def extend(l1, l2):
    """Extend l1 with l2 and return l1 modified"""
    l1.extend(l2)
    return l1


def remove(l, elem):
    l.remove(elem)
    return l


def remove_all(l1, l2):
    return [elem for elem in l1 if elem not in l2]


def construct_struct_schema(schema_tuples_list):
    struct_fields = []
    atomic_types_dict = {
        'int': IntegerType(),
        'long': LongType(),
        'string': StringType()
    }
    for (col_name, col_type, nullable) in schema_tuples_list:
        field_type = atomic_types_dict[col_type]
        struct_fields.append(StructField(name=col_name, dataType=field_type, nullable=nullable))
    return StructType(struct_fields)


def empty(df):
    return df.rdd.isEmpty()


def drop_columns(df, columns_to_drop):
    for col in columns_to_drop:
        df = df.drop(col)
    return df


def rename_columns(df, rename_tuples=None):
    """
    Rename columns, for each key in rename_map, rename column from key to value
    :param df: dataframe
    :param rename_map: map for columns to be renamed
    :return: new dataframe with columns renamed
    """
    for old, new in rename_tuples:
        df = df.withColumnRenamed(old, new)
    return df


def get_schema_type(df, column_name):
    return df.select(column_name).schema.fields[0].dataType


def join_other_to_single_column(df, other, on, how, new_column_name):
    """
    :param df: this dataframe
    :param other: other dataframe
    :param on: the column to join on
    :param how: :param how: str, default 'inner'. One of `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`.
    :param new_column_name: the column name for all fields from the other dataframe
    :return: this dataframe, with a single new column containing all fields of the other dataframe
    :type df: DataFrame
    :type other: DataFrame
    :type new_column_name: str
    """
    other_cols = remove(other.columns, on)
    other_combined = other.select([on, struct(other_cols).alias(new_column_name)])
    return df.join(other=other_combined, on=on, how=how)


def coalesce_by_row_count(df, desired_rows_per_partition=10):
    """
    Coalesce dataframe to reduce number of partitions, to avoid fragmentation of data
    :param df: dataframe
    :param desired_rows_per_partition: desired number of rows per partition, there is no guarantee the actual rows count
    is larger or smaller
    :type df: DataFrame
    :return: dataframe coalesced
    """
    count = df.count()
    partitions = count / desired_rows_per_partition + 1
    return df.coalesce(partitions)


def batch_items_within_partition(sql_context, df, key_col, value_col, values_col):
    """
    Group a DataFrame of key, value pairs, create a list of values for the same key in each spark partition, but there
    is no cross-partition data interaction, so the same key may be shown multiple times in the output dataframe
    :param sql_context: spark sqlContext
    :param df: DataFrame with only two columns, a key_col and a value_col
    :param key_col: name of key column
    :param value_col: name of value column
    :param values_col: name of values column, which is an array of value_col
    :type df: DataFrame
    :type key_col: str
    :type value_col: str
    :return: DataFrame of values grouped by key within each partition
    """
    def group_by_key(it):
        grouped = dict()
        for row in it:
            (k, v) = (row[key_col], row[value_col])
            if k in grouped:
                grouped[k].append(v)
            else:
                grouped[k] = [v]
        row = Row(key_col, values_col)
        for k in grouped:
            yield row(k, grouped[k])

    return sql_context.createDataFrame(data=df.rdd.mapPartitions(group_by_key), schema=StructType([
        StructField(key_col, get_schema_type(df, key_col), True),
        StructField(values_col, ArrayType(get_schema_type(df, value_col)), True)
    ]))


def batch_metastore_partitions(sql_context, df_parts):
    """
    :param sql_context: the spark SqlContext
    :param df_parts: the dataframe of partitions with the schema of DATACATALOG_PARTITION_SCHEMA
    :type df_parts: DataFrame
    :return: a dataframe partition in which each row contains a list of catalog partitions
    belonging to the same database and table.
    """
    df_kv = df_parts.select(struct(['database', 'table', 'type']).alias('key'), 'item')
    batched_kv = batch_items_within_partition(sql_context, df_kv, key_col='key', value_col='item', values_col='items')
    batched_parts = batched_kv.select(
        batched_kv.key.database.alias('database'),
        batched_kv.key.table.alias('table'),
        batched_kv.key.type.alias('type'), batched_kv.items)
    return batched_parts


def register_methods_to_dataframe():
    """
    Register self-defined helper methods to dataframe
    """
    DataFrame.empty = MethodType(empty, None, DataFrame)
    DataFrame.drop_columns = MethodType(drop_columns, None, DataFrame)
    DataFrame.rename_columns = MethodType(rename_columns, None, DataFrame)
    DataFrame.get_schema_type = MethodType(get_schema_type, None, DataFrame)
    DataFrame.join_other_to_single_column = MethodType(join_other_to_single_column, None, DataFrame)


register_methods_to_dataframe()

class UTC(tzinfo):
    """
    Have to implement timezone class myself because python standard library doesn't have one, and I want to avoid adding
    external libraries, to make it simpler for people new to Spark to run the script
    """

    def utcoffset(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return timedelta(0)


class HiveMetastoreTransformer:
    def transform_params(self, params_df, id_col, key='PARAM_KEY', value='PARAM_VALUE'):
        """
        Transform a PARAMS table dataframe to dataframe of 2 columns: (id, Map<key, value>)
        :param params_df: dataframe of PARAMS table
        :param id_col: column name for id field
        :param key: column name for key
        :param value: column name for value
        :return: dataframe of params in map
        """
        return self.kv_pair_to_map(params_df, id_col, key, value, 'parameters')

    def kv_pair_to_map(self, df, id_col, key, value, map_col_name):
        def merge_dict(dict1, dict2):
            dict1.update(dict2)
            return dict1

        def remove_none_key(dictionary):
            if None in dictionary:
                del dictionary[None]
            return dictionary

        id_type = df.get_schema_type(id_col)
        map_type = MapType(keyType=df.get_schema_type(key), valueType=df.get_schema_type(value))
        output_schema = StructType([StructField(name=id_col, dataType=id_type, nullable=False),
                                    StructField(name=map_col_name, dataType=map_type)])

        return self.sql_context.createDataFrame(
            df.rdd.map(lambda row: (row[id_col], {row[key]: row[value]})).reduceByKey(merge_dict).map(
                lambda (id_name, dictionary): (id_name, remove_none_key(dictionary))), output_schema)

    def join_with_params(self, df, df_params, id_col):
        df_params_map = self.transform_params(params_df=df_params, id_col=id_col)
        df_with_params = df.join(other=df_params_map, on=id_col, how='left_outer')
        return df_with_params

    def transform_df_with_idx(self, df, id_col, idx, payloads_column_name, payload_type, payload_func):
        """
        Aggregate dataframe by ID, create a single PAYLOAD column where each row is a list of data sorted by IDX, and
        each element is a payload created by payload_func. Example:

        Input:
        df =
        +---+---+----+----+
        | ID|IDX|COL1|COL2|
        +---+---+----+----+
        |  1|  2|   1|   1|
        |  1|  1|   2|   2|
        |  2|  1|   3|   3|
        +---+---+----+----+
        id = 'ID'
        idx = 'IDX'
        payload_list_name = 'PAYLOADS'
        payload_func = row.COL1 + row.COL2

        Output:
        +------+--------+
        |    ID|PAYLOADS|
        +------+--------+
        |     1| [4, 2] |
        |     2|    [6] |
        +------+--------+

        The method assumes (ID, IDX) is input table primary key. ID and IDX values cannot be None

        :param df: dataframe with id and idx columns
        :param id_col: name of column for id
        :param idx: name of column for sort index
        :param payloads_column_name: the column name for payloads column in the output dataframe
        :param payload_func: the function to transform an input row to a payload object
        :param payload_type: the schema type for a single payload object
        :return: output dataframe with data grouped by id and sorted by idx
        """
        rdd_result = df.rdd.map(lambda row: (row[id_col], (row[idx], payload_func(row)))) \
            .aggregateByKey([], append, extend) \
            .map(lambda (id_column, list_with_idx): (id_column, sorted(list_with_idx, key=lambda t: t[0]))) \
            .map(lambda (id_column, list_with_idx): (id_column, [payload for index, payload in list_with_idx]))

        schema = StructType([StructField(name=id_col, dataType=LongType(), nullable=False),
                             StructField(name=payloads_column_name, dataType=ArrayType(elementType=payload_type))])
        return self.sql_context.createDataFrame(rdd_result, schema)

    def transform_ms_partition_keys(self, ms_partition_keys):
        return self.transform_df_with_idx(df=ms_partition_keys,
                                          id_col='TBL_ID',
                                          idx='INTEGER_IDX',
                                          payloads_column_name='partitionKeys',
                                          payload_type=StructType([
                                              StructField(name='name', dataType=StringType()),
                                              StructField(name='type', dataType=StringType()),
                                              StructField(name='comment', dataType=StringType())]),
                                          payload_func=lambda row: (
                                              row['PKEY_NAME'], row['PKEY_TYPE'], row['PKEY_COMMENT']))

    def transform_ms_partition_key_vals(self, ms_partition_key_vals):
        return self.transform_df_with_idx(df=ms_partition_key_vals,
                                          id_col='PART_ID',
                                          idx='INTEGER_IDX',
                                          payloads_column_name='values',
                                          payload_type=StringType(),
                                          payload_func=lambda row: row['PART_KEY_VAL'])

    def transform_ms_bucketing_cols(self, ms_bucketing_cols):
        return self.transform_df_with_idx(df=ms_bucketing_cols,
                                          id_col='SD_ID',
                                          idx='INTEGER_IDX',
                                          payloads_column_name='bucketColumns',
                                          payload_type=StringType(),
                                          payload_func=lambda row: row['BUCKET_COL_NAME'])

    def transform_ms_columns(self, ms_columns):
        return self.transform_df_with_idx(df=ms_columns,
                                          id_col='CD_ID',
                                          idx='INTEGER_IDX',
                                          payloads_column_name='columns',
                                          payload_type=StructType([
                                              StructField(name='name', dataType=StringType()),
                                              StructField(name='type', dataType=StringType()),
                                              StructField(name='comment', dataType=StringType())]),
                                          payload_func=lambda row: (
                                              row['COLUMN_NAME'], row['TYPE_NAME'], row['COMMENT']))

    def transform_ms_skewed_col_names(self, ms_skewed_col_names):
        return self.transform_df_with_idx(df=ms_skewed_col_names,
                                          id_col='SD_ID',
                                          idx='INTEGER_IDX',
                                          payloads_column_name='skewedColumnNames',
                                          payload_type=StringType(),
                                          payload_func=lambda row: row['SKEWED_COL_NAME'])

    def transform_ms_skewed_string_list_values(self, ms_skewed_string_list_values):
        return self.transform_df_with_idx(df=ms_skewed_string_list_values,
                                          id_col='STRING_LIST_ID',
                                          idx='INTEGER_IDX',
                                          payloads_column_name='skewedColumnValuesList',
                                          payload_type=StringType(),
                                          payload_func=lambda row: row['STRING_LIST_VALUE'])

    def transform_ms_sort_cols(self, sort_cols):
        return self.transform_df_with_idx(df=sort_cols,
                                          id_col='SD_ID',
                                          idx='INTEGER_IDX',
                                          payloads_column_name='sortColumns',
                                          payload_type=StructType([
                                              StructField(name='column', dataType=StringType()),
                                              StructField(name='order', dataType=IntegerType())]),
                                          payload_func=lambda row: (row['COLUMN_NAME'], row['ORDER']))

    @staticmethod
    def udf_escape_chars(param_value):
        ret_param_value = param_value.replace('\\', '\\\\')\
            .replace('|', '\\|')\
            .replace('"', '\\"')\
            .replace('{', '\\{')\
            .replace(':', '\\:')\
            .replace('}', '\\}')

        return ret_param_value

    @staticmethod
    def udf_skewed_values_to_str():
        return UserDefinedFunction(lambda values: ''.join(
            map(lambda v: '' if v is None else '%d%%%s' % (len(v), v), values)
        ), StringType())

    @staticmethod
    def modify_column_by_udf(df, udf, column_to_modify, new_column_name=None):
        """
        transform a column of the dataframe with the user-defined function, keeping all other columns unchanged.
        :param new_column_name: new column name. If None, old column name will be used
        :param df: dataframe
        :param udf: user-defined function
        :param column_to_modify: the name of the column to modify.
        :type column_to_modify: str
        :return: the dataframe with single column modified
        """
        if new_column_name is None:
            new_column_name = column_to_modify
        return df.select(
            *[udf(column).alias(new_column_name) if column == column_to_modify else column for column in df.columns])

    @staticmethod
    def add_prefix_to_column(df, column_to_modify, prefix):
        if prefix is None or prefix == '':
            return df
        udf = UserDefinedFunction(lambda col: prefix + col, StringType())
        return HiveMetastoreTransformer.modify_column_by_udf(df=df, udf=udf, column_to_modify=column_to_modify)

    @staticmethod
    def utc_timestamp_to_iso8601_time(df, date_col_name, new_date_col_name):
        """
        Tape DataCatalog writer uses Gson to parse Date column. According to Gson deserializer, (https://goo.gl/mQdXuK)
        it uses either java DateFormat or ISO-8601 format. I convert Date to be compatible with java DateFormat
        :param df: dataframe with a column of unix timestamp in seconds of number type
        :param date_col_name: timestamp column
        :param new_date_col_name: new column with converted timestamp, if None, old column name is used
        :type df: DataFrame
        :type date_col_name: str
        :type new_date_col_name: str
        :return: dataframe with timestamp column converted to string representation of time
        """

        def convert_time(timestamp):
            if timestamp is None:
                return None
            return datetime.fromtimestamp(timestamp=float(timestamp), tz=UTC()).strftime("%b %d, %Y %I:%M:%S %p")

        udf_time_int_to_date = UserDefinedFunction(convert_time, StringType())
        return HiveMetastoreTransformer.modify_column_by_udf(df, udf_time_int_to_date, date_col_name, new_date_col_name)

    @staticmethod
    def transform_timestamp_cols(df, date_cols_map):
        """
        Call timestamp_int_to_iso8601_time in batch, rename all time columns in date_cols_map keys.
        :param df: dataframe with columns of unix timestamp
        :param date_cols_map: map from old column name to new column name
        :type date_cols_map: dict
        :return: dataframe
        """
        for k, v in date_cols_map.iteritems():
            df = HiveMetastoreTransformer.utc_timestamp_to_iso8601_time(df, k, v)
        return df

    @staticmethod
    def fill_none_with_empty_list(df, column):
        """
        Given a column of array type, fill each None value with empty list.
        This is not doable by df.na.fill(), Spark will throw Unsupported value type java.util.ArrayList ([]).
        :param df: dataframe with array type
        :param column: column name string, the column must be array type
        :return: dataframe that fills None with empty list for the given column
        """
        return HiveMetastoreTransformer.modify_column_by_udf(
            df=df,
            udf=UserDefinedFunction(
                lambda lst: [] if lst is None else lst,
                get_schema_type(df, column)
            ),
            column_to_modify=column,
            new_column_name=column
        )

    @staticmethod
    def join_dbs_tbls(ms_dbs, ms_tbls):
        return ms_dbs.select('DB_ID', 'NAME').join(other=ms_tbls, on='DB_ID', how='inner')

    def transform_skewed_values_and_loc_map(self, ms_skewed_string_list_values, ms_skewed_col_value_loc_map):
        # columns: (STRING_LIST_ID:BigInt, skewedColumnValuesList:List[String])
        skewed_values_list = self.transform_ms_skewed_string_list_values(ms_skewed_string_list_values)

        # columns: (STRING_LIST_ID:BigInt, skewedColumnValuesStr:String)
        skewed_value_str = self.modify_column_by_udf(df=skewed_values_list,
                                                     udf=HiveMetastoreTransformer.udf_skewed_values_to_str(),
                                                     column_to_modify='skewedColumnValuesList',
                                                     new_column_name='skewedColumnValuesStr')

        # columns: (SD_ID: BigInt, STRING_LIST_ID_KID: BigInt, STRING_LIST_ID: BigInt,
        # LOCATION: String, skewedColumnValuesStr: String)
        skewed_value_str_with_loc = ms_skewed_col_value_loc_map \
            .join(other=skewed_value_str,
                  on=[ms_skewed_col_value_loc_map['STRING_LIST_ID_KID'] == skewed_value_str['STRING_LIST_ID']],
                  how='inner')

        # columns: (SD_ID: BigInt, skewedColumnValueLocationMaps: Map[String, String])
        skewed_column_value_location_maps = self.kv_pair_to_map(df=skewed_value_str_with_loc,
                                                                id_col='SD_ID',
                                                                key='skewedColumnValuesStr',
                                                                value='LOCATION',
                                                                map_col_name='skewedColumnValueLocationMaps')

        # columns: (SD_ID: BigInt, skewedColumnValues: List[String])
        skewed_column_values = self.sql_context.createDataFrame(
            data=skewed_value_str_with_loc.rdd.map(
                lambda row: (row['SD_ID'], row['skewedColumnValues'])
            ).aggregateByKey([], append, extend),
            schema=StructType([
                StructField(name='SD_ID', dataType=LongType()),
                StructField(name='skewedColumnValues', dataType=ArrayType(elementType=StringType()))
            ]))

        return skewed_column_values, skewed_column_value_location_maps

    def transform_skewed_info(self, ms_skewed_col_names, ms_skewed_string_list_values, ms_skewed_col_value_loc_map):
        (skewed_column_values, skewed_column_value_location_maps) = self.transform_skewed_values_and_loc_map(
            ms_skewed_string_list_values, ms_skewed_col_value_loc_map)

        # columns: (SD_ID: BigInt, skewedColumnNames: List[String])
        skewed_column_names = self.transform_ms_skewed_col_names(ms_skewed_col_names)

        # columns: (SD_ID: BigInt, skewedColumnNames: List[String], skewedColumnValues: List[String],
        # skewedColumnValueLocationMaps: Map[String, String])
        skewed_info = skewed_column_names \
            .join(other=skewed_column_value_location_maps, on='SD_ID', how='outer') \
            .join(other=skewed_column_values, on='SD_ID', how='outer')
        return skewed_info

    # TODO: remove when escape special characters fix in DatacatalogWriter is pushed to production.
    def transform_param_value(self, df):
        udf_escape_chars = UserDefinedFunction(HiveMetastoreTransformer.udf_escape_chars, StringType())

        return df.select('*',
                         udf_escape_chars('PARAM_VALUE').alias('PARAM_VALUE_ESCAPED'))\
            .drop('PARAM_VALUE')\
            .withColumnRenamed('PARAM_VALUE_ESCAPED', 'PARAM_VALUE')

    def transform_ms_serde_info(self, ms_serdes, ms_serde_params):
        escaped_serde_params = self.transform_param_value(ms_serde_params)

        serde_with_params = self.join_with_params(df=ms_serdes, df_params=escaped_serde_params, id_col='SERDE_ID')
        serde_info = serde_with_params.rename_columns(rename_tuples=[
            ('NAME', 'name'),
            ('SLIB', 'serializationLibrary')
        ])
        return serde_info

    def transform_storage_descriptors(self, ms_sds, ms_sd_params, ms_columns, ms_bucketing_cols, ms_serdes,
                                      ms_serde_params, ms_skewed_col_names, ms_skewed_string_list_values,
                                      ms_skewed_col_value_loc_map, ms_sort_cols):
        bucket_columns = self.transform_ms_bucketing_cols(ms_bucketing_cols)
        columns = self.transform_ms_columns(ms_columns)
        parameters = self.transform_params(params_df=ms_sd_params, id_col='SD_ID')
        serde_info = self.transform_ms_serde_info(ms_serdes=ms_serdes, ms_serde_params=ms_serde_params)
        skewed_info = self.transform_skewed_info(ms_skewed_col_names=ms_skewed_col_names,
                                                 ms_skewed_string_list_values=ms_skewed_string_list_values,
                                                 ms_skewed_col_value_loc_map=ms_skewed_col_value_loc_map)
        sort_columns = self.transform_ms_sort_cols(ms_sort_cols)

        storage_descriptors_joined = ms_sds \
            .join(other=bucket_columns, on='SD_ID', how='left_outer') \
            .join(other=columns, on='CD_ID', how='left_outer') \
            .join(other=parameters, on='SD_ID', how='left_outer') \
            .join_other_to_single_column(other=serde_info, on='SERDE_ID', how='left_outer',
                                         new_column_name='serdeInfo') \
            .join_other_to_single_column(other=skewed_info, on='SD_ID', how='left_outer',
                                         new_column_name='skewedInfo') \
            .join(other=sort_columns, on='SD_ID', how='left_outer')

        storage_descriptors_renamed = storage_descriptors_joined.rename_columns(rename_tuples=[
            ('INPUT_FORMAT', 'inputFormat'),
            ('OUTPUT_FORMAT', 'outputFormat'),
            ('LOCATION', 'location'),
            ('NUM_BUCKETS', 'numberOfBuckets'),
            ('IS_COMPRESSED', 'compressed'),
            ('IS_STOREDASSUBDIRECTORIES', 'storedAsSubDirectories')
        ])

        storage_descriptors_with_empty_sorted_cols = HiveMetastoreTransformer.fill_none_with_empty_list(
            storage_descriptors_renamed, 'sortColumns')
        storage_descriptors_final = storage_descriptors_with_empty_sorted_cols.drop_columns(['SERDE_ID', 'CD_ID'])
        return storage_descriptors_final

    def transform_tables(self, db_tbl_joined, ms_table_params, storage_descriptors, ms_partition_keys):
        tbls_date_transformed = self.transform_timestamp_cols(db_tbl_joined, date_cols_map={
            'CREATE_TIME': 'createTime',
            'LAST_ACCESS_TIME': 'lastAccessTime'
        })
        tbls_with_params = self.join_with_params(df=tbls_date_transformed, df_params=self.transform_param_value(ms_table_params), id_col='TBL_ID')
        partition_keys = self.transform_ms_partition_keys(ms_partition_keys)

        tbls_joined = tbls_with_params\
            .join(other=partition_keys, on='TBL_ID', how='left_outer')\
            .join_other_to_single_column(other=storage_descriptors, on='SD_ID', how='left_outer',
                                         new_column_name='storageDescriptor')

        tbls_renamed = rename_columns(df=tbls_joined, rename_tuples=[
            ('NAME', 'database'),
            ('TBL_NAME', 'name'),
            ('TBL_TYPE', 'tableType'),
            ('CREATE_TIME', 'createTime'),
            ('LAST_ACCESS_TIME', 'lastAccessTime'),
            ('OWNER', 'owner'),
            ('RETENTION', 'retention'),
            ('VIEW_EXPANDED_TEXT', 'viewExpandedText'),
            ('VIEW_ORIGINAL_TEXT', 'viewOriginalText'),
        ])

        tbls_dropped_cols = tbls_renamed.drop_columns(['DB_ID', 'TBL_ID', 'SD_ID', 'LINK_TARGET_ID'])
        tbls_drop_invalid = tbls_dropped_cols.na.drop(how='any', subset=['name', 'database'])
        tbls_with_empty_part_cols = HiveMetastoreTransformer.fill_none_with_empty_list(
            tbls_drop_invalid, 'partitionKeys')
        tbls_final = tbls_with_empty_part_cols.select(
            'database', struct(remove(tbls_dropped_cols.columns, 'database')).alias('item')
        ).withColumn('type', lit('table'))
        return tbls_final

    def transform_partitions(self, db_tbl_joined, ms_partitions, storage_descriptors, ms_partition_params,
                             ms_partition_key_vals):
        parts_date_transformed = self.transform_timestamp_cols(df=ms_partitions, date_cols_map={
            'CREATE_TIME': 'creationTime',
            'LAST_ACCESS_TIME': 'lastAccessTime'
        })
        db_tbl_names = db_tbl_joined.select(db_tbl_joined['NAME'].alias('namespaceName'),
                                            db_tbl_joined['TBL_NAME'].alias('tableName'), 'DB_ID', 'TBL_ID')
        parts_with_db_tbl = parts_date_transformed.join(other=db_tbl_names, on='TBL_ID', how='inner')
        parts_with_params = self.join_with_params(df=parts_with_db_tbl, df_params=self.transform_param_value(ms_partition_params), id_col='PART_ID')
        parts_with_sd = parts_with_params.join_other_to_single_column(
            other=storage_descriptors, on='SD_ID', how='left_outer', new_column_name='storageDescriptor')
        part_values = self.transform_ms_partition_key_vals(ms_partition_key_vals)
        parts_with_values = parts_with_sd.join(other=part_values, on='PART_ID', how='left_outer')
        parts_renamed = rename_columns(df=parts_with_values, rename_tuples=[
            ('CREATE_TIME', 'createTime'),
            ('LAST_ACCESS_TIME', 'lastAccessTime')
        ])
        parts_dropped_cols = parts_renamed.drop_columns([
            'DB_ID', 'TBL_ID', 'PART_ID', 'SD_ID', 'PART_NAME', 'LINK_TARGET_ID'
        ])
        parts_drop_invalid = parts_dropped_cols.na.drop(how='any', subset=['values', 'namespaceName', 'tableName'])
        parts_final = parts_drop_invalid.select(
            parts_drop_invalid['namespaceName'].alias('database'),
            parts_drop_invalid['tableName'].alias('table'),
            struct(parts_drop_invalid.columns).alias('item')
        ).withColumn('type', lit('partition'))
        return parts_final

    def transform_databases(self, ms_dbs, ms_database_params):
        dbs_with_params = self.join_with_params(df=ms_dbs, df_params=ms_database_params, id_col='DB_ID')
        dbs_renamed = rename_columns(df=dbs_with_params, rename_tuples=[
            ('NAME', 'name'),
            ('DESC', 'description'),
            ('DB_LOCATION_URI', 'locationUri')
        ])
        dbs_dropped_cols = dbs_renamed.drop_columns(['DB_ID', 'OWNER_NAME', 'OWNER_TYPE'])
        dbs_drop_invalid = dbs_dropped_cols.na.drop(how='any', subset=['name'])
        dbs_final = dbs_drop_invalid.select(struct(dbs_dropped_cols.columns).alias('item')) \
            .withColumn('type', lit('database'))
        return dbs_final

    def transform(self, hive_metastore):
        dbs_prefixed = HiveMetastoreTransformer.add_prefix_to_column(hive_metastore.ms_dbs, 'NAME', self.db_prefix)
        tbls_prefixed = HiveMetastoreTransformer.add_prefix_to_column(
            hive_metastore.ms_tbls, 'TBL_NAME', self.table_prefix)

        databases = self.transform_databases(
            ms_dbs=dbs_prefixed,
            ms_database_params=hive_metastore.ms_database_params)

        db_tbl_joined = HiveMetastoreTransformer.join_dbs_tbls(ms_dbs=dbs_prefixed, ms_tbls=tbls_prefixed)

        storage_descriptors = self.transform_storage_descriptors(
            ms_sds=hive_metastore.ms_sds,
            ms_sd_params=hive_metastore.ms_sd_params,
            ms_columns=hive_metastore.ms_columns,
            ms_bucketing_cols=hive_metastore.ms_bucketing_cols,
            ms_serdes=hive_metastore.ms_serdes,
            ms_serde_params=hive_metastore.ms_serde_params,
            ms_skewed_col_names=hive_metastore.ms_skewed_col_names,
            ms_skewed_string_list_values=hive_metastore.ms_skewed_string_list_values,
            ms_skewed_col_value_loc_map=hive_metastore.ms_skewed_col_value_loc_map,
            ms_sort_cols=hive_metastore.ms_sort_cols)

        tables = self.transform_tables(
            db_tbl_joined=db_tbl_joined,
            ms_table_params=hive_metastore.ms_table_params,
            storage_descriptors=storage_descriptors,
            ms_partition_keys=hive_metastore.ms_partition_keys)

        partitions = self.transform_partitions(
            db_tbl_joined=db_tbl_joined,
            ms_partitions=hive_metastore.ms_partitions,
            storage_descriptors=storage_descriptors,
            ms_partition_params=hive_metastore.ms_partition_params,
            ms_partition_key_vals=hive_metastore.ms_partition_key_vals)

        return databases, tables, partitions

    def __init__(self, sc, sql_context, db_prefix, table_prefix):
        self.sc = sc
        self.sql_context = sql_context
        self.db_prefix = db_prefix
        self.table_prefix = table_prefix


class DataCatalogTransformer:
    """
    The class to extract data from DataCatalog entities into Hive metastore tables.
    """
    @staticmethod
    def udf_array_to_map(array):
        if array is None:
            return array
        return dict((i, v) for i, v in enumerate(array))

    @staticmethod
    def udf_partition_name_from_keys_vals(keys, vals):
        """
        udf_partition_name_from_keys_vals, create name string from array of keys and vals
        :param keys: array of partition keys from a datacatalog table
        :param vals: array of partition vals from a datacatalog partition
        :return: partition name, a string in the form 'key1(type),key2(type)=val1,val2'
        """
        if not keys or not vals:
            return ""
        s_keys = []
        for k in keys:
            s_keys.append("%s(%s)" % (k['name'], k['type']))

        return ','.join(s_keys) + '=' + ','.join(vals)

    @staticmethod
    def udf_milliseconds_str_to_timestamp(milliseconds_str):
        return 0L if milliseconds_str is None else long(milliseconds_str) / 1000

    @staticmethod
    def udf_string_list_str_to_list(str):
        """
        udf_string_list_str_to_list, transform string of a specific format into an array
        :param str: array represented as a string, format should be '<len>%['ele1', 'ele2', 'ele3']'
        :return: array, in this case would be [ele1, ele2, ele3]
        """
        try:
            r = re.compile("\d%\[('\w+',?\s?)+\]")
            if r.match(str) is None:
                return []
            return [s.strip()[1:-1] for s in str.split('%')[1][1:-1].split(',')]
        except (IndexError, AssertionError):
            return []

    @staticmethod
    def udf_parameters_to_map(parameters):
        return parameters.asDict()

    @staticmethod
    def udf_with_non_null_locationuri(locationUri):
        if locationUri is None:
            return ""
        return locationUri

    @staticmethod
    def generate_idx_for_df(df, id_name, col_name, col_schema):
        """
        generate_idx_for_df, explodes rows with array as a column into a new row for each element in
        the array, with 'INTEGER_IDX' indicating its index in the original array.
        :param df: dataframe with array columns
        :param id_name: the id field of df
        :param col_name: the col of df to explode
        :param col_schema: the schema of each element in col_name array
        :return: new df with exploded rows.
        """
        idx_udf = UserDefinedFunction(
            DataCatalogTransformer.udf_array_to_map,
            MapType(IntegerType(), col_schema, True))

        return df.withColumn('idx_columns', idx_udf(col(col_name)))\
            .select(id_name, explode('idx_columns').alias("INTEGER_IDX", "col"))

    @staticmethod
    def column_date_to_timestamp(df, column):
        date_to_udf_time_int = UserDefinedFunction(
            DataCatalogTransformer.udf_milliseconds_str_to_timestamp,
            IntegerType())
        return df.withColumn(column + '_new', date_to_udf_time_int(col(column)))\
            .drop(column)\
            .withColumnRenamed(column + '_new', column)

    @staticmethod
    def params_to_df(df, id_name):
        return df.select(col(id_name), explode(df['parameters'])
                         .alias('PARAM_KEY', 'PARAM_VALUE'))

    def generate_id_df(self, df, id_name):
        """
        generate_id_df, creates a new column <id_name>, with unique id for each row in df
        :param df: dataframe to be given id column
        :param id_name: the id name
        :return: new df with generated id
        """
        initial_id = self.start_id_map[id_name] if id_name in self.start_id_map else 0

        row_with_index = Row(*(["id"] + df.columns))
        df_columns = df.columns

        # using zipWithIndex to generate consecutive ids, rather than monotonically_increasing_ids
        # consecutive ids are desired because ids unnecessarily large will complicate future
        # appending to the same metastore (generated ids have to be bigger than the max of ids
        # already in the database
        def make_row_with_uid(columns, row, uid):
            row_dict = row.asDict()
            return row_with_index(*([uid] + [row_dict.get(c) for c in columns]))

        df_with_pk = (df.rdd
                      .zipWithIndex()
                      .map(lambda row_uid: make_row_with_uid(df_columns, *row_uid))
                      .toDF(StructType([StructField("zip_id", LongType(), False)] + df.schema.fields)))

        return df_with_pk.withColumn(id_name, df_with_pk.zip_id + initial_id).drop("zip_id")

    def extract_dbs(self, databases):
        ms_dbs_no_id = databases.select('item.*')
        ms_dbs = self.generate_id_df(ms_dbs_no_id, 'DB_ID')

        # if locationUri is null, fill with empty string value
        udf_fill_location_uri = UserDefinedFunction(DataCatalogTransformer
                                                    .udf_with_non_null_locationuri, StringType())

        ms_dbs = ms_dbs.select('*',
                               udf_fill_location_uri('locationUri')
                               .alias('locationUriNew'))\
            .drop('locationUri')\
            .withColumnRenamed('locationUriNew', 'locationUri')

        return ms_dbs

    def reformat_dbs(self, ms_dbs):
        ms_dbs = rename_columns(df=ms_dbs, rename_tuples=[
            ('locationUri', 'DB_LOCATION_URI'),
            ('name', 'NAME')
        ])

        return ms_dbs

    def extract_tbls(self, tables, ms_dbs):
        ms_tbls_no_id = tables\
            .join(ms_dbs, tables.database == ms_dbs.NAME, 'inner')\
            .select(tables.database, tables.item, ms_dbs.DB_ID)\
            .select('DB_ID', 'database', 'item.*')# database col needed for later
        ms_tbls = self.generate_id_df(ms_tbls_no_id, 'TBL_ID')

        return ms_tbls

    def reformat_tbls(self, ms_tbls):
        # reformat CREATE_TIME and LAST_ACCESS_TIME
        ms_tbls = DataCatalogTransformer.column_date_to_timestamp(ms_tbls, 'createTime')
        ms_tbls = DataCatalogTransformer.column_date_to_timestamp(ms_tbls, 'lastAccessTime')

        ms_tbls = rename_columns(df=ms_tbls, rename_tuples=[
            ('database', 'DB_NAME'),
            ('createTime', 'CREATE_TIME'),
            ('lastAccessTime', 'LAST_ACCESS_TIME'),
            ('owner', 'OWNER'),
            ('retention', 'RETENTION'),
            ('name', 'TBL_NAME'),
            ('tableType', 'TBL_TYPE'),
            ('viewExpandedText', 'VIEW_EXPANDED_TEXT'),
            ('viewOriginalText', 'VIEW_ORIGINAL_TEXT')
        ])

        return ms_tbls

    def get_name_for_partitions(self, ms_partitions, ms_tbls):
        tbls_for_join = ms_tbls.select('TBL_ID', 'partitionKeys')

        combine_part_key_and_vals = \
            UserDefinedFunction(DataCatalogTransformer.udf_partition_name_from_keys_vals,
                                StringType())

        ms_partitions = ms_partitions.join(tbls_for_join, ms_partitions.TBL_ID == tbls_for_join.TBL_ID, 'inner')\
            .drop(tbls_for_join.TBL_ID)\
            .withColumn('PART_NAME', combine_part_key_and_vals(col('partitionKeys'), col('values')))\
            .drop('partitionKeys')

        return ms_partitions

    def extract_partitions(self, partitions, ms_dbs, ms_tbls):
        ms_partitions = partitions.join(ms_dbs, partitions.database == ms_dbs.NAME, 'inner')\
            .select(partitions.item, ms_dbs.DB_ID, partitions.table)

        cond = [ms_partitions.table == ms_tbls.TBL_NAME, ms_partitions.DB_ID == ms_tbls.DB_ID]

        ms_partitions = ms_partitions.join(ms_tbls, cond, 'inner')\
            .select(ms_partitions.item, ms_tbls.TBL_ID)\
            .select('TBL_ID', 'item.*')

        # generate PART_ID
        ms_partitions = self.generate_id_df(ms_partitions, 'PART_ID')

        ms_partitions = self.get_name_for_partitions(ms_partitions, ms_tbls)

        return ms_partitions

    def reformat_partitions(self, ms_partitions):
        # reformat CREATE_TIME and LAST_ACCESS_TIME
        ms_partitions = DataCatalogTransformer.column_date_to_timestamp(ms_partitions, 'creationTime')
        ms_partitions = DataCatalogTransformer.column_date_to_timestamp(ms_partitions, 'lastAccessTime')

        ms_partitions = rename_columns(df=ms_partitions, rename_tuples=[
            ('creationTime', 'CREATE_TIME'),
            ('lastAccessTime', 'LAST_ACCESS_TIME')
        ])

        return ms_partitions

    def extract_sds(self, ms_tbls, ms_partitions):

        ms_tbls = ms_tbls.withColumn('ID', concat(ms_tbls.TBL_NAME, ms_tbls.DB_NAME))
        ms_partitions = ms_partitions.withColumn('ID', ms_partitions.PART_ID.cast(StringType()))
        ms_tbls_sds = ms_tbls\
            .select('ID', 'storageDescriptor.*')\
            .withColumn('type', lit("table"))
        ms_partitions_sds = ms_partitions\
            .select('ID', 'storageDescriptor.*')\
            .withColumn('type', lit("partition"))

        ms_sds_no_id = ms_partitions_sds\
            .union(ms_tbls_sds)

        ms_sds = self.generate_id_df(ms_sds_no_id, 'SD_ID')

        ms_sds_for_join = ms_sds.select('type', 'ID', 'SD_ID')

        cond = [ms_sds_for_join.type == 'partition',
                ms_sds_for_join.ID == ms_partitions.ID]
        ms_partitions = ms_partitions\
            .join(ms_sds_for_join, cond, 'inner')\
            .drop_columns(['ID', 'type'])

        cond = [ms_sds_for_join.type == 'table', ms_sds_for_join.ID == ms_tbls.ID]
        ms_tbls = ms_tbls\
            .join(ms_sds_for_join, cond, 'inner')\
            .drop('ID').drop_columns(['ID', 'type'])

        ms_sds = ms_sds.drop_columns(['ID', 'type'])

        return (ms_sds, ms_tbls, ms_partitions)

    def reformat_sds(self, ms_sds):
        ms_sds = rename_columns(df=ms_sds, rename_tuples=[
            ('inputFormat', 'INPUT_FORMAT'),
            ('compressed', 'IS_COMPRESSED'),
            ('storedAsSubDirectories', 'IS_STOREDASSUBDIRECTORIES'),
            ('location', 'LOCATION'),
            ('numberOfBuckets', 'NUM_BUCKETS'),
            ('outputFormat', 'OUTPUT_FORMAT')
        ])

        ms_sds = self.generate_id_df(ms_sds, 'CD_ID')
        ms_sds = self.generate_id_df(ms_sds, 'SERDE_ID')

        return ms_sds

    def extract_from_dbs(self, hms, ms_dbs):
        ms_database_params = DataCatalogTransformer.params_to_df(ms_dbs, 'DB_ID')

        hms.ms_database_params = ms_database_params
        hms.ms_dbs = ms_dbs.drop('parameters').withColumnRenamed('description', 'DESC')

    def extract_from_tbls(self, hms, ms_tbls):
        ms_table_params = DataCatalogTransformer.params_to_df(ms_tbls, 'TBL_ID')

        part_key_schema = StructType([
            StructField('name', StringType(), True),
            StructField('type', StringType(), True),
            StructField('comment', StringType(), True)
        ])

        ms_partition_keys = DataCatalogTransformer.generate_idx_for_df(ms_tbls, 'TBL_ID', 'partitionKeys', part_key_schema)\
            .select('TBL_ID', 'INTEGER_IDX', 'col.*')

        ms_partition_keys = rename_columns(df=ms_partition_keys, rename_tuples=[
            ('name', 'PKEY_NAME'),
            ('type', 'PKEY_TYPE'),
            ('comment', 'PKEY_COMMENT')
        ])

        hms.ms_table_params = ms_table_params
        hms.ms_partition_keys = ms_partition_keys
        hms.ms_tbls = ms_tbls.drop_columns(['partitionKeys', 'storageDescriptor',
                                            'parameters', 'DB_NAME'])

    def extract_from_partitions(self, hms, ms_partitions):

        # split into table PARTITION_PARAMS
        ms_partition_params = DataCatalogTransformer.params_to_df(ms_partitions, 'PART_ID')

        # split into table PARTITION_KEY_VAL
        part_key_val_schema = StringType()

        ms_partition_key_vals = \
            DataCatalogTransformer.generate_idx_for_df(ms_partitions, 'PART_ID', 'values', part_key_val_schema)\
                .withColumnRenamed('col', 'PART_KEY_VAL')

        hms.ms_partition_key_vals = ms_partition_key_vals
        hms.ms_partition_params = ms_partition_params
        hms.ms_partitions = ms_partitions.drop_columns(['namespaceName', 'values',
                                                        'storageDescriptor', 'tableName',
                                                        'parameters'])

    def extract_from_sds(self, hms, ms_sds):
        ms_sd_params = DataCatalogTransformer.params_to_df(ms_sds, 'SD_ID')

        ms_cds = ms_sds.select('CD_ID')

        ms_columns = self.extract_from_sds_columns(ms_sds)

        (ms_serdes, ms_serde_params) = self.extract_from_sds_serde_info(ms_sds)

        ms_sort_cols = self.extract_from_sds_sort_cols(ms_sds)

        hms.ms_sd_params = ms_sd_params
        hms.ms_cds = ms_cds
        hms.ms_columns = ms_columns
        hms.ms_serdes = ms_serdes
        hms.ms_serde_params = ms_serde_params
        hms.ms_sort_cols = ms_sort_cols

        self.extract_from_sds_skewed_info(hms, ms_sds)
        hms.ms_sds = ms_sds.drop_columns(['parameters', 'serdeInfo',
                                          'bucketColumns', 'columns',
                                          'skewedInfo', 'sortColumns'])

    def extract_from_sds_columns(self, ms_sds):
        COLUMN_SCHEMA = StructType([
            StructField('name', StringType(), True),
            StructField('type', StringType(), True),
            StructField('comment', StringType(), True)
        ])

        ms_columns = DataCatalogTransformer.generate_idx_for_df(ms_sds, 'CD_ID', 'columns', COLUMN_SCHEMA)\
            .select('CD_ID', 'INTEGER_IDX', 'col.*')

        ms_columns = rename_columns(df=ms_columns, rename_tuples=[
            ('name', 'COLUMN_NAME'),
            ('type', 'TYPE_NAME'),
            ('comment', 'COMMENT')
        ])

        return ms_columns

    def extract_from_sds_serde_info(self, ms_sds):
        ms_serdes = ms_sds.select('SERDE_ID' , 'serdeInfo.*')
        ms_serdes = rename_columns(df=ms_serdes, rename_tuples=[
            ('name', 'NAME'),
            ('serializationLibrary', 'SLIB')
        ])

        ms_serde_params = DataCatalogTransformer.params_to_df(ms_serdes, 'SERDE_ID')
        ms_serdes = ms_serdes.drop('parameters')

        return (ms_serdes, ms_serde_params)

    def extract_from_sds_skewed_info(self, hms, ms_sds):

        skewed_info = ms_sds.select('SD_ID', 'skewedInfo.*')

        ms_skewed_col_names = skewed_info.select('SD_ID', explode('skewedColumnNames').alias('SKEWED_COL_NAME'))

        # with extra field 'STRING_LIST_STR'
        skewed_col_value_loc_map = skewed_info\
            .select('SD_ID', explode('skewedColumnValueLocationMaps')\
                    .alias('STRING_LIST_STR', 'LOCATION'))

        skewed_col_value_loc_map = self.generate_id_df(skewed_col_value_loc_map, 'STRING_LIST_ID_KID')

        udf_string_list_list = UserDefinedFunction(DataCatalogTransformer.udf_string_list_str_to_list,
                                                   ArrayType(StringType(), True))

        skewed_string_list_values = skewed_col_value_loc_map\
            .select(col('STRING_LIST_ID_KID').alias('STRING_LIST_ID'),
                    udf_string_list_list('STRING_LIST_STR').alias('STRING_LIST_LIST'))

        ms_skewed_string_list_values = DataCatalogTransformer.generate_idx_for_df(
            skewed_string_list_values,
            'STRING_LIST_ID',
            'STRING_LIST_LIST',
            StringType()
        ).withColumnRenamed('col', 'STRING_LIST_VALUE')

        ms_skewed_col_value_loc_map = skewed_col_value_loc_map\
            .drop_columns(['STRING_LIST_STR'])

        ms_skewed_string_list = ms_skewed_string_list_values.select('STRING_LIST_ID')

        hms.ms_skewed_col_names = ms_skewed_col_names
        hms.ms_skewed_col_value_loc_map = ms_skewed_col_value_loc_map
        hms.ms_skewed_string_list_values = ms_skewed_string_list_values
        hms.ms_skewed_string_list = ms_skewed_string_list

    def extract_from_sds_sort_cols(self, ms_sds):

        return DataCatalogTransformer.generate_idx_for_df(ms_sds, 'SD_ID', 'sortColumns',
                                                          col_schema=StructType([
                                                              StructField('column', StringType(), True),
                                                              StructField('order', IntegerType(), True)
                                                          ]))\
            .select('SD_ID', 'INTEGER_IDX', 'col.*')\
            .withColumnRenamed('column', 'COLUMN_NAME')\
            .withColumnRenamed('order', 'ORDER')

    def get_start_id_for_id_name(self, hms):
        hms.extract_metastore()

        info_tuples = {
            ('ms_dbs', 'DB_ID'),
            ('ms_tbls', 'TBL_ID'),
            ('ms_sds', 'SD_ID'),
            ('ms_sds', 'CD_ID'),
            ('ms_sds', 'SERDE_ID'),
            ('ms_partitions', 'PART_ID'),
            ('ms_skewed_col_value_loc_map', 'STRING_LIST_ID_KID')
        }

        for table_name, id_name in info_tuples:
            hms_df = eval('hms.' + table_name)

            if hms_df and hms_df.count() > 0:
                max_id = hms_df.select(id_name).rdd.max()[0] + 1
            else:
                max_id = 0
            self.start_id_map[id_name] = max_id

    def transform(self, hms, databases, tables, partitions):

        # for metastore tables that require unique ids, find max id (start id)
        # for rows already in each table
        self.get_start_id_for_id_name(hms)

        # establish foreign keys between dbs, tbls, partitions, sds
        ms_dbs = self.reformat_dbs(self.extract_dbs(databases))

        ms_tbls = self.reformat_tbls(self.extract_tbls(tables, ms_dbs))

        ms_partitions = self.reformat_partitions(self.extract_partitions(partitions, ms_dbs, ms_tbls))

        (ms_sds, ms_tbls, ms_partitions) = self.extract_sds(ms_tbls, ms_partitions)
        ms_sds = self.reformat_sds(ms_sds)

        # extract child tables from above four tables and then clean up extra columns
        self.extract_from_dbs(hms, ms_dbs)
        self.extract_from_tbls(hms, ms_tbls)
        self.extract_from_sds(hms, ms_sds)
        self.extract_from_partitions(hms, ms_partitions)

    def __init__(self, sc, sql_context):
        self.sc = sc
        self.sql_context = sql_context
        self.start_id_map = dict()


class HiveMetastore:
    """
    The class to extract data from Hive Metastore into DataFrames and write Dataframes to
    Hive Metastore. Each field represents a single Hive Metastore table.
    As a convention, the fields are prefixed by ms_ to show that it is raw Hive Metastore data
    """

    def read_table(self, connection, db_name='hive', table_name=None):
        """
        Load a JDBC table into Spark Dataframe
        """
        return self.sql_context.read.format('jdbc').options(
            url=connection['url'],
            dbtable='%s.%s' % (db_name, table_name),
            user=connection['user'],
            password=connection['password'],
            driver=MYSQL_DRIVER_CLASS
        ).load()

    def write_table(self, connection, db_name='hive', table_name=None, df=None):
        """
        Write from Spark Dataframe into a JDBC table
        """
        return df.write.jdbc(
            url=connection['url'],
            table='%s.%s' % (db_name, table_name),
            mode='append',
            properties={
                'user': connection['user'],
                'password': connection['password'],
                'driver': MYSQL_DRIVER_CLASS
            }
        )

    def extract_metastore(self):
        self.ms_dbs = self.read_table(connection=self.connection, table_name='DBS')
        self.ms_database_params = self.read_table(connection=self.connection, table_name='DATABASE_PARAMS')
        self.ms_tbls = self.read_table(connection=self.connection, table_name='TBLS')
        self.ms_table_params = self.read_table(connection=self.connection, table_name='TABLE_PARAMS')
        self.ms_columns = self.read_table(connection=self.connection, table_name='COLUMNS_V2')
        self.ms_bucketing_cols = self.read_table(connection=self.connection, table_name='BUCKETING_COLS')
        self.ms_sds = self.read_table(connection=self.connection, table_name='SDS')
        self.ms_sd_params = self.read_table(connection=self.connection, table_name='SD_PARAMS')
        self.ms_serdes = self.read_table(connection=self.connection, table_name='SERDES')
        self.ms_serde_params = self.read_table(connection=self.connection, table_name='SERDE_PARAMS')
        self.ms_skewed_col_names = self.read_table(connection=self.connection, table_name='SKEWED_COL_NAMES')
        self.ms_skewed_string_list = self.read_table(connection=self.connection, table_name='SKEWED_STRING_LIST')
        self.ms_skewed_string_list_values = self.read_table(connection=self.connection,
                                                            table_name='SKEWED_STRING_LIST_VALUES')
        self.ms_skewed_col_value_loc_map = self.read_table(connection=self.connection,
                                                           table_name='SKEWED_COL_VALUE_LOC_MAP')
        self.ms_sort_cols = self.read_table(connection=self.connection, table_name='SORT_COLS')
        self.ms_partitions = self.read_table(connection=self.connection, table_name='PARTITIONS')
        self.ms_partition_params = self.read_table(connection=self.connection, table_name='PARTITION_PARAMS')
        self.ms_partition_keys = self.read_table(connection=self.connection, table_name='PARTITION_KEYS')
        self.ms_partition_key_vals = self.read_table(connection=self.connection, table_name='PARTITION_KEY_VALS')

    # order of write matters here
    def export_to_metastore(self):
        self.ms_dbs.show()
        self.write_table(connection=self.connection, table_name='DBS', df=self.ms_dbs)
        self.write_table(connection=self.connection, table_name='DATABASE_PARAMS', df=self.ms_database_params)
        self.write_table(connection=self.connection, table_name='CDS', df=self.ms_cds)
        self.write_table(connection=self.connection, table_name='SERDES', df=self.ms_serdes)
        self.write_table(connection=self.connection, table_name='SERDE_PARAMS', df=self.ms_serde_params)
        self.write_table(connection=self.connection, table_name='COLUMNS_V2', df=self.ms_columns)
        self.write_table(connection=self.connection, table_name='SDS', df=self.ms_sds)
        self.write_table(connection=self.connection, table_name='SD_PARAMS', df=self.ms_sd_params)
        self.write_table(connection=self.connection, table_name='SKEWED_COL_NAMES', df=self.ms_skewed_col_names)
        self.write_table(connection=self.connection, table_name='SKEWED_STRING_LIST', df=self.ms_skewed_string_list)
        self.write_table(connection=self.connection, table_name='SKEWED_STRING_LIST_VALUES',
                         df=self.ms_skewed_string_list_values)
        self.write_table(connection=self.connection, table_name='SKEWED_COL_VALUE_LOC_MAP',
                         df=self.ms_skewed_col_value_loc_map)
        self.write_table(connection=self.connection, table_name='SORT_COLS',
                         df=self.ms_sort_cols)
        self.write_table(connection=self.connection, table_name='TBLS', df=self.ms_tbls)
        self.write_table(connection=self.connection, table_name='TABLE_PARAMS', df=self.ms_table_params)
        self.write_table(connection=self.connection, table_name='PARTITION_KEYS', df=self.ms_partition_keys)
        self.write_table(connection=self.connection, table_name='PARTITIONS', df=self.ms_partitions)
        self.write_table(connection=self.connection, table_name='PARTITION_PARAMS', df=self.ms_partition_params)
        self.write_table(connection=self.connection, table_name='PARTITION_KEY_VALS', df=self.ms_partition_key_vals)

    def __init__(self, connection, sql_context):
        self.connection = connection
        self.sql_context = sql_context
        self.ms_dbs = None
        self.ms_database_params = None
        self.ms_tbls = None
        self.ms_table_params = None
        self.ms_columns = None
        self.ms_bucketing_cols = None
        self.ms_sds = None
        self.ms_sd_params = None
        self.ms_serdes = None
        self.ms_serde_params = None
        self.ms_skewed_col_names = None
        self.ms_skewed_string_list = None
        self.ms_skewed_string_list_values = None
        self.ms_skewed_col_value_loc_map = None
        self.ms_sort_cols = None
        self.ms_partitions = None
        self.ms_partition_params = None
        self.ms_partition_keys = None
        self.ms_partition_key_vals = None


def get_output_dir(output_dir_parent):
    if not output_dir_parent:
        raise ValueError('output path cannot be empty')
    if output_dir_parent[-1] != '/':
        output_dir_parent = output_dir_parent + '/'
    return '%s%s/' % (output_dir_parent, strftime('%Y-%m-%d-%H-%M-%S', localtime()))


def get_options(parser, args):
    parsed, extra = parser.parse_known_args(args[1:])
    print("Found arguments:", vars(parsed))
    if extra:
        print('Found unrecognized arguments:', extra)
    return vars(parsed)


def parse_arguments(args):
    parser = argparse.ArgumentParser(prog=args[0])
    parser.add_argument('-m', '--mode', required=True, choices=[FROM_METASTORE, TO_METASTORE], help='Choose to migrate metastore either from JDBC or from S3')
    parser.add_argument('-U', '--jdbc-url', required=True, help='Hive metastore JDBC url, example: jdbc:mysql://metastore.abcd.us-east-1.rds.amazonaws.com:3306')
    parser.add_argument('-u', '--jdbc-username', required=True, help='Hive metastore JDBC user name')
    parser.add_argument('-p', '--jdbc-password', required=True, help='Hive metastore JDBC password')
    parser.add_argument('-d', '--database-prefix', required=False, help='Optional prefix for database names in Glue DataCatalog')
    parser.add_argument('-t', '--table-prefix', required=False, help='Optional prefix for table name in Glue DataCatalog')
    parser.add_argument('-o', '--output-path', required=False, help='Output path, either local directory or S3 path')
    parser.add_argument('-i', '--input_path', required=False, help='Input path, either local directory or S3 path')

    options = get_options(parser, args)

    if options['mode'] == FROM_METASTORE:
        validate_options_in_mode(
            options=options, mode=FROM_METASTORE,
            required_options=['output_path'],
            not_allowed_options=['input_path']
        )
    elif options['mode'] == TO_METASTORE:
        validate_options_in_mode(
            options=options, mode=TO_METASTORE,
            required_options=['input_path'],
            not_allowed_options=['output_path']
        )
    else:
        raise AssertionError('unknown mode ' + options['mode'])

    return options


def get_spark_env():
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    sql_context = SQLContext(sc)
    return (conf, sc, sql_context)


def etl_from_metastore(sc, sql_context, db_prefix, table_prefix, hive_metastore, options):
    # extract
    hive_metastore.extract_metastore()

    # transform
    (databases, tables, partitions) = HiveMetastoreTransformer(sc, sql_context, db_prefix, table_prefix)\
        .transform(hive_metastore)

    # load
    output_path = get_output_dir(options['output_path'])

    coalesce_by_row_count(databases).write.format('json').mode('overwrite').save(output_path + 'databases')
    coalesce_by_row_count(tables).write.format('json').mode('overwrite').save(output_path + 'tables')
    coalesce_by_row_count(partitions).write.format('json').mode('overwrite').save(output_path + 'partitions')


def etl_to_metastore(sc, sql_context, hive_metastore, options):
    # extract
    input_path = options['input_path']

    databases = sql_context.read.json(path=input_path + 'databases', schema=METASTORE_DATABASE_SCHEMA)
    tables = sql_context.read.json(path=input_path + 'tables', schema=METASTORE_TABLE_SCHEMA)
    partitions = sql_context.read.json(path=input_path + 'partitions', schema=METASTORE_PARTITION_SCHEMA)

    # transform
    transform_databases_tables_partitions(sc, sql_context, hive_metastore, databases, tables, partitions)

    # load
    hive_metastore.export_to_metastore()


def transform_items_to_item(dc_databases, dc_tables, dc_partitions):
    databases = dc_databases.select('*', explode('items').alias('item')).drop('items')
    tables = dc_tables.select('*', explode('items').alias('item')).drop('items')
    partitions = dc_partitions.select('*', explode('items').alias('item')).drop('items')

    return (databases, tables, partitions)


def transform_databases_tables_partitions(sc, sql_context, hive_metastore, databases, tables, partitions):
    DataCatalogTransformer(sc, sql_context)\
        .transform(hms=hive_metastore, databases=databases, tables=tables, partitions=partitions)


def validate_options_in_mode(options, mode, required_options, not_allowed_options):
    for option in required_options:
        if options.get(option) is None:
            raise AssertionError('Option %s is required for mode %s' % (option, mode))
    for option in not_allowed_options:
        if options.get(option) is not None:
            raise AssertionError('Option %s is not allowed for mode %s' % (option, mode))


def validate_aws_regions(region):
    """
    To validate the region in the input. The region list below may be outdated as AWS and Glue expands, so it only
    create an error message if validation fails.
    If the migration destination is in a region other than Glue supported regions, the job will fail.
    :return: None
    """
    if region is None:
        return

    aws_glue_regions = [
        'ap-northeast-1' # Tokyo
        'eu-west-1', # Ireland
        'us-east-1', # North Virginia
        'us-east-2', # Ohio
        'us-west-2', # Oregon
    ]

    aws_regions = aws_glue_regions + [
        'ap-northeast-2', # Seoul
        'ap-south-1', # Mumbai
        'ap-southeast-1', # Singapore
        'ap-southeast-2', # Sydney
        'ca-central-1', # Montreal
        'cn-north-1', # Beijing
        'cn-northwest-1', # Ningxia
        'eu-central-1', # Frankfurt
        'eu-west-2', # London
        'sa-east-1', # Sao Paulo
        'us-gov-west-1', # GovCloud
        'us-west-1' # Northern California
    ]

    error_msg = "Invalid region: {0}, the job will fail if the destination is not in a Glue supported region".format(region)
    if region not in aws_regions:
        logging.error(error_msg)
    elif region not in aws_glue_regions:
        logging.warn(error_msg)



def main():
    options = parse_arguments(sys.argv)

    connection = {
        'url': options['jdbc_url'],
        'user': options['jdbc_username'],
        'password': options['jdbc_password']
    }
    db_prefix = options.get('database_prefix') or ''
    table_prefix = options.get('table_prefix') or ''

    # spark env
    (conf, sc, sql_context) = get_spark_env()
    # extract
    hive_metastore = HiveMetastore(connection, sql_context)

    if options['mode'] == FROM_METASTORE:
        etl_from_metastore(sc, sql_context, db_prefix, table_prefix, hive_metastore, options)
    else:
        etl_to_metastore(sc, sql_context, hive_metastore, options)


if __name__ == '__main__':
    main()
