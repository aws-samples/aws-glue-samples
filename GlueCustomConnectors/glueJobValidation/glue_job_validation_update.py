#  Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT-0
import sys
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SparkConf

from awsglue.dynamicframe import DynamicFrame
from awsglue.gluetypes import Field, IntegerType, TimestampType, StructType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

######################################## test connection options ########################################
## please pick up and customize the right connection options for testing
## If you are using a large testing data set, please consider using column partitioning to parallel the data reading for performance purpose.

# DataSourceTest - - please configure according to your connector type and options
options_dataSourceTest_jdbc = {
    "query": "select NumberOfEmployees, CreatedDate from Account",
    "className" : "partner.jdbc.some.Driver",

    # test parameters
    "url": "jdbc:some:url:SecurityToken=abc;",
    "user": "user",
    "password": "password",
}

# ColumnPartitioningTest
# for JDBC connector only
options_columnPartitioningTest = {
    "query": "select NumberOfEmployees, CreatedDate from Account where ",
    "url": "jdbc:some:url:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
    "secretId": "test-partner-driver",
    "className" : "partner.jdbc.some.Driver",

    # test parameters
    "partitionColumn" : "RecordId__c",
    "lowerBound" : "0",
    "upperBound" : "13",
    "numPartitions" : "2",
}

# DataTypeMappingTest
# for JDBC connector only
options_dataTypeMappingTest = {
    "query" : "select NumberOfEmployees, CreatedDate from Account where ",
    "url" : "jdbc:some:url:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
    "secretId" : "test-partner-driver",
    "className" : "partner.jdbc.some.Driver",

    # test parameter
    "dataTypeMapping": {"INTEGER" : "STRING"}

}

# DbtableQueryTest
# for JDBC connector only
options_dbtableQueryTest = {
    "url" : "jdbc:some:url:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
    "secretId" : "test-partner-driver",
    "className" : "partner.jdbc.some.Driver",

    # test parameter
    "query": "select NumberOfEmployees, CreatedDate from Account"
    # "dbTable" : "Account"
}

# JDBCUrlTest - extra jdbc connections UseBulkAPI appended
# for JDBC connector only
options_JDBCUrlTest = {
    "query": "select NumberOfEmployees, CreatedDate from Account",
    "secretId": "test-partner-driver",
    "className" : "partner.jdbc.some.Driver",

    # test parameter
    "url": "jdbc:some:url:user=${user};Password=${Password};SecurityToken=${SecurityToken};UseBulkAPI=true",
}

# SecretsManagerTest - - please configure according to your connector type and options
options_secretsManagerTest = {
    "query": "select NumberOfEmployees, CreatedDate from Account",
    "url": "jdbc:some:url:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
    "className" : "partner.jdbc.some.Driver",

    # test parameter
    "secretId": "test-partner-driver"
}

# FilterPredicateTest
# for JDBC connector only
options_filterPredicateTest = {
    "query": "select NumberOfEmployees, CreatedDate from Account where",
    "url": "jdbc:some:url:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
    "secretId": "test-partner-driver",
    "className" : "partner.jdbc.some.Driver",

    # test parameter
    "filterPredicate": "BillingState='CA'"
}

##################################### read data from data source ######################################
datasource0 = glueContext.create_dynamic_frame_from_options(
    connection_type = "marketplace.jdbc",
    connection_options = options_secretsManagerTest) # pick up the right test conection options

######################################## validate data reading ########################################

## validate data schema and count
# more data type: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-types.html
expected_schema = StructType([Field("NumberOfEmployees", IntegerType()), Field("CreatedDate", TimestampType())])
expected_count = 2

assert datasource0.schema() == expected_schema

print("expected schema: " + str(expected_schema.jsonValue()))
print("result schema: " + str(datasource0.schema().jsonValue()))
print("result schema in tree structure: ")
datasource0.printSchema()

## validate data count is euqal to expected count
assert datasource0.count() == expected_count

print("expected record count: " + str(expected_count))
print("result record count: " + str(datasource0.count()))

######################################## write data to s3 ########################################
datasource0.write(
    connection_type="s3",
    connection_options = {"path": "s3://your/output/path/"},
    format="json"
)

######################################## DataSinkTest ########################################


## Create a DynamicFrame on the fly
jsonStrings = ['{"Name":"Andrew"}']
rdd = sc.parallelize(jsonStrings)
sql_df = spark.read.json(rdd)
df = DynamicFrame.fromDF(sql_df, glueContext, "new_dynamic_frame")

## DataSinkTest options
options_dataSinkTest = {
    "secretId": "test-partner-driver",
    "dbtable" : "Account",
    "className" : "partner.jdbc.some.Driver",
    "url": "jdbc:some:url:user=${user};Password=${Password};SecurityToken=${SecurityToken};"
}

## Write to data target
glueContext.write_dynamic_frame.from_options(frame = df,
                                             connection_type = "marketplace.jdbc",
                                             connection_options = options_dataSinkTest)

## write validation
# You may check data in the database side.
# You may also refer to 'read data from data source' and 'validate data reading' part to compose your own validation logics.

job.commit()