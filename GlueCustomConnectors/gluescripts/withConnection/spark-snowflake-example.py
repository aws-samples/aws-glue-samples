#  Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT-0

#
# This script outlines how to Extract data from a Glue Catalog Data Source, Transform it, and Load it into a Snowflake database using the Snowflake Spark Connector and JDBC driver
# It includes the ability to perform pre- and post-actions on the Snowflake database during the Load operation, including performing the pre-, load-, and post-action as a single database transaction
#
# The Snowflake JDBC Driver and Snowflake Spark Connector must be provided to the Glue Job in the "Dependent Jars Path"
# These jars should be sourced from the Maven Central Repository for "net.snowflake", uploaded to an S3 bucket, and referenced as a comma separated pair
# e.g. Glue Job "Dependent Jars Path" is "s3://bucket-name/snowflake-jdbc-3.13.2.jar,s3://bucket-name/spark-snowflake_2.11-2.8.5-spark_2.4.jar"
#
# It also expects that a JDBC connection is configured within Glue to store the target database Username and Password
# Note that this JDBC connection is not directly used by the script and does not need to be attached to the Glue Job
# This information could alternatively be stored in AWS Secrets Manager and referenced from there using boto3 libraries
#
# The script expects the following parameters to be supplied to the Glue Job:
# JOB_NAME - provided by Glue by default
# TempDir - provided by Glue based on the "Temporary Directory" job configuration
# glueCatalogDatabase - must be configured in the Glue Job "Job Parameters" with a Key of --glueCatalogDatabase and a Value of the source Glue Catalog Database
# glueCatalogTable - must be configured in the Glue Job "Job Parameters" with a Key of --glueCatalogTable and a Value of the source Glue Catalog Table## sfURL - must be configured in the Glue Job "Job Parameters" with a Key of --sfURL and a Value of the target Snowflake connection URL e.g. xxx00000.us-east-1.snowflakecomputing.com
# sfDatabase - must be configured in the Glue Job "Job Parameters" with a Key of --sfDatabase and a Value of the target Snowflake database name
# sfSchema - must be configured in the Glue Job "Job Parameters" with a Key of --sfSchema and a Value of the target Snowflake schema name
# sfTable - must be configured in the Glue Job "Job Parameters" with a Key of --sfTable and a Value of the target Snowflake table name
# sfJDBCConnectionName - must be configured in the Glue Job "Job Parameters" with a Key of --sfJDBCConnectionName and a Value of the Glue JDBC connection configured with the Snowflake username/password
#


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# @params: [TempDir, JOB_NAME, glueCatalogDatabase, glueCatalogTable, sfURL, sfDatabase, sfSchema, sfTable, sfJDBCConnectionName]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME','glueCatalogDatabase','glueCatalogTable','sfURL','sfDatabase','sfSchema','sfTable','sfJDBCConnectionName'])

# Initialise the Glue Job
job.init(args['JOB_NAME'], args)

# Create a DynamicFrame based on the Glue Catalog source (could be replaced with any other DynamicFrame or DataFrame generating source based on other examples)
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args['glueCatalogDatabase'], table_name = args['glueCatalogTable'], transformation_ctx = "datasource0")

#
# Perform any required Transform logic using Glue Spark - see other scripts or auto-generated Glue jobs for examples
#

# Convert DynamicFrame to DataFrame in preparation to write to the database target.  
# Note that this may require resolving the choice types in DynamicFrames before conversion.
# Refer to Section 2 of the "FAQ_and_How_to.md" in the root of this Git Repository for further guidance.
dataframe = datasource0.toDF()

# Extract database username / password from JDBC connection (or AWS Secrets Manager - see note above)
jdbc_conf = glueContext.extract_jdbc_conf("JDBC_CONNECTION_NAME")

# Configure the rest of the Snowflake database connection options based on a combination of the JDBC connection detail and Job Parameters
sfOptions = {
    "sfURL": args['sfURL'],
    "sfUser": jdbc_conf.get('user'),
    "sfPassword": jdbc_conf.get('password'),
    "sfDatabase": args['sfDatabase'],
    "sfSchema": args['sfSchema']
}
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

# Define pre- and post-actions for database transaction.  These should be a semi-colon separated string of SQL statements
# These can include any relevant SQL and/or call Stored Procedures within the operating logic of the target database
preactions = "BEGIN TRANSACTION;"
postactions = "COMMIT WORK;"

# Call Write method on DataFrame to copy data to database, referencing pre- and post-actions
dataframe.write \
    .format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .option("dbtable", args['sfTable']) \
    .option("preactions", preactions) \
    .option("postactions", postactions) \
    .mode("append") \
    .save()

# Commit Job
job.commit()