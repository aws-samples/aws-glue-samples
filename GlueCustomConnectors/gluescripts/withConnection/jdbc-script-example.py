#  Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT-0
import sys
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
# Please update the values in the options to connect to your own data source
options = {
    "dbTable":"Account",
    "partitionColumn":"RecordId__c",
    "lowerBound" : "0",
    "upperBound" : "13",
    "numPartitions" : "2",
    "connectionName" : "my-connection", # please refer to Glue Studio Create Custom Connector doc to create a connection
    "dataTypeMapping" : {"FLOAT" : "STRING"}
    }
datasource = glueContext.create_dynamic_frame_from_options(
    connection_type = "custom.jdbc", # for marketplace workflow, use marketplace.jdbc
    connection_options = options,
    transformation_ctx = "datasource")
datasource.show()

# Please update the values in the options to connect to your own data source
sink_options = {
    "dbTable":"Account",
    "connectionName" : "my-connection" # please refer to Glue Studio Create Custom Connector doc to create a connection
}

## Write to data target
glueContext.write_dynamic_frame.from_options(frame = datasource,
                                             connection_type = "custom.jdbc",
                                             connection_options = sink_options)

job.commit()