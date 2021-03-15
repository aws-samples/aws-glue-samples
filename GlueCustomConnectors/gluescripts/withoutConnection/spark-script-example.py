#  Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT-0
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)

# Please update the values in the options to connect to your own data source
options = {
    "sfDatabase":"snowflake_sample_data",
    "sfSchema":"PUBLIC",
    "sfWarehouse" : "WORKSHOP_123",
    "dbtable" : "lineitem", 
    "secretId" : "my-secret", # optional
    "className" : "net.snowflake.spark.snowflake"
    }

datasource = glueContext.create_dynamic_frame_from_options(
    connection_type = "custom.spark", # for marketplace workflow, use marketplace.spark
    connection_options = options,
    transformation_ctx = "datasource")
datasource.show()

## Write to data target
glueContext.write_dynamic_frame.from_options(frame = datasource,
                                             connection_type = "custom.spark",
                                             connection_options = options)
job.commit()