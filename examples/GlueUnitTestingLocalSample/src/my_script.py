import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


s3data_dynf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://somebucket/somepath"]},
    format="csv"
)

output_path = "s3://outputbucket/datapath"
glueContext.purge_s3_path(output_path, {"retentionPeriod": 0})
glueContext.write_dynamic_frame.from_options(
    frame=s3data_dynf,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": output_path,
    },
    format_options={
        "useGlueParquetWriter": True,
    },
)

customer_df = glueContext.create_dynamic_frame.from_catalog(
    database="corey-reporting-db",
    table_name="cust-corey_customer",
    transformation_ctx="dynamoDbConnection_node1",
)

customer_df = ApplyMapping.apply(frame=customer_df, mappings=[
    ("customerId", "string", "customerId", "int"),
    ("firstname", "string", "firstname", "string"),
    ("lastname", "string", "lastname", "string")
], transformation_ctx = "customerMapping")

customerSink = glueContext.write_dynamic_frame.from_options(
    frame=customer_df,
    connection_type='postgresql',
    connection_options={
        "url": "jdbc:postgresql://********.us-east-1.rds.amazonaws.com:5432/corey_reporting",
        "dbtable": "poc.customer",
        "user": "postgres",
        "password": "********"

    },
    transformation_ctx="customerSink"
)

job.commit()
