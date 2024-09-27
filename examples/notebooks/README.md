# AWS Glue Notebook Samples
This repository has sample iPython notebook files which show you how to use open data dake formats; Apache Hudi, Delta Lake, and Apache Iceberg on AWS Glue Interactive Sessions and AWS Glue Studio Notebook.

## Native connector based Examples
Following examples are based on native connectors.

### Apache Hudi
 - [Hudi DataFrame example](native_hudi_dataframe.ipynb)
 - [Hudi Spark SQL example](native_hudi_sql.ipynb)
 - [Hudi to Redshift incremental load](hudi2redshift-incremental-load.ipynba)

### Delta Lake
 - [Delta Lake DataFrame example](native_delta_dataframe.ipynb)
 - [Delta Lake Spark SQL example](native_delta_sql.ipynb)
 - [Delta Lake Spark SQL example using S3 path](native_delta_sql_s3path.ipynb)
 - [Delta to Snowflake incremental load](delta2snowflake-incremental-load.ipynb)

### Apache Iceberg
 - [Iceberg DataFrame example](native_iceberg_dataframe.ipynb)
 - [Iceberg Spark SQL example](native_iceberg_sql.ipynb)


## Custom/Marketplace connector based Examples
Following examples are based on custom/marketplace connectors.
See details in [Process Apache Hudi, Delta Lake, Apache Iceberg datasets at scale, part 1: AWS Glue Studio Notebook](https://aws.amazon.com/blogs/big-data/part-1-integrate-apache-hudi-delta-lake-apache-iceberg-datasets-at-scale-aws-glue-studio-notebook/).

### Apache Hudi
 - [Hudi DataFrame example](hudi_dataframe.ipynb)
 - [Hudi Spark SQL example](hudi_sql.ipynb)

### Delta Lake
 - [Delta Lake DataFrame example](delta_dataframe.ipynb)
 - [Delta Lake Spark SQL example](delta_sql.ipynb)
 - [Delta Lake Spark SQL example using S3 path](delta_sql_s3path.ipynb)

### Apache Iceberg
 - [Iceberg DataFrame example](iceberg_dataframe.ipynb)
 - [Iceberg Spark SQL example](iceberg_sql.ipynb)

## Other Examples
 - [Building RAG using AWS Glue for Apache Spark](langchain-spark-rag-jumpstart.ipynb)
