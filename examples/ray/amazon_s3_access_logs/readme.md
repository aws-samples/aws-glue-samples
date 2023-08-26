# Converting Amazon S3 Access Logs to Parquet with Glue for Ray

This script uses AWS Glue for Ray to efficiently read Amazon S3 access logs, clean them and write out partitioned parquet. It distributes the object listing, reading, cleaning and writing of the data while minimizing movement in memory.

## Arguments

The script requires the user to provide parameters (which are passed in as environment variables) in their Glue job configuration. The required parameters are:

`--source-s3-uri` which is the S3 URI of the prefix containing the raw access logs: i.e. `s3://my-bucket/access_logs/`
`--dest-s3-uri` which is the S3 URI of the prefix you'd like the parquet logs written to i.e.`s3://my-output/access_logs/`
`--checkpoint-uri` which is the full S3 URI of the object (file) to store the timestamp of the last successful run providing
basic bookmark functionality.

The script has the following optional arguments:
`--log-object-prefix` This is required if the object (file) name of your Amazon S3 access logs have a prefix pattern before the date stamp.
`--list-bucket-size` Sets the group size for the distributed list operation by defining the prefix granularity. HOURLY will create the most tasks but fewer objects per task while YEARLY is the opposite. Acceptable values are `HOURLY, DAILY, MONTHLY, and YEARLY`. The default value is `DAILY` and is sufficient for most use cases.
  
Ray data requires the schema to be specified in the script and was taken from [here](https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html#log-record-fields). The script should work for modern Amazon S3 Access Logs but may need to be modified for older schema variations. The schema can be adjusted on lines 239 - 299 in the lists `col_names` and `column_types`.
