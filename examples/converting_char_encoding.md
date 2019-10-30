# Converting character encoding in Glue ETL Job

Unicode has become the standard way in modern systems, however, a lot of customers who use **CJK (Chinese-Japanese-Korean)** characters are still struggling with handling those character codes. From the viewpoint of ETL, it is important to convert those characters into unicode.

This sample codes are tested with following conditions and it would work for other character encoding as well. We recommend you to test and modify the script based on your actual use-case, and data.

* Glue version: Glue 1.0 (Spark 2.4.3)
* Character encoding: 
    * Simplified Chinese: gb2312 (`gb2312`)
    * Traditional Chinese: Big5 (`big5`)
    * Japanese: Shift-JIS (`sjis`)
    * Korean: EUC-KR (`euc-kr`)

Below scripts use Shift-JIS just as an example of non-unicode encoding.

## S3 (CSV/Shift-JIS) to S3 (Parquet/UTF-8) by using Spark job

Currently Glue DynamicFrame supports custom encoding in XML, but not in other formats like JSON or CSV. In order to convert from **CJK specific character codes** into **UTF-8** in Glue ETL jobs in those formats, you would need to use **Apache Spark’s DataFrame** instead.

In this example, there are CSV files written in **Shift-JIS** on your S3 bucket. These input files are partitioned by year, month, and day like this.

* s3://path_to_input_prefix/year=2019/month=9/day=30/aaa.csv
* s3://path_to_input_prefix/year=2019/month=9/day=30/bbb.csv

If you want to write the data as Parquet format into Hive partition style location, you can run following sample code on Glue ETL jobs.

```
df = spark.read.csv('s3://path_to_input_prefix/', encoding='sjis')
df.write.partitionBy('year', 'month', 'day').parquet('s3://path_to_output_prefix/')
```

Spark writes data in UTF-8 by default, so you do not need to specify output encoding.

## S3 (JSON/UTF-8) to S3 (JSON/Shift-JIS) by using Spark job

You can also convert character code **from UTF-8 to Shift-JIS** as well. Here’s the sample for PySpark to handle JSON files.

```
df = spark.read.json('s3://path_to_input_prefix/')
df.write.partitionBy('year', 'month', 'day').option('encoding', 'sjis').json('s3://path_to_output_prefix/')
```

Note: Writing CSV/JSON files with specific character set is supported in **Glue 1.0 or later**.

## S3 (CSV/Shift-JIS) to S3 (CSV/UTF-8) by using Python Shell job

If input files are not so large and the number is small, you can use a Python Shell job instead of a Spark job. Here’s the sample code for Python Shell job.

```
import boto3

INPUT_BUCKET_NAME='bucket_name'
INPUT_FILE_PATH='path_to_input_file'
INPUT_FILE_ENCODING='shift_jis'

OUTPUT_BUCKET_NAME='bucket_name'
OUTPUT_FILE_PATH='path_to_output_file'

s3 = boto3.resource('s3')

input_object = s3.Object(INPUT_BUCKET_NAME, INPUT_FILE_PATH)
body = input_object.get()['Body'].read().decode(INPUT_FILE_ENCODING)

output_object = s3.Object(OUTPUT_BUCKET_NAME, OUTPUT_FILE_PATH)
output_object.put(Body = body)
```
