# Data cleaning with AWS Glue
**Using ResolveChoice, lambda, and ApplyMapping**

AWS Glue's dynamic data frames are powerful. They provide a more precise representation
of the underlying semi-structured data, especially when dealing with columns or fields
with varying types. They also provide powerful primitives to deal with nesting and unnesting.

This example shows how to process CSV files that have unexpected variations in them
and convert them into nested and structured Parquet for fast analysis.

The associated Python file in the examples folder is:

    data_cleaning_and_lambda.py

### 1. Crawl our sample dataset

The dataset used in this example consists of Medicare-Provider payment data downloaded from two
Data.CMS.gov sites: [Inpatient Prospective Payment System Provider Summary for the Top 100 Diagnosis-Related Groups - FY2011](https://data.cms.gov/Medicare-Inpatient/Inpatient-Prospective-Payment-System-IPPS-Provider/97k6-zzx3), and
[Inpatient Charge Data FY 2011](https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Inpatient2011.html).
After downloading it, we modified the data to introduce a couple of erroneous records at the tail end of the file.
This modified file can be found in:

    s3://awsglue-datasets/examples/medicare/Medicare_Hospital_Provider.csv

The first step is to crawl this data and put the results into a database called `payments`
in your Data Catalog, as described [here in the Developer Guide](http://docs.aws.amazon.com/glue/latest/dg/console-crawlers.html).
The crawler will read the first 2 MB of data from that file and create one table, `medicare`,
in the `payments` database in the AWS Glue Data Catalog.

The schema for the `medicare` table in the AWS Glue Data Catalog is as follows:

```
Column  name                            Data type
==================================================
drg definition                          string
provider id                             bigint
provider name                           string
provider street address                 string
provider city                           string
provider state                          string
provider zip code                       bigint
hospital referral region description    string
total discharges                        bigint
average covered charges                 string
average total payments                  string
average medicare payments               string
```

### 2. Spin up a DevEndpoint and notebook to work with

An easy way to debug your pySpark ETL scripts is to create a `DevEndpoint', spin up and attach a Zeppelin notebook server to
the endpoint, and edit and refine the scripts in the notebook. You can set this up through the AWS Glue console, as described 
[here in the Developer Guide](http://docs.aws.amazon.com/glue/latest/dg/tutorial-development-endpoint-notebook.html)

### 3. Getting started

Begin by pasting some boilerplate into the DevEndpoint notebook to import the
AWS Glue libraries we'll need and set up a single `GlueContext`.

    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.sql import SparkSession

    glueContext = GlueContext(SparkContext.getOrCreate())


### 4. Data-type variations

First, let's see what the schema looks like using Spark DataFrames:

    medicare = spark.read.format(
       "com.databricks.spark.csv").option(
       "header", "true").option(
       "inferSchema", "true").load(
       's3://awsglue-datasets/examples/medicare/Medicare_Hospital_Provider.csv')
    medicare.printSchema()

The output from `printSchema` is:

    root
     |-- DRG Definition: string (nullable = true)
     |-- Provider Id: string (nullable = true)
     |-- Provider Name: string (nullable = true)
     |-- Provider Street Address: string (nullable = true)
     |-- Provider City: string (nullable = true)
     |-- Provider State: string (nullable = true)
     |-- Provider Zip Code: integer (nullable = true)
     |-- Hospital Referral Region Description: string (nullable = true)
     |--  Total Discharges : integer (nullable = true)
     |--  Average Covered Charges : string (nullable = true)
     |--  Average Total Payments : string (nullable = true)
     |-- Average Medicare Payments: string (nullable = true)

Now, let's look at the schema that a DynamicFrame generates:

    medicare_dynamicframe = glueContext.create_dynamic_frame.from_catalog(
           database = "payments",
           table_name = "medicare")
    medicare_dynamicframe.printSchema()

The output from `printSchema` this time is:

    root
    |-- drg definition: string
    |-- provider id: choice
    |    |-- long
    |    |-- string
    |-- provider name: string
    |-- provider street address: string
    |-- provider city: string
    |-- provider state: string
    |-- provider zip code: long
    |-- hospital referral region description: string
    |-- total discharges: long
    |-- average covered charges: string
    |-- average total payments: string
    |-- average medicare payments: string

The DynamicFrame generates a schema in which `provider id` could be either a `long`
or a `string`, whereas the DataFrame schema lists `Provider Id` as being a `string`,
and the Data Catalog lists `provider id` as being a `bigint`.

Which one is right? Well, it turns out there are two records (out of 160K records)
at the end of the file with `string`s in that column (these are the erroneous records
that we introduced to illustrate our point).

DynamicFrames introduce the notion of a `choice` type. In this case, it shows that both
`long` and `string` may appear in that column. The AWS Glue crawler misses the `string`
because it only considers a 2MB prefix of the data. The Spark DataFrame considers the
whole dataset, but is forced to assign the most general type to the column (`string`).
In fact, Spark often resorts to the most general case when there are complex types or
variations with which it is unfamiliar.

To query the `provider id` column, we first need to resolve the choice. With DynamicFrames,
we can try to convert those `string` values to `long` values uaing the `resolveChoice`
transform method with a `cast:long` option:

    medicare_res = medicare_dynamicframe.resolveChoice(specs = [('provider id','cast:long')])
    medicare_res.printSchema()

The output of the `printSchema` call is now:

    root
    |-- drg definition: string
    |-- provider id: long
    |-- provider name: string
    |-- provider street address: string
    |-- provider city: string
    |-- provider state: string
    |-- provider zip code: long
    |-- hospital referral region description: string
    |-- total discharges: long
    |-- average covered charges: string
    |-- average total payments: string
    |-- average medicare payments: string

Where the value was a `string` that cannot be cast, AWS Glue inserts a `null`.

Another option is to convert the `choice` type to a `struct`, which keeps both types.

Let's take a look at the rows that were anomalous:

    medicare_res.toDF().where("`provider id` is NULL").show()

What we see is:

    +--------------------+-----------+---------------+-----------------------+-------------+--------------+-----------------+------------------------------------+----------------+-----------------------+----------------------+-------------------------+
    |      drg definition|provider id|  provider name|provider street address|provider city|provider state|provider zip code|hospital referral region description|total discharges|average covered charges|average total payments|average medicare payments|
    +--------------------+-----------+---------------+-----------------------+-------------+--------------+-----------------+------------------------------------+----------------+-----------------------+----------------------+-------------------------+
    |948 - SIGNS & SYM...|       null|            INC|       1050 DIVISION ST|      MAUSTON|            WI|            53948|                        WI - Madison|              12|              $11961.41|              $4619.00|                 $3775.33|
    |948 - SIGNS & SYM...|       null| INC- ST JOSEPH|     5000 W CHAMBERS ST|    MILWAUKEE|            WI|            53210|                      WI - Milwaukee|              14|              $10514.28|              $5562.50|                 $4522.78|
    +--------------------+-----------+---------------+-----------------------+-------------+--------------+-----------------+------------------------------------+----------------+-----------------------+----------------------+-------------------------+

Let's remove those malformed records now:

    medicare_dataframe = medicare_res.toDF()
    medicare_dataframe = medicare_dataframe.where("`provider id` is NOT NULL")


### 5. Lambda functions (aka Python UDFs) and ApplyMapping

Although AWS Glue DynamicFrames do not yet support lambda functions, also known
as user-defined functions, you can always convert a DynamicFrame to and from a
Spark DataFrame to take advantage of Spark functionality as well as the special
features of DynamicFrames.

Let's turn the payment information into numbers, so analytic engines like Amazon
Redshift or Amazon Athena can do their number crunching faster:

    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    chop_f = udf(lambda x: x[1:], StringType())
    medicare_dataframe = medicare_dataframe.withColumn(
            "ACC", chop_f(
                medicare_dataframe["average covered charges"])).withColumn(
                    "ATP", chop_f(
                        medicare_dataframe["average total payments"])).withColumn(
                            "AMP", chop_f(
                                medicare_dataframe["average medicare payments"]))
    medicare_dataframe.select(['ACC', 'ATP', 'AMP']).show()

The output from the `show` call is:

    +--------+-------+-------+
    |     ACC|    ATP|    AMP|
    +--------+-------+-------+
    |32963.07|5777.24|4763.73|
    |15131.85|5787.57|4976.71|
    |37560.37|5434.95|4453.79|
    |13998.28|5417.56|4129.16|
    |31633.27|5658.33|4851.44|
    |16920.79|6653.80|5374.14|
    |11977.13|5834.74|4761.41|
    |35841.09|8031.12|5858.50|
    |28523.39|6113.38|5228.40|
    |75233.38|5541.05|4386.94|
    |67327.92|5461.57|4493.57|
    |39607.28|5356.28|4408.20|
    |22862.23|5374.65|4186.02|
    |31110.85|5366.23|4376.23|
    |25411.33|5282.93|4383.73|
    | 9234.51|5676.55|4509.11|
    |15895.85|5930.11|3972.85|
    |19721.16|6192.54|5179.38|
    |10710.88|4968.00|3898.88|
    |51343.75|5996.00|4962.45|
    +--------+-------+-------+
    only showing top 20 rows

These are all still strings in the data. We can use the DynamicFrame's powerful
`apply_mapping` tranform method to drop, rename, cast, and nest
the data so that other data programming langages and sytems can
easily access it:

    medicare_tmp_dyf = DynamicFrame.fromDF(medicare_dataframe, glueContext, "nested")
    medicare_nest_dyf = medicare_tmp_dyf.apply_mapping([('drg definition', 'string', 'drg', 'string'),
                     ('provider id', 'long', 'provider.id', 'long'),
                     ('provider name', 'string', 'provider.name', 'string'),
                     ('provider city', 'string', 'provider.city', 'string'),
                     ('provider state', 'string', 'provider.state', 'string'),
                     ('provider zip code', 'long', 'provider.zip', 'long'),
                     ('hospital referral region description', 'string','rr', 'string'),
                     ('ACC', 'string', 'charges.covered', 'double'),
                     ('ATP', 'string', 'charges.total_pay', 'double'),
                     ('AMP', 'string', 'charges.medicare_pay', 'double')])
    medicare_nest_dyf.printSchema()

The `printSchema` output is:

    root
    |-- drg: string
    |-- provider: struct
    |    |-- id: long
    |    |-- name: string
    |    |-- city: string
    |    |-- state: string
    |    |-- zip: long
    |-- rr: string
    |-- charges: struct
    |    |-- covered: double
    |    |-- total_pay: double
    |    |-- medicare_pay: double

Turning the data back to DataFrame, we can show what it now looks like:

    medicare_nest_dyf.toDF().show()

The output is:

    +--------------------+--------------------+---------------+--------------------+
    |                 drg|            provider|             rr|             charges|
    +--------------------+--------------------+---------------+--------------------+
    |039 - EXTRACRANIA...|[10001,SOUTHEAST ...|    AL - Dothan|[32963.07,5777.24...|
    |039 - EXTRACRANIA...|[10005,MARSHALL M...|AL - Birmingham|[15131.85,5787.57...|
    |039 - EXTRACRANIA...|[10006,ELIZA COFF...|AL - Birmingham|[37560.37,5434.95...|
    |039 - EXTRACRANIA...|[10011,ST VINCENT...|AL - Birmingham|[13998.28,5417.56...|
    |039 - EXTRACRANIA...|[10016,SHELBY BAP...|AL - Birmingham|[31633.27,5658.33...|
    |039 - EXTRACRANIA...|[10023,BAPTIST ME...|AL - Montgomery|[16920.79,6653.8,...|
    |039 - EXTRACRANIA...|[10029,EAST ALABA...|AL - Birmingham|[11977.13,5834.74...|
    |039 - EXTRACRANIA...|[10033,UNIVERSITY...|AL - Birmingham|[35841.09,8031.12...|
    |039 - EXTRACRANIA...|[10039,HUNTSVILLE...|AL - Huntsville|[28523.39,6113.38...|
    |039 - EXTRACRANIA...|[10040,GADSDEN RE...|AL - Birmingham|[75233.38,5541.05...|
    |039 - EXTRACRANIA...|[10046,RIVERVIEW ...|AL - Birmingham|[67327.92,5461.57...|
    |039 - EXTRACRANIA...|[10055,FLOWERS HO...|    AL - Dothan|[39607.28,5356.28...|
    |039 - EXTRACRANIA...|[10056,ST VINCENT...|AL - Birmingham|[22862.23,5374.65...|
    |039 - EXTRACRANIA...|[10078,NORTHEAST ...|AL - Birmingham|[31110.85,5366.23...|
    |039 - EXTRACRANIA...|[10083,SOUTH BALD...|    AL - Mobile|[25411.33,5282.93...|
    |039 - EXTRACRANIA...|[10085,DECATUR GE...|AL - Huntsville|[9234.51,5676.55,...|
    |039 - EXTRACRANIA...|[10090,PROVIDENCE...|    AL - Mobile|[15895.85,5930.11...|
    |039 - EXTRACRANIA...|[10092,D C H REGI...|AL - Tuscaloosa|[19721.16,6192.54...|
    |039 - EXTRACRANIA...|[10100,THOMAS HOS...|    AL - Mobile|[10710.88,4968.0,...|
    |039 - EXTRACRANIA...|[10103,BAPTIST ME...|AL - Birmingham|[51343.75,5996.0,...|
    +--------------------+--------------------+---------------+--------------------+
    only showing top 20 rows

Finally, let's write the data out in an optimized Parquet format for Redshift Spectrum or Athena:

    glueContext.write_dynamic_frame.from_options(
           frame = medicare_nest_dyf,
           connection_type = "s3",
           connection_options = {"path": "s3://glue-sample-target/output-dir/medicare_parquet"},
           format = "parquet")




