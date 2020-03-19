# Exploring the `resolveChoice` Method

The [Data Cleaning](data_cleaning_and_lambda.md) sample 
gives a taste of how useful AWS Glue's resolve-choice capability
can be. This example expands on that and explores each of the
strategies that the DynamicFrame's `resolveChoice` method offers.

The associated Python file in the examples folder is: [resolve_choice.py](resolve_choice.py)

A Scala version of the script corresponding to this example can be found in the file: [ResolveChoice.scala](ResolveChoice.scala)

We set up for this example in the same way we did for the data-cleaning
sample.

### 1. Crawl our sample dataset

Again, the dataset used in this example is Medicare-Provider payment data downloaded from two
Data.CMS.gov sites: [Inpatient Prospective Payment System Provider Summary for the Top 100 Diagnosis-Related Groups - FY2011](https://data.cms.gov/Medicare-Inpatient/Inpatient-Prospective-Payment-System-IPPS-Provider/97k6-zzx3), and
[Inpatient Charge Data FY 2011](https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Inpatient2011.html).
After downloading it, we modified the data to introduce a couple of erroneous records at the tail end of the file.
This modified file can be found in:

    s3://awsglue-datasets/examples/medicare/Medicare_Hospital_Provider.csv

The first step is to crawl this data and put the results into a database called `payments`
in your Data Catalog, as described [here in the Developer Guide](http://docs.aws.amazon.com/glue/latest/dg/console-crawlers.html).
The crawler will read the first 2 MB of data from that file and create one table, `medicare`,
in the `payments` datebase in the Data Catalog.


### 2. Spin up a DevEndpoint to work with

The easiest way to debug pySpark ETL scripts is to create a `DevEndpoint'
and run your code there.  You can do this in the AWS Glue console, as described
[here in the Developer Guide](http://docs.aws.amazon.com/glue/latest/dg/tutorial-development-endpoint-notebook.html).


### 3. Getting started

Begin by pasting some boilerplate into the DevEndpoint notebook to import the
AWS Glue libraries we'll need and set up a single `GlueContext`. We also initialize
the spark session variable for executing Spark SQL queries later in this script.

    import sys
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.job import Job

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

### 4. Revisiting the `string`-`int` choice type in the data

Now, let's look at the schema after we load all the data into a DynamicFrame,
starting from the metadata that the crawler put in the AWS Glue Data Catalog:

    medicare_dyf = glueContext.create_dynamic_frame.from_catalog(
           database = "payments",
           table_name = "medicare")
    medicare_dyf.printSchema()

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

The DynamicFrame generated a schema in which `provider id` could be either a `long`
or a 'string', whereas the DataFrame schema listed `Provider Id` as being a `string`.
Which one is right? Well, it turns out there are two records (out of 160K records)
at the end of the file with strings in that column (these are the erroneous records
that we introduced to illustrate our point).

As we saw in the [Data Cleaning](data_cleaning_and_lambda.md) sample, DynamicFrames
has the notion of a `choice` type. In this case, both `long` and `string` may appear
in the 'provider id' column. The AWS Glue crawler missed the `string` type
because it only considered a 2MB prefix of the data. The Spark DataFrame considered the
whole dataset, but was forced to assign the most general type to the column (`string`).
In fact, Spark often resorts to the most general case when there are complex types or
variations with which it is unfamiliar.

Below, we'll try each of the four different strategies for resolving choice types
that the DynamicFrame `resolveChoice` method offers, namely:

 - `cast`
 - `project`
 - `make_cols`
 - `make_struct`

#### The `cast` option
Using the DynamicFrames `resolveChoice` method, we can eliminate the `string` values
with the `cast:long` option:

    med_resolve_cast = medicare_dyf.resolveChoice(specs = [('provider id','cast:long')])
    med_resolve_cast.printSchema()

This replaces the string values with `null` values, and the output of the `printSchema`
call is now:

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


#### The `project` option
We could also project the long values and discard the string values using the
`project:long` option:

    med_resolve_project = medicare_dyf.resolveChoice(specs = [('provider id','project:long')])
    med_resolve_project.printSchema()
  
Again, the schema now shows that the 'provider id' column only contains long values:

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

#### The `make_cols` option
Another possible strategy is to use `make_cols`, which splits the choice
column into two, one for each type. The new column names are composed of
the original column name with the type name appended following an underscore.
Here, `provider id` is replaced by the two new columns `provider id_long`
and `provider id_string`:

    med_resolve_make_cols = medicare_dyf.resolveChoice(specs = [('provider id','make_cols')])
    med_resolve_make_cols.printSchema()

The resulting schema is:

    root
    |-- drg definition: string
    |-- provider id_long: long
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
    |-- provider id_string: string

  
#### The `make_struct` option
Using `make_struct` turns the choice column's type into a struct
that contains each of the choice types separately:

    med_resolve_make_struct = medicare_dyf.resolveChoice(specs = [('provider id','make_struct')])
    med_resolve_make_struct.printSchema()

The resulting schema is:

    root
    |-- drg definition: string
    |-- provider id: struct
    |    |-- long: long
    |    |-- string: string
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

### 4. Executing Spark SQL on a Spark DataFrame

Finally, let's execute some SQL queries. For this, we can convert the
DynamicFrame into a Spark DataFrame using the toDF method.

    medicare_df = medicare_dyf.toDF()

We can then create a temporary view of the table and execute a SQL
query to select all records with 'total discharges' > 30.

    medicare_df.createOrReplaceTempView("medicareTable")
    medicare_sql_df = spark.sql("SELECT * FROM medicareTable WHERE `total discharges` > 30")

We can also convert the resulting DataFrame back to a Dynamic Frame
using the fromDF method.

    medicare_sql_dyf = DynamicFrame.fromDF(medicare_sql_df, glueContext, "medicare_sql_dyf")

