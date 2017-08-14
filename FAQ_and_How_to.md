# AWS Glue FAQ, or How to Get Things Done

### 1. How do I repartition or coalesce my output into more or fewer files?

AWS Glue is based on Apache Spark, which partitions data across multiple nodes
to achieve high throughput. When writing data to a file-based sink like
Amazon S3, Glue will write a separate file for each partition. In some cases
it may be desirable to change the number of partitions, either to change the
degree of parallelism or the number of output files.

To change the number of partitions in a DynamicFrame, you can first convert
it into a DataFrame and then leverage Apache Spark's partitioning capabilities.
For example, the first line of the following snippet converts the DynamicFrame
called "datasource0" to a DataFrame and then repartitions it to a single
partition. The second line converts it back to a DynamicFrame for further
processing in AWS Glue.

    # Convert to a dataframe and partition based on "partition_col"
    partitioned_dataframe = datasource0.toDF().repartition(1)

    # Convert back to a DynamicFrame for further processing.
    partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")

You can also pass the name of a column to the repartition method to use that field as
a partitioning key. This may help performance in certain cases where there is benefit
in co-locating data. Note that while different records with the same value for this
column will be assigned to the same partition, there is no guarantee that there will
be a separate partition for each distinct value.


### 2. I have a ChoiceType in my schema. So...

a. **How can I convert to a data frame?**

  Since DataFrames do not have the type flexibility that DynamicFrames do, you have
  to resolve the choice type in your DynamicFrame before conversion. Glue provides a
  transformation called
  [ResolveChoice](http:docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-transforms-ResolveChoice.html)
  with the following signature:

     ResolveChoice.apply(self, frame, specs = None, choice = "",
                         transformation_ctx = "", info = "",
                         stageThreshold = 0, totalThreshold = 0)

  This transformation provides you two general ways to resolve choice types in a DynamicFrame.

  * You can specify a list of (path, action) tuples for each individual choice column,
    where *path* is the full path of the column and *action* is the strategy to resolve
    the choice in this column.

  * You can give an action for *all* the potential choice columns in your data using the
    *choice* parameter.

  The *action* above is a string, one of four strategies that AWS Glue provides:

  1. `cast` - When this is specified, the user must specify a type to cast to, such
     as `cast:int`.

  2. `make_cols` This flattens a potential choice.  For instance, if `col1` is
     `choice<int, string>`, then using `make_cols` creates two columns in the target:
     `col1_int` and `col1_string`.

  3. `make_struct` This creates a struct containing both choices. For example, if `col1`
      is `choice<int, string>`, then using `make_struct` creates a column called
      `struct<int, string>` that contains one or the other of the choice types.

  4. `project` When this is specified, the user must also specify a type.
     For instance, when `project:string` is  specified for `col1` that is
     `choice<int, string>`, then the column produced in the target would
     only contain `string` values.

   Once all the choice types in your DynamicFrame are resolved, you can convert it
   to a data frame using the 'toDF()' method.

b. **How do I write to targets that do not handle ChoiceTypes?**

  Resolve the choice types as described above and then write the data out using
  DynamicFrame writers or DataFrame write, depending on your use case.



### 3. There are some transforms that I cannot figure out.

a. **How can I use SQL queries with DynamicFrames?**

  You can leverage Spark's SQL engine to run SQL queries over your data.
  If you have a DynamicFrame called `my_dynamic_frame`, you can use the following snippet
  to convert the DynamicFrame to a DataFrame, issue a SQL query, and then
  convert back to a DynamicFrame.

      df = my_dynamic_frame.toDF()
      df.createOrReplaceTempView("temptable")
      sql_df = spark.sql("SELECT * FROM temptable")
      new_dynamic_frame = DynamicFrame.fromDF(sql_df, glueContext, "new_dynamic_frame")

  Note that we assign the `spark` variable at the start of generated scripts
  for convenience, but if you modify your script and delete this variable,
  you can also reference the SparkSession using `glueContext.spark_session`.

b. **How do I do filtering in DynamicFrames?**

  DynamicFrames support basic filtering via the SplitRows transformation which
  partitions it into two new DynamicFrames based on a predicate. For example,
  the snippet partition my_dynamic_frame into two frames called "adults" and "youths".:

      frame_collection = SplitRows.apply(my_dynamic_frame,
                                         {"age": {">": 21}},
                                         "adults", "youths")

    You can access these by indexing into the frame_collection. For instance,
    `frame_collection['adults']` returns the DynamicFrame containing all records
    with `age > 21`.

     It is possible to perform more sophisticated filtering by converting to a     DataFrame and then using the filter method. For instance, the query above     could be expressed as:     ```     result = my_dynamic_frame.toDF().filter("age > 21")     new_dynamic_frame = DynamicFrame.fromDF(result, glueContext, "new_dynamic_frame")     ```     More information about Spark SQL's filter syntax can be found in the [Spark SQL     Programming Guide](https://spark.apache.org/docs/2.1.0/sql-programming-guide.html).c. **Can I use a lambda function with a DynamicFrame?**  Lambda functions and other user-defined functions are currently only supported
  using SparkSQL DataFrames. You can convert a DynamicFrame to a DataFrame using
  the `toDF()` method and then specify Python functions (including lambdas) when
  calling methods like `foreach`. More information about methods on DataFrames
  can be found in the [Spark SQL Programming Guide](https://spark.apache.org/docs/2.1.0/sql-programming-guide.html)
  or the [PySpark Documentation](http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html).

d. **So, what else can I do with DynamicFrames?**

  DynamicFrames are designed to provide maximum flexibility when dealing with messy
  data that may lack a declared schema. Records are represented in a flexible
  self-describing way that preserves information about schema inconsistencies in the
  data.

  For example, with changing requirements, an address column stored as a
  `string` in some records might be stored as a `struct` in later rows.
  Rather than failing or falling back to a `string`, DynamicFrames will
  track both types and gives users a number of options in how to resolve
  these inconsistencies, providing fine grain resolution options via the
  `ResolveChoice` transforms.

  DynamicFrames also provide a number of powerful high-level ETL operations
  that are not found in DataFrames. For example, the `Relationalize` transform
  can be used to flatten and pivot complex nested data into tables suitable for
  transfer to a relational database. In additon, the `ApplyMapping` transform
  supports complex renames and casting in a declarative fashion.

  DynamicFrames are also integrated with the AWS Glue Data Catalog, so creating
  frames from tables is a simple operation. Writing to databases can be done
  through connections without specifying the password. Moreover, DynamicFrames
  are integrated with job bookmarks, so running these scripts in the job system
  can allow the script to implictly keep track of what was read and written.

  We are continually adding new transform, so be sure to check our documentation
  and let us know if there are new transforms that would be useful to you.

  
### 4. File Formats?

a. *Which file formats do you support for input and for output?*

   Out of the box we support JSON, CSV, ORC, Parquet, and Avro.

b. *What compression types do you support?*

   We support gzip, bzip2, and lz4.



### 5. JDBC access?

a. *Which JDBC databases do you support?*

   Out of the box we support Postgres, MySQL, Redshift, and Aurora.

b. *Can you support my JDBC driver for database XYZ?*

   In the case of unsupported databases, we fall back to using Spark.  You can specify the
   driver and driver options using the options fields, and make the driver available using
   the *–extra-jars* options in the job arguments.

   
   
### 6. How can I handle multi-line CSV files?

By default we assume that each CSV record is contained on a single line. This allows us to
automatically split large files to achieve much better parallelism while reading. If your CSV
data does contain multiline fields enclosed in double-quotes, you can set the 'multiLine' 
table property in the Data Catalog to 'true' to disable splitting.


### 7. My test connection is not working... What do I do?

AWS Glue uses private IP addresses in the subnet while creating Elastic Network Interface(s)
in the customer’s specified VPC/Subnet. Security groups specified in the Connection are applied
on each of the ENIs. Check whether your Security Groups allow outbound access and whether
they allow connectivity to the database cluster. Also, Spark requires bi-directional connectivity
among driver and executor nodes. One of the security groups need to allow ingress rules on all TCP
ports. You can prevent it from being open to the world by restricting the source of the Security
Group to itself (self-referential security group).


### 8. I'm getting the error that I have no S3 access... What do I do?

AWS Glue uses private IP addresses in the subnet while creating Elastic Network Interface(s)
in customer’s specified VPC/Subnet. Check your VPC route tables to ensure that there is an S3
VPC Endpoint so that traffic does not leave out to the internet. If the S3 buckets that you need
to access are in a different region, you need to set up a NAT Gateway (the IP addresses are private).


### 9. How do I take advantage of JobBookmarks?

a. *What is a JobBookmark?*

   A JobBookmark captures the state of job. It is composed of states for various elements of the job,
   including sources, transformations, and sinks. Currently we only have implementation for S3 sources
   and `Relationalize`. Enabling a JobBookmark ensures that if a job is run again after a previous successful
   run, it will continue from where it left off. However, if a job is run after a previous failed run,
   it will process the data that it failed to process in the previous attempt.

b. *How do I enable/disable Job Bookmarks?*

   Bookmarks are optional and can be disabled or suspended and re-enabled in the console.

c. *I modified my script... can I reset my JobBookmark?*

   Yes if you want to reset the Job bookmark, it can be reset in the console, or by calling the `ResetJobBookmark` API.

d. *I made a mistake... can I rewind my JobBookmark?*

   Currently we don’t support rewinding to any arbitrary state. You can manually clean up the data and reset
   the bookmark.

### 10. What is a development endpoint?

A DevEndpoint is used for developing and debugging your ETL scripts. You can connect a Notebook such as Zeppelin,
or an IDE, or a terminal to a DevEndpoint, which can then provide interactive development and testing of a pyspark script.
Once the script succeeds in the DevEndpoint, you can upload the script to S3 and run it in a Job.


### 11. Custom Libraries

a. *How do I create a Python library and use it with Glue?*

   You can split your script into multiple scripts and refer to these functions in the main script
   or within the scripts. You can use the `extra python files` option in the job to provide a list
   of files that will be made available to the main script. If you have more than a handful of files
   or if they are in some hierarchy, you can create a `zip` archive of the files and just pass the
   archive as the `extra python files` in the Job option.

b. *How do I create a Java library and use it with Glue?

   It is not a typical use case to write to java function and invoke it in Python. However, it is
   possible to invoke a java function from Python as follows:

       from py4j.java_gateway import java_import
       java_import(sc._jvm, "com.amazonaws.glue.userjava.UserJava")
       uj = sc._jvm.UserJava()
       uj.foo()

   Provide the jar which has the class `com.amazonaws.glue.userjava.UserJava` using the `extra jars`
   options in the Job argument.


### 12. GlueContext, SparkContext, and SparkSession, Oh My!

With Spark 2.1, SparkSession is the recommended way to run SQL queries or create temporary table views.
Here is an example of a SQL query that uses a SparkSession:

     sql_df = spark.sql("SELECT * FROM temptable")

To simplify using spark for registered jobs in AWS Glue, our code generator initializes the spark
session in the `spark` variable similar to GlueContext and SparkContext.

     spark = glueContext.spark_session

On DevEndpoints, a user can initialize the spark session herself in a similar way.



### 13. Can I use a graphical tool to build my ETL scripts?

*I don't like to write programs, and the console doesn't provide all the transformations I need...
Is there any other graphical tool that I can use for building ETL scripts?*

We are constantly improving our suite of transformations as well as the ability to graphically
construct ETL flows. We would love your feedback on what new transforms you'd like to have and
what tools you'd like us to support.


### 14. Do you support other sources and sinks?
*How about DynamoDB, Kinesis, ElasticSearch, and others like those?*

We are constantly adding connectors to new data sources. In the meantime, our environment
comes with Boto3 pre-installed, so for small data sets you can connect directly to these services
using standard API calls through Python. For larger data sets, you can dump their data into S3
periodically and use AWS Glue to schedule jobs to process those data sets.


### 15. I'm having a problem recrawling an S3 bucket...

It is possible that you might be encountering this problem:
Suppose you have an s3 bucket with contents like this:

    billing
    |--- year=2016
    |--- year=2017
    |--- unrelated.csv

 - The `billing` folder contains billing information partitioned by year,
   while `unrelated.csv` is a file containing unrelated data.
 - So you created a crawler with target {‘S3 path’ : ‘billing’},
   but you were unaware of the `unrelated csv` file. You expected the
   crawl to create a single table called `billing`.
 - But instead, you ended up with three tables named `year=2016`, `year=2017`, and `unrelated_csv`.
 - So, suppose you now *exclude* the `unrelated.csv` file and crawl again.
 - Again, you expect that one `billing' table will be created, and the other
   tables will be deleted... but it doesn't work!

To make the recrawl work properly, you actually have to *remove* the 
`unrelated.csv` file from the bucket-- excluding it will not work.


