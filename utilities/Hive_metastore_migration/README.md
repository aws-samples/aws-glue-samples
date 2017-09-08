# Migration between the Hive Metastore and the AWS Glue Data Catalog

### Introduction

These scripts let you migrate metadata between a Hive metastore and the
AWS Glue Data Catalog. You can migrate in three different directions:

 - From Hive to AWS Glue
 - From AWS Glue to Hive
 - From one AWS Glue Data Catalog to another

There are two modes of migration:

 - Direct migration
 - Migration using Amazon S3 as an intermediary


#### Hive Metastore to AWS Glue

 - **Direct Migration**: Set up an AWS Glue ETL job which extracts metadata
   from your Hive metastore (MySQL) and loads it into your AWS Glue Data
   Catalog.  This method uses an AWS Glue connection to the Hive metastore
   as a JDBC source. An ETL script is provided to extract metadata from the
   Hive metastore and write it to AWS Glue Data Catalog.

 - **Migration through Amazon S3**: Extract your database, table, and partition
   objects from your Hive metastore into Amazon S3 objects. Then set up an AWS Glue
   ETL job using the script provided here to load the metadata from S3 into the
   AWS Glue Data Catalog.


#### AWS Glue to Hive Metastore

 - **Direct Migration**: A single job extracts metadata from specified databases
   in AWS Glue Data Catalog and loads it into a Hive metastore. This job is run
   on the AWS Glue console, and requires an AWS Glue connection to the Hive metastore
   as a JDBC source.
 - **Migration through Amazon S3**: Two AWS Glue jobs are used. The first extracts
   metadata from specified databases in AWS Glue Data Catalog and loads them
   into S3. The second loads data from S3 into the Hive Metastore. The first job
   is run on AWS Glue Console. The second can be run either on the AWS Glue Console
   or on a cluster with Spark installed.


#### AWS Glue to AWS Glue

- **Migration through Amazon S3**: Two jobs are run on the AWS Glue console. The first
   extracts metadata from specified databases in an AWS Glue Data Catalog and loads them
   into S3. The second loads data from S3 into an AWS Glue Data Catalog. This is the
   only way to migrate between Data Catalogs in different accounts. It combines the
   workflow for AWS Glue to Hive through S3 with the workflow for Hive to AWS Glue
   through S3.


#### Limitations

There are some limitations to what the migration scripts can do currently:

 - Only databases, tables and partitions can be migrated. Other entities such as column
   statistics, privileges, roles, functions, and transactions cannot be migrated.

 - The script only supports a Hive metastore stored in a MySQL-compatible JDBC source.
   Other Hive metastore database engines such as PostgreSQL are not supported.

 - There is no isolation guarantee, which means that if Hive is doing concurrent
   modifications to the metastore while the migration job is running, inconsistent
   data can be introduced in AWS Glue Data Catalog.

 - There is no streaming support. Hive metastore migration is done as a batch job.

 - If there is a naming conflict with existing objects in the target Data Catalog,
   then the existing data is overwritten by the new data.


#### Prerequisites

 - Your account must have access to AWS Glue.

 - Your Hive metastore must reside in a MySQL database accessible to AWS Glue.
   Currently, AWS Glue is able to connect to the JDBC data sources in a VPC subnet,
   such as RDS, EMR local Hive metastore, or a self-managed database on EC2.
   If your Hive metastore is not directly accessible to AWS Glue, then you must
   use Amazon S3 as intermediate staging area for migration.

## Instructions

Below are instructions for using each of the migration workflows described above.

#### Migrate Directly from Hive to AWS Glue

1. Set up AWS Glue as described in the [Getting Started](http://docs.aws.amazon.com/glue/latest/dg/getting-started.html)
   section of the developer guide. Make sure you've configured the security
   group to give access to the Hive metastore. If the Hive metastore is in a local
   database in the EMR master instance, just configure the EMR VPC subnet and EMR
   master security group, similar to the RDS configuration.

2. Gather your Hive metastore JDBC connection information. You can find the Hive
   metastore JDBC URL, username, and password in the file named `hive-site.xml`.
   In EMR, this file is located at: `/etc/hive/conf/hive-site.xml`. For example:
```xml
      <property>
          <name>javax.jdo.option.ConnectionURL</name>
           <value>jdbc:mysql://ip-10-0-12-34.ec2.internal:3306/hive?createDatabaseIfNotExist=true</value>
      </property>

      <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>hive</value>
      </property>

      <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>MySecretPassword</value>
      </property>
```
3. In AWS Glue, create a connection to the Hive metastore as a JDBC data source.
   Follow the [instructions in the developer guide](http://docs.aws.amazon.com/glue/latest/dg/populate-add-connection.html)
   to create a connection that references your Hive metastore. Use the connection
   JDBC URL, username, and password you gathered in a previous step. Specify the VPC,
   subnet, and security group associated with your Hive metastore. You can find these
   on the EMR console if the Hive metastore is on the EMR master node, or on the RDS
   console, if the metastore is an RDS instance.

4. (Optional) Test the connection. Choose "Test connection" on the AWS Glue connections page.
   If the connection test fails, your ETL job might not be able to connect to your
   Hive metastore data source. Fix the network configuration before moving on.

5. Upload the ETL job scripts to S3. Upload these 2 scripts to an S3 bucket:

       hive_metastore_migration.py
       import_into_datacatalog.py

   If you configured AWS Glue to access S3 from a VPC endpoint, you must upload
   the script to a bucket in the same region where your job runs.

6. Create a new job on the AWS Glue console to extract metadata from your Hive
   metastore and write it to AWS Glue Data Catalog. Specify the following
   job parameters.  These parameters are defined in `import_into_datacatalog.py`.

   - `--mode` should be set to 'from-jdbc'`, which means the migration is from a JDBC
     source into an AWS Glue Data Catalog.

   - `--connection-name` should be set to the name of the AWS Glue connection
     you created to point to the Hive metastore. It is used to extract the Hive JDBC
     connection information using the native Spark library.

   - `--database-prefix` should be set to a string prefix that is applied to the
     database name created in AWS Glue Data Catalog. You can use it as a way
     to track the origin of the metadata, and avoid naming conflicts. The default
     is the empty string.

   - `--table-prefix` should be set to a string prefix that is applied to the table
     names created in AWS Glue Data Catalog. Again, you can use it as a way to
     track the origin of the metadata, and avoid naming conflicts. The default is
     the empty string.

   In the Connections page, add the Hive metastore connection you created.
   Finally, review the job and create it.

7. Run the job on the AWS Glue console. When the job is finished, the metadata
   from the Hive metastore is visible in the Glue console.  Check the databases
   and tables listed to verify that they were migrated correctly.


#### Migrate from Hive to AWS Glue Through Amazon S3

If your Hive metastore cannot connect to AWS Glue directly (for example, if it's
on a private corporate network), you can use AWS Direct Connect to establish
private connectivity to a AWS VPC, or use AWS Database Migration Service to
migrate your Hive metastore to a database on AWS.

If the above solutions don't apply to your situation, you can choose to first
migrate your Hive metastore to Amazon S3 as a staging area, then run an ETL
job to import the metadata from S3 to the Data Catalog. To do this, you need to
have a Spark 2.1.x cluster that can connect to your Hive metastore and export
metadata to plain files on S3.

1. Make the MySQL connector jar available to the Spark cluster on master and
   all worker nodes, and include the jar in the Spark driver class path as well
   as with the `--jars` and `--driver-class-path` parameters in the `spark-submit` command. You can download
   the MySql connector [here at MySql.com](https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.42.tar.gz).
   
   If you use EMR to do this configuration, you can run the EMR bootstrap
   script `emr_bootstrap_action.sh` included in the `shell` folder, and
   then provide `--jars /usr/lib/hadoop/mysql-connector-java-5.1.42-bin.jar` and 
   `--driver-class-path <spark-default-driver-classpath>:/usr/lib/hadoop/mysql-connector-java-5.1.42-bin.jar`
   in the spark-submit script. See [EMR documentation](http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html)
   for how to run Bootstrap script on EMR.

2. Submit the `hive_metastore_migration.py` Spark script to your Spark cluster
   using the following parameters:

   - Set `--direction` to `from_metastore`, or omit the argument since
     `from_metastore` is the default.

   - Provide the JDBC connection information through these arguments:
     `--jdbc-url`, `--jdbc-username`, and `--jdbc-password`.

   - The argument `--output-path` is required. It is either a local file system location
     or an S3 location. If the output path is a local directory, you can upload the data
     to an S3 location manually. If it is an S3 path, you need to make sure that the Spark
     cluster has EMRFS library on its class path. The script will export the metadata to a
     subdirectory of the output-path you provided.
     
   - Example spark-submit command to migrate Hive metastore to S3, tested on EMR-4.7.1:
    ```bash
    MYSQL_JAR_PATH=/usr/lib/hadoop/mysql-connector-java-5.1.42-bin.jar
    DRIVER_CLASSPATH=/home/hadoop/*:/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:$MYSQL_JAR_PATH
    spark-submit --driver-class-path $DRIVER_CLASSPATH \
      --jars $MYSQL_JAR_PATH \
      /home/hadoop/hive_metastore_migration.py \
      --mode from-metastore \
      --jdbc-url jdbc:mysql://metastore.foo.us-east-1.rds.amazonaws.com:3306 \
      --jdbc-user hive \
      --jdbc-password myJDBCPassword \
      --database-prefix myHiveMetastore_ \
      --table-prefix myHiveMetastore_ \
      --output-path s3://mybucket/myfolder/
    ```

3. Create an AWS Glue ETL job similar to the one described in the section to
   connect using JDBC to your Hive metastore.  In the job, set the following parameters.

   - `--mode` should be set to `from-S3`
   - `--database-input-path` should be set to the S3 path containing only databases.
   - `--table-input-path` should be set to the S3 path containing only tables.
   - `--partition-input-path` should be set to the S3 path containing only partitions.

   Also, because there is no need to connect to any JDBC source, the job doesn't
   require any connections.

   Finally, if you don't have access to Spark from an on-premises network where your
   Hive metastore resides, you may implement your own script to load data into S3 using
   any script and platform, as long as the intermediary data on S3 conforms to the
   pre-defined format and schema.

   The migration script requires a separate S3 folder for each entity type: database,
   table, and partition. Each folder contains one to many files. Each line of a file
   is a single JSON object. No comma or any other separation character should appear
   at the end of the line. Each JSON object has a field `type` with a value of
   `database`, `table`, or `partition`, and a field `item` that contains
   the metadata payload. The table entity also contains a `database` field to represent the
   database it belongs to. The partition entity contains a `database` and a `table` field.
   The exact schema of the entities are defined in the `hive_metastore_migration.py` script.

   Below is a prettified example of a table entity; in practice, it must all appear on a
   single line with the whitespace removed:

       { "type" : "table",
         "database" : "my_metastore_default",
         "item" : {
           "createTime" : "Apr 04, 2017 08:58:43 AM",
           "lastAccessTime" : "Jan 01, 1970 00:00:00 AM",
           "owner" : "hadoop",
           "retention" : 0,
           "name" : " my_metastore_tbl_mytable",
           "tableType" : "EXTERNAL_TABLE",
           "parameters" : {
             "EXTERNAL" : "TRUE",
             "transient_lastDdlTime" : "1491296323"
           },
           "partitionKeys" : [
             { "name" : "key1", "type" : "int" },
             { "name" : "key2", "type" : "string" },
             { "name" : "key3", "type" : "double" }
           ],
           "storageDescriptor" : {
             "inputFormat" : "org.apache.hadoop.mapred.TextInputFormat",
             "compressed" : false,
             "storedAsSubDirectories" : false,
             "location" : "S3://mydefaultgluetest/test3keys",
             "numberOfBuckets" : -1,
             "outputFormat" : "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
             "columns" :[
               { "name" : "col1", "type" : "int" }
             ],
             "serdeInfo" : {
               "serializationLibrary" : "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
               "parameters" : { "serialization.format" : "1" }
             }
           }
         }
       }


#### Migrate Directly from AWS Glue to Hive

1. Follow steps 1 through 4 in the instructions for **Migrate Directly from Hive to AWS Glue**.

2. Upload the the following job scripts to an S3 bucket:

       hive_metastore_migration.py
       export_from_datacatalog.py

   If you configured AWS Glue to access S3 from VPC endpoint, you must upload the script
   to a bucket in the same region where your job runs. If you configured AWS Glue to
   access the public internet through a NAT gateway or NAT instance, cross-region S3 access is allowed.

3. Create the AWS Glue Job that performs the direct migration from Glue to the JDBC
   Hive metastore. Specify the following job parameters as key-value pairs.
   Note that these parameters are pre-defined in the script.

   - `--mode` should be set to `to-jdbc`, which means the migration is
      directly to a jdbc Hive Metastore
   - `--connection-name` should be set to the name of the AWS Glue connection
      you created to point to the Hive metastore. It is the destination of the migration.
   - `--database-names` should be set to a semi-colon(;) separated list of
      database names to export from Data Catalog.

   In the Connections page, add the Hive metastore connection you created:

4. Run the job on the Glue Console. When the job is finished, run Hive queries
   on a cluster connected to the metastore to verify that metadata was successfully migrated.



#### Migrate from AWS Glue to Hive through Amazon S3

1. Follow step 1 in **Migrate from Hive to AWS Glue Through Amazon S3**.

2. Create an AWS Glue ETL job similar to the one described in the Direct Migration
   instructions above. Since the destination is now an S3 bucket instead of a Hive metastore,
   no connections are required. In the job, set the following parameters:

   - `--mode` should be set to `to-S3`, which means the migration is to S3.
   - `--database-names` should be set to a semi-colon(;) separated list of
      database names to export from Data Catalog.
   - `--output-path` should be set to the S3 destination path.

3. Submit the `hive_metastore_migration.py` Spark script to your Spark cluster.

   - Set `--direction` to `to_metastore`.
   - Provide the JDBC connection information through the arguments:
     `--jdbc-url`, `--jdbc-username`, and `--jdbc-password`.
   - The argument `--input-path` is required. This can be a local directory or
     an S3 path. The path should point to the directory containing `databases`,
     `partitions`, `tables` folders. This path should be the same as in the
     `--output-path` parameter of step 2, with date and time information
     appended at the end.

     For example, if `--output-path` was:

         s3://gluemigrationbucket/export_output/

     The result of the AWS Glue job in step 2 would create `databases`,
     `partitions`, and `tables` folders in:

         s3://gluemigrationbucket/export_output/<year-month-day-hour-minute-seconds>

     Then `--input-path` here should be:

         s3://gluemigrationbucket/export_output/<year-month-day-hour-minute-seconds>/

