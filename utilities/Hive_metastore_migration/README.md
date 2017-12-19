# Migration between the Hive Metastore and the AWS Glue Data Catalog

### Introduction

The provided scripts migrate metadata between Hive metastore and AWS Glue Data Catalog. The following scenarios are
supported.


#### Hive Metastore to an AWS Glue Data Catalog

 - **Direct Migration**: Set up an AWS Glue ETL job which extracts metadata
   from your Hive metastore (MySQL) and loads it into your AWS Glue Data
   Catalog.  This method requires an AWS Glue connection to the Hive metastore
   as a JDBC source. An ETL script is provided to extract metadata from the
   Hive metastore and write it to AWS Glue Data Catalog.

 - **Migration using Amazon S3 Objects**: Two ETL jobs are used. The first job extracts 
   your database, table, and partition metadata from your Hive metastore into Amazon S3.
   This job can be run either as an AWS Glue job or on a cluster with Spark installed. 
   The second is an AWS Glue job that loads the metadata from S3 into the AWS Glue Data Catalog.


#### AWS Glue Data Catalog to Hive Metastore

 - **Direct Migration**: An ETL job extracts metadata from specified databases
   in the AWS Glue Data Catalog and loads it into a Hive metastore. This job is run
   by AWS Glue, and requires an AWS Glue connection to the Hive metastore
   as a JDBC source.
 - **Migration using Amazon S3 Objects**: Two ETL jobs are used. The first is an AWS Glue job that extracts
   metadata from specified databases in the AWS Glue Data Catalog and then writes it as S3 objects.
   The second job loads the S3 objects into a Hive Metastore. The second job can be run either 
   as an AWS Glue job or on a cluster with Spark installed.


#### AWS Glue Data Catalog to another AWS Glue Data Catalog

- **Migration using Amazon S3 Objects**: Two AWS Glue jobs ETL jobs are run. The first
   extracts metadata from specified databases in an AWS Glue Data Catalog and loads it
   into S3. The second loads data from S3 into an AWS Glue Data Catalog. This is the
   only way to migrate between Data Catalogs in different accounts. It combines the
   workflow for AWS Glue to Hive using S3 with the workflow for Hive to AWS Glue
   using S3.


#### Limitations

 - Only databases, tables and partitions can be migrated. Other entities such as column
   statistics, privileges, roles, functions, and transactions cannot be migrated.

 - The script only supports a Hive metastore stored in a MySQL-compatible JDBC source.
   Other Hive metastore database engines such as PostgreSQL are not supported yet.

 - There is no isolation guarantee, which means that if Hive is doing concurrent
   modifications to the metastore while the migration job is running, inconsistent
   metadata can be introduced into the AWS Glue Data Catalog.

 - There is no streaming support. Hive metastore migration is done as a batch job.

 - If there is a naming conflict with existing objects in the target Data Catalog,
   then the existing data is overwritten by the new data.

 - Your Hive metastore must reside in a MySQL database accessible by AWS Glue.
   Currently, AWS Glue is able to connect to the JDBC data sources in a VPC subnet,
   such as RDS, EMR local Hive metastore, or a self-managed database on EC2.
   If your Hive metastore is not directly accessible by AWS Glue, then you must
   use Amazon S3 as intermediate staging area for migration.

## Instructions

Below are instructions for using each of the migration workflows described above.

#### Migrate Directly from Hive to AWS Glue

1. Set up AWS Glue as described in the following steps: 
   - [Set up IAM permissions for AWS Glue](http://docs.aws.amazon.com/glue/latest/dg/getting-started-access.html).
   - [Set up VPC endpoints for Amazon S3](http://docs.aws.amazon.com/glue/latest/dg/vpc-endpoints-s3.html).
   - [Set up VPC to connect to JDBC Data Stores](http://docs.aws.amazon.com/glue/latest/dg/setup-vpc-for-glue-access.html).
     If the Hive metastore is in a local database in the EMR master instance, just configure 
     the EMR VPC subnet and EMR master security group, similar to the RDS configuration.
   - [Set up DNS in your VPC](http://docs.aws.amazon.com/glue/latest/dg/set-up-vpc-dns.html).

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
3. On the AWS Glue console, create a connection to the Hive metastore as a JDBC data source.
   Follow the [instructions in the Developer Guide](http://docs.aws.amazon.com/glue/latest/dg/populate-add-connection.html)
   to create a connection that references your Hive metastore. Use the connection
   JDBC URL, username, and password you gathered in a previous step. Specify the VPC,
   subnet, and security group associated with your Hive metastore. You can find these
   on the EMR console if the Hive metastore is on the EMR master node, or on the RDS
   console, if the metastore is an RDS instance.

4. (Recommended) Test the connection. Choose "Test connection" on the AWS Glue connections page.
   If the connection test fails, your ETL job might not be able to connect to your
   Hive metastore data source. Fix the network configuration before moving on.

5. Upload these ETL job scripts to an S3 bucket:

       import_into_datacatalog.py
       hive_metastore_migration.py

   If you configured AWS Glue to access S3 from a VPC endpoint, you must upload
   the script to a bucket in the same AWS region where your job runs.

6. Create a job on the AWS Glue console to extract metadata from your Hive
   metastore to migrate it to AWS Glue Data Catalog. Define the job with 
   **An existing script that you provide** and the following properties:
    
   - **Script path where the script is stored** - S3 path to `import_into_datacatalog.py`
   - **Python library path** - S3 path to `hive_metastore_migration.py`
   
   Also add the following job parameters. These parameters are defined in the source code of `import_into_datacatalog.py`.

   - `--mode` set to `from-jdbc`, which means the migration is from a JDBC
     source into an AWS Glue Data Catalog.

   - `--connection-name` set to the name of the AWS Glue connection
     you created to point to the Hive metastore. It is used to extract the Hive JDBC
     connection information using the native Spark library.

   - `--region` the AWS region for Glue Data Catalog, for example, `us-east-1`.
     You can find a list of Glue supported regions here: http://docs.aws.amazon.com/general/latest/gr/rande.html#glue_region.
     If not provided, `us-east-1` is used as default.

   - `--database-prefix` (optional) set to a string prefix that is applied to the
     database name created in AWS Glue Data Catalog. You can use it as a way
     to track the origin of the metadata, and avoid naming conflicts. The default
     is the empty string.

   - `--table-prefix` (optional) set to a string prefix that is applied to the table
     names created in AWS Glue Data Catalog. Again, you can use it as a way to
     track the origin of the metadata, and avoid naming conflicts. The default is
     the empty string.

   In the Connections page, add the Hive metastore connection you created. The same connection must
   be specified both here and in the `--connection-name` argument. 
   Finally, review the job and create it.

7. Run the job on demand with the AWS Glue console. When the job is finished, the metadata
   from the Hive metastore is visible in the AWS Glue console.  Check the databases
   and tables listed to verify that they were migrated correctly.


#### Migrate from Hive to AWS Glue using Amazon S3 Objects

If your Hive metastore cannot connect to the AWS Glue Data Catalog directly (for example, if it's
on a private corporate network), you can use AWS Direct Connect to establish
private connectivity to an AWS VPC, or use AWS Database Migration Service to
migrate your Hive metastore to a database on AWS.

If the above solutions don't apply to your situation, you can choose to first
migrate your Hive metastore to Amazon S3 objects as a staging area, then run an ETL
job to import the metadata from S3 to the AWS Glue Data Catalog. To do this, you need to
have a Spark 2.1.x cluster that can connect to your Hive metastore and export
metadata to plain files on S3. The Hive metastore to S3 migration can also run
as an Glue ETL job, if AWS Glue can directly connect to your Hive metastore.

1. Make the MySQL connector jar available to the Spark cluster on the master and
   all worker nodes. Include the jar in the Spark driver class path as well
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
     cluster has EMRFS library in its class path. The script will export the metadata to a
     subdirectory of the output-path you provided.
     
   - `--database-prefix` and `--table-prefix` (optional) to set a string prefix that is applied to the 
     database and table names. They are empty by default. 
     
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
    
    - If the job finishes successfully, it creates 3 sub-folders in the S3 output path you
    specified named "databases", "tables" and "partitions". These paths will be used in the next step.
    
3. Upload these ETL job scripts to an S3 bucket:

       import_into_datacatalog.py
       hive_metastore_migration.py    

4. Create a job on the AWS Glue console to extract metadata from your Hive
   metastore and write it to AWS Glue Data Catalog. Define the job with **An existing script that you provide** and the following properties:
    
   - **Script path where the script is stored** - S3 path to `import_into_datacatalog.py`
   - **Python library path** - S3 path to `hive_metastore_migration.py`

   Add the following parameters.

   - `--mode` set to `from-s3`
   - `--region` the AWS region for Glue Data Catalog, for example, `us-east-1`.
     You can find a list of Glue supported regions here: http://docs.aws.amazon.com/general/latest/gr/rande.html#glue_region.
     If not provided, `us-east-1` is used as default.
   - `--database-input-path` set to the S3 path containing only databases. For example: `s3://someBucket/output_path_from_previous_job/databases`
   - `--table-input-path` set to the S3 path containing only tables. For example: `s3://someBucket/output_path_from_previous_job/tables`
   - `--partition-input-path` set to the S3 path containing only partitions. For example: `s3://someBucket/output_path_from_previous_job/partitions`

   Also, because there is no need to connect to any JDBC source, the job doesn't
   require any connections.

   Finally, if you don't have access to Spark from an on-premises network where your
   Hive metastore resides, you may implement your own script to load data into S3 using
   any script and platform, as long as the intermediary data on S3 conforms to the
   pre-defined format and schema.

   The migration script requires a separate S3 folder for each entity type: database,
   table, and partition. Each folder contains one to many files. Each line of a file
   is a single JSON object. No comma or any other separation character can appear
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

       export_from_datacatalog.py
       hive_metastore_migration.py

   If you configured AWS Glue to access S3 from a VPC endpoint, you must upload the script
   to a bucket in the same region where your job runs. If you configured AWS Glue to
   access the public internet through a NAT gateway or NAT instance, cross-region S3 access is allowed.

3. Create a job on the AWS Glue console to extract metadata from the AWS Glue Data Catalog to the JDBC Hive metastore.
   Define the job with **An existing script that you provide** and the following properties:
    
   - **Script path where the script is stored** - S3 path to `export_from_datacatalog.py`
   - **Python library path** - S3 path to `hive_metastore_migration.py`

   Add the following parameters.
   
   - `--mode` set to `to-jdbc`, which means the migration is
      directly to a jdbc Hive Metastore
   - `--connection-name` set to the name of the AWS Glue connection
      you created to point to the Hive metastore. It is the destination of the migration.
   - `--region` the AWS region for Glue Data Catalog, for example, `us-east-1`.
     You can find a list of Glue supported regions here: http://docs.aws.amazon.com/general/latest/gr/rande.html#glue_region.
     If not provided, `us-east-1` is used as default.
   - `--database-names` set to a semi-colon(;) separated list of
      database names to export from Data Catalog.

   In the Connections page, add the Hive metastore connection you created:

4. Run the job on demand with the AWS Glue console. When the job is finished, run Hive queries
   on a cluster connected to the metastore to verify that metadata was successfully migrated.



#### Migrate from AWS Glue to Hive through Amazon S3 Objects

1. Follow step 1 in **Migrate from Hive to AWS Glue using Amazon S3 Objects**.

2. Create an AWS Glue ETL job similar to the one described in the Direct Migration
   instructions above. Since the destination is now an S3 bucket instead of a Hive metastore,
   no connections are required. In the job, add the following parameters:

   - `--mode` set to `to-s3`, which means the migration is to S3.
   - `--region` the AWS region for Glue Data Catalog, for example, `us-east-1`.
     You can find a list of Glue supported regions here: http://docs.aws.amazon.com/general/latest/gr/rande.html#glue_region.
     If not provided, `us-east-1` is used as default.
   - `--database-names` set to a semi-colon(;) separated list of
      database names to export from Data Catalog.
   - `--output-path` set to the S3 destination path.

3. Submit the `hive_metastore_migration.py` Spark script to your Spark cluster.

   - Set `--direction` to `to_metastore`.
   - Provide the JDBC connection information through the arguments:
     `--jdbc-url`, `--jdbc-username`, and `--jdbc-password`.
   - The argument `--input-path` is required. This can be a local directory or
     an S3 path. The path points to the directory containing `databases`,
     `partitions`, `tables` folders. This path should be the same as in the
     `--output-path` parameter of step 2, with date and time information
     appended at the end.

     For example, if `--output-path` is:

         s3://gluemigrationbucket/export_output/

     The result of the AWS Glue job in step 2 creates `databases`,
     `partitions`, and `tables` folders in:

         s3://gluemigrationbucket/export_output/<year-month-day-hour-minute-seconds>

     Then `--input-path` is specified as:

         s3://gluemigrationbucket/export_output/<year-month-day-hour-minute-seconds>/

    	 
#### AWS Glue Data Catalog to another AWS Glue Data Catalog
 
You can migrate (copy) metadata from the Data Catalog in one account to another. The steps are:
 
1. Enable cross-account access for an S3 bucket so that both source and target accounts can access it. See  
   [the Amazon S3 documenation](http://docs.aws.amazon.com/AmazonS3/latest/dev/example-bucket-policies.html#example-bucket-policies-use-case-1) 
   for S3 cross-account access configuration.
   
2. Upload the the following scripts to an S3 bucket accessible from the source AWS account:

       export_from_datacatalog.py
       hive_metastore_migration.py
       
3. Upload the the following scripts to an S3 bucket accessible from the target AWS account to be updated:

       import_into_datacatalog.py
       hive_metastore_migration.py       

4. In the source AWS account, create a job on the AWS Glue console to extract metadata from the AWS Glue Data Catalog to S3.
   Define the job with **An existing script that you provide** and the following properties:
    
   - **Script path where the script is stored** - S3 path to `export_from_datacatalog.py`
   - **Python library path** - S3 path to `hive_metastore_migration.py`

       
   Add the following parameters:
 
   - `--mode` set to `to-s3`, which means the migration is to S3.
   - `--region` the AWS region for Glue Data Catalog, for example, `us-east-1`.
     You can find a list of Glue supported regions here: http://docs.aws.amazon.com/general/latest/gr/rande.html#glue_region.
     If not provided, `us-east-1` is used as default.
   - `--database-names` set to a semi-colon(;) separated list of
      database names to export from Data Catalog.
   - `--output-path` set to the S3 destination path that you configured with **cross-account access**.
 
   The job will create `databases`, `partitions`, and `tables` folders in the S3 output folder you choose.

5. In the target AWS account, create a job on the AWS Glue console to import metadata to the target AWS Glue Data Catalog.
   Define the job with **An existing script that you provide** and the following properties:
    
   - **Script path where the script is stored** - S3 path to `import_into_datacatalog.py`
   - **Python library path** - S3 path to `hive_metastore_migration.py`
   
   Add the following parameters.
 
   - `--mode` set to `from-s3`
   - `--region` the AWS region for Glue Data Catalog, for example, `us-east-1`.
     You can find a list of Glue supported regions here: http://docs.aws.amazon.com/general/latest/gr/rande.html#glue_region.
     If not provided, `us-east-1` is used as default.
   - `--database-input-path` set to the S3 path containing only databases.
   - `--table-input-path` set to the S3 path containing only tables.
   - `--partition-input-path` set to the S3 path containing only partitions.
   
6. (Optional) Manually delete the temporary files generated in the S3 folder. Also, remember to revoke the 
   cross-account access if it's not needed anymore.          
