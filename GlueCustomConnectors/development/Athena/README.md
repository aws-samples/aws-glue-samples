## Development Guide - AWS Glue custom connector with Athena federated query interface

Glue ETL is adding support for custom connectors of various types, including Spark, Athena and JDBC. This module means to serve as a guided example for writing an Athena connector to be used by AWS Glue to query a custom data source (You can also use the same connector in Athena, more details can be found [here](https://github.com/awslabs/aws-athena-query-federation)). The goal is to help you understand the development process and point out capabilities. In some examples we use hard coded schemas to separate learning how to write a connector from learning how to interface with the target systems you ultimately want to federate to. 

**Please note that Glue doesn't support the User Defined Functions(UDFs), predicate push down of Athena connectors yet.**


## What is a 'Connector'?

A 'connector' is a piece of code that can translate between your target data source and your computing platform. Athena's Query Federation feature provides an [SDK](https://github.com/awslabs/aws-athena-query-federation/tree/master/athena-federation-sdk) to users to define their own connectors for different data sources. AWS Glue is launching support for connectors written in Athena's interface to run in Glue's Spark runtime environment. You will be able to use your existing or to-be-developed Athena connectors with little to no changes in Glue ETL job. In this example, we're developing a simple connector that loads data from S3 in partitions. From the top-level, your connector must provide the following:

1. A source of meta-data for Athena to get schema information about what databases, tables, and columns your connector has. This is done by extending com.amazonaws.athena.connector.lambda.handlers.MetadataHandler in the athena-federation-sdk module. 
2. A way for Athena to read the data stored in your tables. This is done by extending com.amazonaws.athena.connector.lambda.handlers.RecordHandler in the athena-federation-sdk module. 

In order to use such a connector in Athena Query Federation, you would need to deploy it as a Lambda function. It's slight different in Glue, you will deploy the build artifact of this connector, upload it to S3 and create BYOC connector via Glue Studio to use it in the ETL job. More details about the end-to-end workflow can be found in this [doc](https://docs.aws.amazon.com/glue/latest/ug/connectors-chapter.html#creating-custom-connectors
), here we focus on providing an example and run it locally.

In the next section we take a closer look at the methods we must implement on the MetadataHandler and RecordHandler.

### MetadataHandler Details

Lets take a closer look at MetadataHandler requirements. In the following example, we have the basic functions that you need to implement when using the Amazon Athena Query Federation SDK's MetadataHandler to satisfy the boiler plate work of serialization and initialization.

```java
public class MyMetadataHandler extends MetadataHandler
{
    /**
     * Used to get the list of schemas (aka databases) that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @return A ListSchemasResponse which primarily contains a Set<String> of schema names and a catalog name
     * corresponding the Athena catalog that was queried.
     */
    @Override
    protected ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request) {}

    /**
     * Used to get the list of tables that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog and database they are querying.
     * @return A ListTablesResponse which primarily contains a List<TableName> enumerating the tables in this
     * catalog, database tuple. It also contains the catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    protected ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request) {}

    /**
     * Used to get definition (field names, types, descriptions, etc...) of a Table.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @return A GetTableResponse which primarily contains:
     *             1. An Apache Arrow Schema object describing the table's columns, types, and descriptions.
     *             2. A Set<String> of partition column names (or empty if the table isn't partitioned).
     */
    @Override
    protected GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request) {}

    /**
     * Used to get the partitions that must be read from the request table in order to satisfy the requested predicate.
     *
     * @param blockWriter Used to write rows (partitions) into the Apache Arrow response.
     * @param request Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @note Partitions are partially opaque to Amazon Athena in that it only understands your partition columns and
     * how to filter out partitions that do not meet the query's constraints. Any additional columns you add to the
     * partition data are ignored by Athena but passed on to calls on GetSplits. Also note tat the BlockWriter handlers 
     * automatically constraining and filtering out values that don't satisfy the query's predicate. This is how we
     * we accomplish partition pruning. 
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) {}

    /**
     * Used to split-up the reads required to scan the requested batch of partition(s).
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details of the catalog, database, table, andpartition(s) being queried as well as
     * any filter predicate.
     * @return A GetSplitsResponse which primarily contains:
     *             1. A Set<Split> which represent read operations Amazon Athena must perform by calling your read function.
     *             2. (Optional) A continuation token which allows you to paginate the generation of splits for large queries.
     * @note A Split is a mostly opaque object to Amazon Athena. Amazon Athena will use the optional SpillLocation and
     * optional EncryptionKey for pipelined reads but all properties you set on the Split are passed to your read
     * function to help you perform the read.
     */
    @Override
    protected GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) {}
}
```

You can find example MetadataHandlers by looking at some of the connectors in the repository. [athena-cloudwatch](https://github.com/awslabs/aws-athena-query-federation/tree/master/athena-cloudwatch) and [athena-tpcds](https://github.com/awslabs/aws-athena-query-federation/tree/master/athena-tpcds) are fairly easy to follow along with.

You can also, use the AWS Glue DataCatalog as the authoritative (or supplemental) source of meta-data for your connector. To do this, you can extend [com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-sdk/src/main/java/com/amazonaws/athena/connector/lambda/handlers/GlueMetadataHandler.java) instead of [com.amazonaws.athena.connector.lambda.handlers.MetadataHandler](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-sdk/src/main/java/com/amazonaws/athena/connector/lambda/handlers/MetadataHandler.java). GlueMetadataHandler comes with implementations for doListSchemas(...), doListTables(...), and doGetTable(...) leaving you to implemented only 2 methods. The Amazon Athena DocumentDB Connector in the [athena-docdb](https://github.com/awslabs/aws-athena-query-federation/tree/master/athena-docdb) module is an example of using GlueMetadataHandler.

### RecordHandler Details

Lets take a closer look at what is required for a RecordHandler requirements. In the following example, we have the basic functions we need to implement when using the Amazon Athena Query Federation SDK's [RecordHandler](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-sdk/src/main/java/com/amazonaws/athena/connector/lambda/handlers/RecordHandler.java) to satisfy the boiler plate work of serialization and initialization.

```java
public class MyRecordHandler
        extends RecordHandler
{
    /**
     * Used to read the row data associated with the provided Split.
     * @param constraints A ConstraintEvaluator capable of applying constraints form the query that request this read.
     * @param spiller A BlockSpiller that should be used to write the row data associated with this Split.
     *                The BlockSpiller automatically handles chunking the response, encrypting, and spilling to S3. Note:
     *                Glue will send in a BlockSpiller instance that doesn't spill to S3, you can still call the writeRows
     *                method to write your data in as normal.
     * @param recordsRequest Details of the read request, including:
     *                           1. The Split
     *                           2. The Catalog, Database, and Table the read request is for.
     *                           3. The filtering predicate (if any)
     *                           4. The columns required for projection.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @note Avoid writing >10 rows per-call to BlockSpiller.writeRow(...) because this will limit the BlockSpiller's
     *       ability to control Block size. The resulting increase in Block size may cause failures and reduced performance.
     */
    @Override
    protected void readWithConstraint(ConstraintEvaluator constraints, BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker){}
}
```

## How To Build & Deploy

You can use any IDE or even just a command line editor to write your connector. The following steps show you how to use IntelliJ IDEA to get started but most of the steps are applicable to any Linux based development machine.


### Step 1: Setup your IntelliJ IDE

1. Open the IntelliJ website and download the IDE from this [link](https://www.jetbrains.com/idea/).
2. Install the IDE by following the instruction after the download has finished.


### Step 2: Download The SDK + Connectors

1. Open your terminal and run `git clone https://github.com/awslabs/aws-athena-query-federation.git` to get a copy of the Amazon Athena Query Federation SDK, Connector Suite, and Example Connector.

### Step 3: Install Prerequisites for Development

1. If you are working on a development machine that already has Apache Maven, the AWS CLI, you can skip this step. If not, you can run the `./tools/prepare_dev_env.sh` script in the root of the Github project you just checked out.
2. To ensure your terminal can see the new tools that we installed run `source ~/.profile` or open a fresh terminal. If you skip this step you will get errors later about the AWS CLI not able to publish your connector artifact.

As of 12/21/2020, Glue supports Athena connectors built with `athena-federation-sdk` version 2020.5.1. So you need to run `git reset --hard 112500b499be9326a8da51f5cf402c4ddc14b5de` to travel back to that version of SDK. Now run `mvn clean install -DskipTests=true > /tmp/log` from the athena-federation-sdk directory within the Github project you checked out earlier. We are skipping tests with the `-DskipTests=true` option to make the build faster. As a best practice, you should let the tests run. 

### Step 4: Write The Code

1. Create an s3 bucket (in the same region you will be deploying the connector), that we can upload some sample data using the following command `aws s3 mb s3://BUCKET_NAME` but be sure to put your actual bucket name in the command and that you pick something that is unlikely to already exist.
2. If you haven't check out the git repo containing this README you're reading, please run `git clone https://github.com/aws-samples/aws-glue-samples.git` to download the repo to your local dev environment.
3. Complete the boiler plate code in ExampleMetadataHandler in `ETLConnector/development/Athena` by uncommenting the provided example code and providing missing code where indicated.
4. Complete the boiler plate code in ExampleRecordHandler in `ETLConnector/deveclopment/Athena` by uncommenting the provided example code and providing missing code where indicated.
5. Upload our sample data by running the following command from aws-athena-query-federation/athena-example directory in the athena sdk repo. Be sure to replace BUCKET_NAME with the name of the bucket your created earlier.  `aws s3 cp ./sample_data.csv s3://BUCKET_NAME/2017/11/1/sample_data.csv`

### Step 5: build and test your connector
At this step, we're going to build the connector you just created and test it by running it locally.
1. change the working directory of your terminal to `aws-glue-samples/ETLConnector/development/Athena`, run `mvn install`. This command builds your connector and installs the resulting jar into your local maven repository.
2. Build a local Scala environment with local Glue ETL maven library: [Developing Locally with Scala](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html).
3. Open your local Glue ETL library in IntelliJ and add the example connector jar as its dependencies by going to Project Structures -> Libraries -> New Project Library -> Java and supply the path to the connector jar, which should be at `/<your-path>/aws-glue-samples/ETLConnector/development/Athena/target/athena-example-1.0.jar`
4. Run the following script in the local Glue ETL environment. (Please set the environment variable `data_bucket` to the bucket you created in step 4 in your run configuration. This is because the example connector looks up the bucket from the environment.)

```scala
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AthenaLocalTest {
  def main(sysArgs: Array[String]) {

    val conf = new SparkConf().setAppName("SimpleTest").setMaster("local")
    val spark: SparkContext = new SparkContext(conf)
    val glueContext: GlueContext = new GlueContext(spark)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val optionsMap = Map(
      "tableName" -> "table1",
      "schemaName" -> "schema1",
      "className" -> "com.amazonaws.athena.connectors.Example"
    )
    val customSource = glueContext.getSource(
      connectionType = "marketplace.athena",
      connectionOptions = JsonOptions(optionsMap))
    val dyf = customSource.getDynamicFrame()
    dyf.printSchema()
    dyf.show()
  }
}
```
You should be able to see input like below.
```
{"account_id": "2152", "month": 11, "encrypted_payload": "Z4/GTlYJjFbtuZGNZAifd89ZfrcuZTzaBJ8xbmhEawE=", "year": 2017, "day": 1, "transaction": {"id": 426795206, "completed": false}}
{"account_id": "1201", "month": 11, "encrypted_payload": "QGr/rjvAZMR2xCrv3jzQz1jU+hJ6eBQRxwX1/vAAonQ=", "year": 2017, "day": 1, "transaction": {"id": 1745621164, "completed": true}}
{"account_id": "8846", "month": 11, "encrypted_payload": "e1a6Bg5tsqUnAxnksep/YHA+E4QObmtA4YT25m4Xi7E=", "year": 2017, "day": 1, "transaction": {"id": 759562422, "completed": true}}
{"account_id": "4339", "month": 11, "encrypted_payload": "6iJWp3b7rvkWBEZWF5PmAptkPBhIpswTRtdLAQ/BOzo=", "year": 2017, "day": 1, "transaction": {"id": 2065477811, "completed": true}}
{"account_id": "7672", "month": 11, "encrypted_payload": "KxYCbyYoUF/QBS9PC5BV6/+l4Tz/It9w7C2Xbt6em0c=", "year": 2017, "day": 1, "transaction": {"id": 672236762, "completed": false}}
{"account_id": "3094", "month": 11, "encrypted_payload": "utjdZEqRbfuC9rws3UtIj2bgXbKg89Zs15tXcKxruh0=", "year": 2017, "day": 1, "transaction": {"id": 1507133521, "completed": false}}
{"account_id": "0223", "month": 11, "encrypted_payload": "dGsJMfq33kPGUgitYyel5M2qz1+ajzPfcMTDWOgAYzs=", "year": 2017, "day": 1, "transaction": {"id": 1169110059, "completed": false}}

root
|-- account_id: string
|-- month: int
|-- encrypted_payload: string
|-- year: int
|-- day: int
|-- transaction: struct
|    |-- id: int
|    |-- completed: boolean
```

### Step 6: Run a Glue ETL job using your connector!

Ok, now you've built and tested a sample Athena connector. The next for your is to explore how to bring your connector to Glue ETL. You can check out our [doc](https://docs.aws.amazon.com/glue/latest/ug/connectors-chapter.html#creating-custom-connectors) that shows the step-by-step guide. Glue has already published a few connectors in AWS Marketplace, which you can easily plug into your ETL job. You can follow this [Marketplace subscription doc](https://docs.aws.amazon.com/marketplace/latest/buyerguide/buyer-subscribing-to-products.html) to find out how to do that.

