# Develop Glue Custom Spark Connectors

## Introduction

In this doc, we will discuss how to create a Spark connector with Spark DataSource API V2 (Spark 2.4) to read data. 
We use this simple example to give an overview of the Spark DataSource interface implementation. 
The full code example is [MinimalSparkConnector](MinimalSparkConnector.java) in the same folder.

## Setup Environment

Build a local Scala environment with local Glue ETL maven library: [Developing Locally with Scala](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html).
You may also refer to [GlueSparkRuntime](../GlueSparkRuntime/README.md) for more details to custom the local environment for advanced testing.

## Spark DataSource Interfaces

| Interfaces           | Description                                                                                                                                                                                                                   |
|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| DataSourceV2         | The base interface for Spark DataSource v2 API.                                                                                                                                                                               |
| ReadSupport          | A mix-in interface for DataSourceV2 for the connector to provide data reading ability and scan the data from the data source.                                                                                                 |
| DataSourceReader     | A data source reader that is created by ReadSupport to scan the data from this data source. It also supports reading actual schema and generating a list of  InputPartition for parallel reads from Spark executors.          |
| InputPartition       | Each  InputPartition  is responsible for creating a data reader to read data into one RDD partition.  InputPartitions are serialized and sent to executors, then the reader is created on executors to do the actual reading. |
| InputPartitionReader | Responsible for reading data into an RDD partition.                                                                                                                                                                           |

## Workflow

This example implements a DataSourceReader that returns predefined data as InputPartitions stored in-memory with a given schema. The following interfaces need to be implemented for DataSourceReader. The DataSourceReader implementation runs on the Spark driver and plans the execution of Spark executors reading the data in InputPartitions.

The InputPartitions are read in parallel by different Spark executors using the InputPartitionReader implementation, which returns the records in Spark’s InternalRow format. The InputPartitionReader is essentially implemented to return an iterator of the records scanned from the underlying data store.


### 1. Define the marker interface DataSourceV2 and mixed with ReadSupport.

User need to create an entry point for Spark to recognize and use the connector. 
To achieve it, user will implement a DataSourceV2 class mixed with ReadSupport to support reading workflow.

```
public class MinimalSparkConnector implements DataSourceV2, ReadSupport {

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    return new Reader();
  }
}
```

### 2. Define the DataSourceReader.
With ReadSupport, the data reading is achieved by defining readers which extends DataSourceReader interface.
The DataSourceReader interface is responsible for retrieving the actual data source schema and partitions for reading planning and execution in Spark.

In this example, we assumes the data has a simple fixed schema. Also, the reading is simply partitioned to two slots with index from 0-5 and 5-10.
```
class Reader implements DataSourceReader {
  private final StructType schema = new StructType().add("i", "int").add("j", "int");

  @Override
  public StructType readSchema() {
    return schema;
  }

  @Override
  public List<InputPartition<InternalRow>> planInputPartitions() {
    return java.util.Arrays.asList(
            new JavaSimpleInputPartition(0, 5),
            new JavaSimpleInputPartition(5, 10));
  }
}
```
### 3. Implements the InputPartition[InternalRow] interface for partitioned reading.
Each InputPartition will be responsible for reading a part of the data and delegate the real reading job to the InputPartitionReader it created.
```
class JavaSimpleInputPartition implements InputPartition<InternalRow> {

  private int start;
  private int end;

  JavaSimpleInputPartition(int start, int end) {
    this.start = start;
    this.end = end;
  }

  @Override
  public InputPartitionReader<InternalRow> createPartitionReader() {
    return new JavaSimpleInputPartitionReader(start - 1, end);
  }
}
```
### 4. Implements the InputPartitionReader[InternalRow] to read data.
The real reading is carried over by InputPartitionReader and it will output data for a RDD partition. It has three APIs:

* next() - proceed to next record
* get() - return the current record
* close() - inherited from Closeable for clean-up after reading

In this example, we are not actually reading from any data source.
For each partition, we are simulating it by iterating each index *i* and return a corresponding data record *{i, -i}*.
```
class JavaSimpleInputPartitionReader implements InputPartitionReader<InternalRow> {

  private int start;
  private int end;

  JavaSimpleInputPartitionReader(int start, int end) {
    this.start = start;
    this.end = end;
  }

  @Override
  public boolean next() {
    start += 1;
    return start < end;
  }

  @Override
  public InternalRow get() {
    return new GenericInternalRow(new Object[] {start, -start});
  }

  @Override
  public void close() throws IOException {
  }
}
```
### 5. Plug-in and read using the connector with Glue.
You can plug the connectors based on the Spark DataSource API into AWS Glue Spark runtime as follows. You need to supply the connection_type for custom.spark and an AWS Glue catalog connection containing the reader options, such as user name and password. AWS Glue Spark runtime automatically converts the data source into a Glue DynamicFrame.
User can append more input as options to the data source if necessary. 
Please see [MinimalSparkConnectorTest](MinimalSparkConnectorTest.scala) for the full code example.
```
    val optionsMap = Map(
      "className" -> "MinimalSparkConnector"
    )
    val  datasource = glueContext.getSource(
      connectionType = "custom.spark",
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "")
    val dyf = datasource.getDynamicFrame()
    dyf.show()
    dyf.printSchema()
```
The output Dataframe schema in this example:
```
root
 |-- i: integer (nullable = true)
 |-- j: integer (nullable = true)
```
And the output Dataframe data in this example:
```
+---+---+
|  i|  j|
+---+---+
|  0|  0|
|  1| -1|
|  2| -2|
|  3| -3|
|  4| -4|
|  5| -5|
|  6| -6|
|  7| -7|
|  8| -8|
|  9| -9|
+---+---+
```
### 6. Test with catalog connection
Now you are ready to test your Spark connector with a Glue catalog connection. 
Please follow [Creating Custom Connectors](https://docs.aws.amazon.com/glue/latest/ug/connectors-chapter.html#creating-custom-connectors) to create a CUSTOM connector/connection for your Spark connector.
When you created the CUSTOM Spark connection, apply it to the data source options:
```
    val optionsMap = Map(
      "connectionName" -> "test-with-connection"
    )
    val  datasource = glueContext.getSource(
      connectionType = "custom.spark",
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "")
    val dyf = datasource.getDynamicFrame()
    dyf.show()
    dyf.printSchema()
```
Glue will add extra the connector metadata from the connection (e.g. className, secretId). User can append more input as options to the data source if necessary.

Please see [MinimalSparkConnectorTestWithConnection](MinimalSparkConnectorTestWithConnection.scala) for the full code example. 
The output of the flow with a connection should be the same.

Note: To test marketplace workflow for the connector in Glue job system, 
please replace the `connectionType` from `custom.jdbc|spark|athena` to `marketplace.jdbc|spark|athena`.


## Intermediate and Advanced Examples
 - [Spark Connector CSV](SparkConnectorCSV.java)

   A Spark connector which uses an Amazon S3 client to read the data in CSV format from a S3 bucket and path supplied as connection options.
   
 - [Spark Connector MySQL](SparkConnectorMySQL.scala)

   A Spark connector shows how to use a JDBC driver to read data from a MySQL source. It also shows how to push down a SQL query to filter records at source and authenticate with the user name and password supplied as connection options.