# Develop Glue Custom Spark Connectors for Glue 1.0/2.0
***This document is for Glue 1.0/2.0.***  
***For the development guide and example code for Glue 3.0 and Spark 3.1.1, you can see in [glue-3.0](./glue-3.0).***

## Introduction

In this doc, we will discuss how to create a Spark connector with Spark DataSource API V2 (Spark 2.4) to read and write data. We use this simple example to give an overview of the Spark DataSource interface implementation. The full code example is [MinimalSparkConnector](MinimalSparkConnector.java) in the same folder.

## Setup Environment

Build a local Scala environment with local Glue ETL maven library: [Developing Locally with Scala](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html). You may also refer to [GlueSparkRuntime](../GlueSparkRuntime/README.md) for more details to custom the local environment for advanced testing.

## Spark DataSource Interfaces

| Interfaces           | Description                                                                                                                                                                                                                   |
|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| DataSourceV2         | The base interface for Spark DataSource v2 API.                                                                                                                                                                               |
| ReadSupport          | A mix-in interface for DataSourceV2 for the connector to provide data reading ability and scan the data from the data source.                                                                                                 |
| DataSourceReader     | A data source reader that is created by ReadSupport to scan the data from this data source. It also supports reading actual schema and generating a list of InputPartition for parallel reads from Spark executors.           |
| InputPartition       | Each  InputPartition  is responsible for creating a data reader to read data into one RDD partition.  InputPartitions are serialized and sent to executors, then the reader is created on executors to do the actual reading. |
| InputPartitionReader | Responsible for reading data into an RDD partition.                                                                                                                                                                           |
| WriteSupport         | A mix-in interface for DataSourceV2 for the connector to provide data writing ability and save the data to the data source.                                                                                                   |
| DataSourceWriter     | A data source writer that is created by WriteSupport to write the data to this data source. It create a DataWriterFactory, serialize and send it to all the partitions of the input data(RDD) to create the DataWriter and conduct the real data writing. |
| DataWriterFactory    | A factory of DataWriter which creates and initializes the actual data writer at executor side.                                                                                                 |
| DataWriter           | Responsible for writing data for an input RDD partition.                                                                                                |

## Workflow

This example implements a DataSourceReader that returns predefined data as InputPartitions stored in-memory with a given schema. The DataSourceReader implementation runs on the Spark driver and plans the execution of Spark executors reading the data in InputPartitions.

The InputPartitions are read in parallel by different Spark executors using the InputPartitionReader implementation, which returns the records in Spark’s InternalRow format. The InputPartitionReader is essentially implemented to return an iterator of the records scanned from the underlying data store.

The example also implements a DataSourceWriter that simulates writing data. The DataSourceWriter implementation runs on the Spark driver and create a DataWriterFactory, serialize and send it to all the partitions of the input data(RDD) to create the DataWriter to conduct the real data writing work.

Each Spark task has one exclusive DataWriter in which write() is called for each record in the input RDD partition.

### 1. Define the marker interface DataSourceV2 and mixed with ReadSupport and WriteSupport.

User need to create an entry point for Spark to recognize and use the connector. To achieve it, user will implement a DataSourceV2 class mixed with ReadSupport to support reading workflow.

```
public class MinimalSparkConnector implements DataSourceV2, ReadSupport, WriteSupport {

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    return new Reader();
  }
  
  @Override
  public Optional<DataSourceWriter> createWriter(
    String writeUUID, StructType schema, SaveMode mode, DataSourceOptions options) {
      return Optional.of(new Writer(options));
  }
}
```

### 2. Define the DataSourceReader.
With ReadSupport, the data reading is achieved by defining readers which extends DataSourceReader interface. The DataSourceReader interface is responsible for retrieving the actual data source schema and partitions for reading planning and execution in Spark.

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

In this example, we are not actually reading from any data source. For each partition, we are simulating it by iterating each index *i* and return a corresponding data record *{i, -i}*.
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

### 5. Define the DataSourceWriter.
Now let's take a look at the interfaces to implement for writing support.

With WriteSupport, the data writing is achieved by defining writers which extends DataSourceWriter interface. The DataSourceWriter interface is responsible for creating a DataWriterFactory which will be serialized and sent to all the partitions of the input data(RDD) to create the real data writer which conducts the real data writing.

It has five APIs:

* createWriterFactory() - creates a writer factory which will be serialized and sent to executors.
* commit() - commits this writing job with a list of commit messages. The commit messages are collected from successful data writers and are produced by DataWriter.commit()
* abort() - aborts this writing job because some data writers are failed and keep failing when retry, or the Spark job fails with some unknown reasons, or onDataWriterCommit() fails, or commit() fails.
* useCommitCoordinator() - returns whether Spark should use the commit coordinator to ensure that at most one task for each partition commits. It defaults to be true.
* onDataWriterCommit() - handles a commit message on receiving from a successful data writer. It default does nothing.

In this minimal example, we will only implement createWriterFactory() to highlight the basic workflow, the other methods should be customized based on the real use cases.

```
class Writer implements DataSourceWriter {
    Writer(DataSourceOptions options) {
        if (options != null) {
            // simply print out passed-in options, you may handle special Glue options like secretId as needed
            options.asMap().forEach((k, v) -> System.out.println((k + " : " + v)));
        }
    }

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return new JavaSimpleDataWriterFactory();
    }

    @Override
    public void commit(WriterCommitMessage[] messages) { }

    @Override
    public void abort(WriterCommitMessage[] messages) { }
}
```
### 6. Implements the DataWriterFactory[InternalRow] interface for partitioned writing.
DataWriterFactory created by DataSourceWriter.createWriterFactory() will be serialized and sent to executors to create DataWriter for each partition to conduct the real data writing work.

```
class JavaSimpleDataWriterFactory implements DataWriterFactory<InternalRow> {
    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId){
        return new JavaSimpleDataWriter(partitionId, taskId, epochId);
    }
}
```
### 7. Implements the DataWriter<InternalRow> to write data.
The real writing is carried over by DataWriter for each Spark task. It has three APIs:

* write() - write one record
* commit() - commits this writer after all records are written successfully, returns a commit message which will be sent back to driver side and passed to DataSourceWriter.commit()
* abort() - aborts this writer if there is one record failed to write, commit() is failed

In this example, we are not actually writing to any data source. Instead, we are simulating the writing by simply printing out each record.
```
class JavaSimpleDataWriter implements DataWriter<InternalRow> {
    private int partitionId;
    private long taskId;
    private long epochId;

    JavaSimpleDataWriter(int partitionId, long taskId, long epochId) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        // simply print out records for demo purpose
        System.out.println("write record with id : " + record.getInt(0) + " and value: " + record.getInt(1));
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        return null;
    }

    @Override
    public void abort() throws IOException { }
}

```

### 8. Plug-in and read using the connector with Glue.
You can plug the connectors based on the Spark DataSource API into AWS Glue Spark runtime as follows. You need to supply the connection_type for custom.spark and an AWS Glue catalog connection containing the reader options, such as user name and password. AWS Glue Spark runtime automatically converts the data source into a Glue DynamicFrame. User can append more input as options to the data source if necessary. 

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
    
    val customSink = glueContext.getSink(
      connectionType = "marketplace.spark",
      connectionOptions = JsonOptions(optionsMap))
    customSink.writeDynamicFrame(dyf)
```
The output Dataframe schema in this example:
```
root
 |-- i: integer (nullable = true)
 |-- j: integer (nullable = true)
```
The output Dataframe data in this example:
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

And the output of writing data in this example:
```
write record with id : 4 and value: -4
write record with id : 1 and value: -1
write record with id : 3 and value: -3
write record with id : 2 and value: -2
write record with id : 0 and value: 0
write record with id : 5 and value: -5
write record with id : 9 and value: -9
write record with id : 6 and value: -6
write record with id : 7 and value: -7
write record with id : 8 and value: -8
```
### 9. Test with catalog connection
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
    
    val datasink = glueContext.getSink(
      connectionType = "custom.spark",
      connectionOptions = JsonOptions(optionsMap),
      transformationContext = "datasink")
    datasink.writeDynamicFrame(dyf)
```
Glue will add extra the connector metadata from the connection (e.g. className, secretId). User can append more input as options to the data source if necessary.

Please see [MinimalSparkConnectorTestWithConnection](MinimalSparkConnectorTestWithConnection.scala) for the full code example. 
The output of the flow with a connection should be the same.

Note: To test marketplace workflow for the connector in Glue job system, 
please replace the `connectionType` from `custom.jdbc|spark|athena` to `marketplace.jdbc|spark|athena`.


## Intermediate and Advanced Examples
 - [Spark Connector CSV](SparkConnectorCSV.java)

   A Spark connector which uses Amazon S3 client to read the data in CSV format from a S3 bucket with path supplied as connection options, 
   and write data to a S3 bucket with some key prefix supplied as connection options.
   
 - [Spark Connector MySQL](SparkConnectorMySQL.scala)

   A Spark connector shows how to use a JDBC driver to read from and write to a MySQL source. 
   It also shows how to push down a SQL query to filter records at source on data reading and authenticate with the user name and password supplied as connection options.