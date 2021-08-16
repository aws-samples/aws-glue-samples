# Develop Glue Custom Spark Connectors for Glue 3.0

## Introduction
This document shows how to develop a connector supporting Glue 3.0 and Spark 3.1.1 for reading and writing data. We’ll use the simple example to show an overview of DataSourceV2 interface implementations in Spark 3. The full code example is [MinimalSpark3Connector](./MinimalSpark3Connector.scala) in the same folder.

## Setup Environment
Build a local Scala environment with local Glue ETL maven library: [Developing Locally with Scala](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html). You may also refer to [GlueSparkRuntime](https://github.com/aws-samples/aws-glue-samples/blob/master/GlueCustomConnectors/development/GlueSparkRuntime/README.md) for more details to custom the local environment for advanced testing.

## DataSourceV2 Interfaces in Spark 3

| Interface                          | Description                                                  |
|------------------------------------|--------------------------------------------------------------|
| `TableProvider`                    | The base interface for DataSourceV2 API. The class name `DefaultSource` which implements this interface is called by Spark. |
| `Table`                            | A logical structured dataset of a data source. This class points where the connector reads/writes. This interface can mix-in `SupportsRead`/`SupportWrite` to provide actual data read/write functionalities. |
| **Reader**                         | **Reader interfaces below**                                  |
| `SupportsRead`                     | A mix-in interface of `Table` which specifies the read capability for connector. This interface creates the `ScanBuilder` which is used to create a scan for batch, micro-batch or continuous processing. |
| `ScanBuilder`                      | An interface to create a `Scan`. You can add pushdown filter functionality by implementing the `SupportsPushDownFilters` interface and mix-in this class. |
| `Scan`                             | A logical representation of a data source scan. The implementation of this class specifies how the reader works such as batch-read, streaming-read etc. |
| *Batch*                            | **For batch read**                                           |
| `Batch`                            | An interface providing the number of partitions the read-data has, and the behaviour of readers.  |
| `PartitionReaderFactory`           | A factory interface used to create the `PartitionReader`.    |
| `PartitionReader`                  | Responsible for reading data into an RDD partition.          |
| `InputPartition`                   | Each InputPartition is responsible for creating a data reader to read data into one RDD partition. InputPartitions are serialized and sent to executors, then the reader is created on executors to do the actual reading. |
| *Streaming*                        | **For streaming read**                                       |
| `MicroBatchStream`                 | An interface for streaming queries with micro-batch mode, which is inherited from `SparkDataStream`. |
| `ContinuousStream`                 | An interface for streaming queries with continuous mode, which is inherited from `SparkDataStream`. |
| `ContinuousPartitionReaderFactory` | A variation on `PartitionReaderFactory` and returns `ContinuousPartitionReader` for continuous streaming processing. |
| `ContinuousPartitionReader`        | A variation on `PartitionReader` for continuous stream processing. |
| **Writer**                         | **Writer interfaces below**                                  |
| `SupportsWrite`                    | A mix-in interface of `Table` which specifies the write capability for connector. This interface creates the `WriteBuilder` which is used to create a write for batch or streaming. |
| `WriteBuilder`                     | Used to create a `BatchWrite` or `StreamingWrite`.           |
| *Batch*                            | **For batch write**                                          |
| `BatchWrite`                       | An interface that defines how to write the data to data source for batch processing.  |
| `DataWriterFactory`                | A factory of `DataWriter` which creates and initializes the actual data writer at executor side. |
| `DataWriter`                       | Responsible for writing data for an input RDD partition.     |
| *Streaming*                        | **For streaming write**                                      |
| `StreamingWrite`                   | An interface that defines how to write the data to data source in streaming queries. |
| `StreamingDataWriterFactory`       | A factory of `DataWriter` which creates and initializes the actual data writer at executor side. |


## Step-by-Step Guide to Developing Connectors for Glue 3.0

The below example simply shows how you implement data source reader and writer for Glue 3.0 with compatibility with Spark 3. The implementation steps can be mainly divided into three parts:
* Data source - defines the entry point of connector, what the data source is and the connector capabilities such as supporting batch-read or batch-write 
* Reader - defines how the connector reads, such as batch-read and streaming-read, and how the data is partitioned.
* Writer - defines how the connector writes, such as batch-write and streaming-write.

This example is based on MinimalSpark3Connector.scala.

### 1. Define the entry point of connector `TableProvider` and the data source representation `Table`
First of all, you need to implement the `TableProvider` and `Table` interfaces, which are the connector entry point and the definition of data source respectively.

#### 1-1. Define `DefaultSource` class by implementing the entry point `TableProvider` interface
You need to create an entry point for Spark to recognize and use the connector. The entry point with `DefaultSource` name will be called by Spark. For example, if you build the connector whose entry class path is `com.example.DefaultSource`, `DefaultSource` class will be called firstly by specifying `com.example` on your Spark code as a class name.

Additionally, if you implement options which are passed from your Spark code, you can handle them by processing the `CaseInsensitiveStringMap` parameter.

```scala
class DefaultSource extends TableProvider {
  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType =
    getTable(
      null,
      Array.empty[Transform],
      caseInsensitiveStringMap.asCaseSensitiveMap()).schema()

  override def getTable(structType: StructType, transforms: Array[Transform], javaProps: util.Map[String, String]): Table = {
    // If you handle special options passed from connection options,
    // you can do it through processing `javaProps`.
    new SimpleTable
  }
}
```

#### 1-2. Implement the data source representation  `Table` with mix-in `SupporsRead` and `SupportsWrite`

You can read/write functionalities to your connector by mix-in `SupportsRead` and `SupportsWrite` for the implementation of the `Table` interface. In addition to these functionalities, you need to set the capabilities of your connectors such as read-only, write-only or both by specifying the `TableCapability` enumerations to the  `capabilities` method.

In this example, the connector supports batch-read and batch-write, not streaming data source.

```scala
class SimpleTable extends Table with SupportsRead with SupportsWrite {
  override def name(): String = this.getClass.toString // Table name from specified option

  override def schema(): StructType =
    new StructType()
      .add("id", "int")
      .add("value", "int")

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE).asJava // Supports BATCH read/write mode

  // Read
  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder =
    new SimpleScanBuilder(this.schema())

  // Write
  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder =
    new SimpleWriteBuilder
}
```

After implementing `TableProvider` and `Table` mixed-in of `SupportsRead`/`SupportsWrite`, you need to implement the actual reader and/or writer part.

### 2. Define reader part
#### 2-1. Implement the `ScanBuilder` and `Scan` as a data source reader
The `ScanBuilder`	interface is an entry point of the reader part and builds the `Scan` which is responsible for retrieving the actual data source schema and creating the data source reader with batch mode, continuous mode and/or micro-batch mode based on the capabilities which you specify in the implementation of `Table` interface above.

This example only supports batch data processing, and its data schema is fixed.

```scala
class SimpleScanBuilder(schema: StructType) extends ScanBuilder {
  override def build(): Scan = new SimpleScan(schema)
}

class SimpleScan(schema: StructType) extends Scan {
  override def readSchema(): StructType = schema

  override def toBatch: Batch = new SimpleBatch()
}
```

#### 2-2. Implement the `Batch`  for creating batch input partitions and partition readers

Actual readers and partitions are created from the class which implements the `Batch` interface. The `Batch` interface provides the number of partitions the read-data has, and the behaviour of readers. 

This example focuses on batch read and assumes the data has a simple fixed schema. Additionally, the number of partitions are two with index from 0-5 and 5-10.

```scala
class SimpleBatch extends Batch {
  override def planInputPartitions(): Array[InputPartition] = {
    Array(
      new SimpleInputPartition(0, 5),
      new SimpleInputPartition(5, 10)
    )
  }

  override def createReaderFactory(): PartitionReaderFactory = new SimplePartitionReaderFactory
}
```


#### 2-3. Implement the `InputPartition` and `PartitionReaderFactory` for partitioned reading

Each `InputPartition` will be responsible for reading a part of the data and delegate the real reading job to the `PartitionReader` which is created by `PartitionReaderFactory`. Class parameters for `SimpleInputPartition` will be used in actual partition reader jobs.

```scala
class SimpleInputPartition(var start: Int, var end: Int) extends InputPartition {
  override def preferredLocations(): Array[String] = super.preferredLocations()
}

class SimplePartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] =
    new SimplePartitionReader(inputPartition.asInstanceOf[SimpleInputPartition])
}
```

#### 2-4. Implement the `PartitionReader[InteranlRow]` to read data
The real reading is carried over by `PartitionReader` and it will output data for a RDD partition. It has three APIs:
* `next()` - proceed to next record
* `get()` - return the current record
* `close()` - inherited from `java.io.Closeable` for clean-up after reading

This example is simulating process-data by iterating each index `i` and return a relevant record `{i, -i}`, instead of actually reading data from a data source.

```scala
class SimplePartitionReader(val simpleInputPartition: SimpleInputPartition) extends PartitionReader[InternalRow] {
  var start: Int = simpleInputPartition.start
  var end: Int = simpleInputPartition.end
  override def next(): Boolean = {
    start = start + 1
    start < end
  }

  override def get(): InternalRow = {
    val row = Array(start, -start)
    InternalRow.fromSeq(row.toSeq)
  }

  @throws[IOException]
  override def close(): Unit = {}
}

```

### 3. Define writer part
#### 3-1. Implement the `WriteBuilder` and `BatchWrite` as a data source writer

You can build batch and streaming writers through  `WriteBuilder.buildForBatch` and `WriteBuilder.buildForStreaming()`. Actual writer factory is created by the class which implements the `BatchWrite` or `StreamingWrite` interface which is built by the `WriteBuilder`. 

In this example, only `BatchWrite`  is implemented because the connector only supports batch read/write.

```scala
class SimpleWriteBuilder extends WriteBuilder {
  override def buildForBatch(): BatchWrite =
    new SimpleBatchWrite
}

class SimpleBatchWrite extends BatchWrite {
  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory =
    new SimpleDataWriterFactory
}
```


#### 3-2. Implement the `DataWriteFactory` for partitioned writing
`DataWriteFactory` created by `BatchWrite.createBatchWriteFactory`  is responsible for creating and initializing the actual data writer on executor side. This writer factory will be serialized and sent to executors, then the relevant data writer will be created by this factory.

```scala
class SimpleDataWriterFactory extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new SimpleDataWriter(partitionId, taskId)
}
```

#### 3-3. Implement the `DataWriter[InternalRow]` to write data
The actual writing is conducted by `DataWriter` for each Spark task. It has four APIs:

* `abort()` - aborts this writer if there is one record failed to write, when `commit` fails
* `write()` - writes a record
* `commit()` - commits this writer after writing all records is successful, then returns a commit message which will be sent back to Driver and passed to the `BatchWrite` or `StreamingWrite`
* `close()` -  inherited from `java.io.Closeable` for clean-up after reading

This example is simulating writing-data by simply showing each record, instead of actually writing records.

```scala
class SimpleDataWriter(partitionId: Int, taskId: Long) extends DataWriter[InternalRow] {
  @throws[IOException]
  override def abort(): Unit = {}

  @throws[IOException]
  override def commit(): WriterCommitMessage = null

  @throws[IOException]
  override def write(record: InternalRow): Unit =
    println(s"write a record with id : ${record.getInt(0)} and value: ${record.getInt(1)} for partitionId: $partitionId by taskId: $taskId")

  @throws[IOException]
  override def close(): Unit = {}
}

```

### 4. Run tests
You can run the same tests which are described in [8. Plug-in and read using the connector with Glue.](https://github.com/aws-samples/aws-glue-samples/blob/master/GlueCustomConnectors/development/Spark/README.md#8-plug-in-and-read-using-the-connector-with-glue) and [9. Test with catalog connection](https://github.com/aws-samples/aws-glue-samples/blob/master/GlueCustomConnectors/development/Spark/README.md#9-test-with-catalog-connection) which are aiming at Spark 2 environment. And, you should be able to get the same results by running those tests. **Please note that you specify the class name as `simple.spark.connector` (NOT `simple.spark.connector.MinimalSpark3Connector`), and use Glue 3.0 runtime.** As described above, `DefaultSource` class will be automatically called in Spark 3.