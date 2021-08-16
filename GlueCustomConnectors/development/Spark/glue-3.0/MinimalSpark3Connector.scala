import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.io.IOException
import collection.JavaConverters._

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


/* Read */
class SimpleScanBuilder(schema: StructType) extends ScanBuilder {
  override def build(): Scan = new SimpleScan(schema)
}

class SimpleScan(schema: StructType) extends Scan {
  override def readSchema(): StructType = schema

  override def toBatch: Batch = new SimpleBatch()
}


class SimpleBatch extends Batch {
  override def planInputPartitions(): Array[InputPartition] = {
    Array(
      new SimpleInputPartition(0, 5),
      new SimpleInputPartition(5, 10)
    )
  }

  override def createReaderFactory(): PartitionReaderFactory = new SimpleReaderFactory
}

class SimpleInputPartition(var start: Int, var end: Int) extends InputPartition {
  override def preferredLocations(): Array[String] = super.preferredLocations()
}

class SimpleReaderFactory extends PartitionReaderFactory {
  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] =
    new SimplePartitionReader(inputPartition.asInstanceOf[SimpleInputPartition])
}

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

/* Write */
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

class SimpleDataWriterFactory extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new SimpleDataWriter(partitionId, taskId)
}

class SimpleDataWriter(partitionId: Int, taskId: Long) extends DataWriter[InternalRow] {
  @throws[IOException]
  override def abort(): Unit = {}

  @throws[IOException]
  override def commit(): WriterCommitMessage = null

  @throws[IOException]
  override def write(record: InternalRow): Unit = {
    // In this sample code, this part simply prints records for testing-purpose.
    println(s"write a record with id : ${record.getInt(0)} and value: ${record.getInt(1)} for partitionId: $partitionId by taskId: $taskId")
  }

  @throws[IOException]
  override def close(): Unit = {}
}