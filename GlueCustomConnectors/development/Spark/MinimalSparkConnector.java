/*
 * Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

public class MinimalSparkConnector implements DataSourceV2, ReadSupport, WriteSupport {
    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        return new Reader(options);
    }

    @Override
    public Optional<DataSourceWriter> createWriter(
            String writeUUID, StructType schema, SaveMode mode, DataSourceOptions options) {
        return Optional.of(new Writer(options));
    }
}

class Reader implements DataSourceReader {
    private final StructType schema = new StructType().add("id", "int").add("value", "int");

    Reader(DataSourceOptions options) {
        if (options != null) {
            // simply print out passed-in options, you may handle special Glue options like secretId as needed
            options.asMap().forEach((k, v) -> System.out.println((k + " : " + v)));
        }
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        return java.util.Arrays.asList(
                new SimpleInputPartition(0, 5),
                new SimpleInputPartition(5, 10));
    }
}

class SimpleInputPartition implements InputPartition<InternalRow> {
    private int start;
    private int end;

    SimpleInputPartition(int start, int end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        return new SimpleInputPartitionReader(start - 1, end);
    }
}

class SimpleInputPartitionReader implements InputPartitionReader<InternalRow> {
    private int start;
    private int end;

    SimpleInputPartitionReader(int start, int end) {
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

class JavaSimpleDataWriterFactory implements DataWriterFactory<InternalRow> {
    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId){
        return new JavaSimpleDataWriter(partitionId, taskId, epochId);
    }
}

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
