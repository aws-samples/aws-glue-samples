/*
 * Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
import java.io.IOException;
import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;

import org.apache.spark.sql.types.StructType;

public class MinimalSparkConnector implements DataSourceV2, ReadSupport {

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        return new Reader(options);
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
