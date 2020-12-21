/*
 * Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * A simple Spark DataSource V2 reads from s3 csv files.
 */
public class SparkConnectorCSV implements DataSourceV2, ReadSupport {

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        return new Reader(options);
    }

}

class Reader implements DataSourceReader {
    // Assuming the csv files to be read has a fixed schema on premise with two integer columns name, id.
    private final StructType schema = new StructType()
            .add("name", "string")
            .add("id", "int");
    private String bucket, path;

    Reader(DataSourceOptions options) {
        bucket = options.get("bucket").get();
        path = options.get("path").get();
    }


    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        // parallel reading with simple partition planning - each partition read a specific csv file.
        return java.util.Arrays.asList(
                new JavaSimpleInputPartition(1, bucket, path),
                new JavaSimpleInputPartition(2, bucket, path));
    }
}

class JavaSimpleInputPartition implements InputPartition<InternalRow> {

    private int fileId;
    private String bucket, path;
    JavaSimpleInputPartition(int fileId, String bucket, String path) {
        this.fileId = fileId;
        this.bucket = bucket;
        this.path = path;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        return new JavaSimpleInputPartitionReader(fileId, bucket, path);
    }
}


class JavaSimpleInputPartitionReader implements InputPartitionReader<InternalRow>  {
    BufferedReader reader;
    String line = null;

    JavaSimpleInputPartitionReader(int filedId, String bucket, String path) {
        AmazonS3 s3Client = AmazonS3Client.builder()
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .withRegion(new DefaultAwsRegionProviderChain().getRegion())
                .withForceGlobalBucketAccessEnabled(true)
                .build();
        S3Object s3Object = s3Client
                .getObject(new GetObjectRequest(bucket, String.format("%s%s.csv", path, filedId)));
        reader  = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
    }

    @Override
    public boolean next() {
        try {
            line = reader.readLine();
            return line != null;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InternalRow get() {
        String[] fields = line.split(",");
        return new GenericInternalRow(new Object[] {UTF8String.fromString(fields[0]), Integer.parseInt(fields[1])});
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }
}