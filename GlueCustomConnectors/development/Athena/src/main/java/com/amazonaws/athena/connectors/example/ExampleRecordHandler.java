/*-
 * #%L
 * athena-example
 * %%
 * Copyright (C) 2020 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.example;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

/**
 * This class is part of an tutorial that will walk you through how to build a connector for your
 * custom data source. The README for this module (athena-example) will guide you through preparing
 * your development environment, modifying this example RecordHandler, building, deploying, and then
 * using your new source in an Athena query.
 * <p>
 * More specifically, this class is responsible for providing Athena with actual rows level data from your source. Athena
 * will call readWithConstraint(...) on this class for each 'Split' you generated in ExampleMetadataHandler.
 * <p>
 * For more examples, please see the other connectors in this repository (e.g. athena-cloudwatch, athena-docdb, etc...)
 */
public class ExampleRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleRecordHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "example";

    private AmazonS3 amazonS3;

    public ExampleRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), AmazonAthenaClientBuilder.defaultClient());
    }

    @VisibleForTesting
    protected ExampleRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena amazonAthena)
    {
        super(amazonS3, secretsManager, amazonAthena, SOURCE_TYPE);
        this.amazonS3 = amazonS3;
    }

    /**
     * Used to read the row data associated with the provided Split.
     *
     * @param spiller A BlockSpiller that should be used to write the row data associated with this Split.
     * The BlockSpiller automatically handles chunking the response, encrypting, and spilling to S3.
     * @param recordsRequest Details of the read request, including:
     * 1. The Split
     * 2. The Catalog, Database, and Table the read request is for.
     * 3. The filtering predicate (if any)
     * 4. The columns required for projection.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @throws IOException
     * @note Avoid writing >10 rows per-call to BlockSpiller.writeRow(...) because this will limit the BlockSpiller's
     * ability to control Block size. The resulting increase in Block size may cause failures and reduced performance.
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            throws IOException
    {
        logger.info("readWithConstraint: enter - " + recordsRequest.getSplit());

        Split split = recordsRequest.getSplit();
        int splitYear = 0;
        int splitMonth = 0;
        int splitDay = 0;

        /**
         * TODO: Extract information about what we need to read from the split. If you are following the tutorial
         *  this is basically the partition column values for year, month, day.
         *
         splitYear = split.getPropertyAsInt("year");
         splitMonth = split.getPropertyAsInt("month");
         splitDay = split.getPropertyAsInt("day");
         *
         */

        String dataBucket = null;
        /**
         * TODO: Get the data bucket from the env variable set by athena-example.yaml
         *
         dataBucket = System.getenv("data_bucket");
         *
         */

        String dataKey = format("%s/%s/%s/sample_data.csv", splitYear, splitMonth, splitDay);

        BufferedReader s3Reader = openS3File(dataBucket, dataKey);
        if (s3Reader == null) {
            //There is no data to read for this split.
            return;
        }

        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());

        /**
         * TODO: Add extractors for each field to our RowWRiterBuilder, the RowWriterBuilder will then 'generate'
         * optomized code for converting our data to Apache Arrow, automatically minimizing memory overhead, code
         * branches, etc... Later in the code when we call RowWriter for each line in our S3 file
         *
         builder.withExtractor("year", (IntExtractor) (Object context, NullableIntHolder value) -> {
         value.isSet = 1;
         value.value = Integer.parseInt(((String[]) context)[0]);
         });
         builder.withExtractor("month", (IntExtractor) (Object context, NullableIntHolder value) -> {
         value.isSet = 1;
         value.value = Integer.parseInt(((String[]) context)[1]);
         });
         builder.withExtractor("day", (IntExtractor) (Object context, NullableIntHolder value) -> {
         value.isSet = 1;
         value.value = Integer.parseInt(((String[]) context)[2]);
         });
         builder.withExtractor("encrypted_payload", (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
         value.isSet = 1;
         value.value = ((String[]) context)[6];
         });
         */

        /**
         * TODO: The account_id field is a sensitive field, so we'd like to mask it to the last 4 before
         *  returning it to Athena. Note that this will mean you can only filter (where/having)
         *  on the masked value from Athena.
         *
         builder.withExtractor("account_id", (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
         value.isSet = 1;
         String accountId = ((String[]) context)[3];
         value.value = accountId.length() > 4 ? accountId.substring(accountId.length() - 4) : accountId;
         });
         */

        /**
         * TODO: Write data for our transaction STRUCT:
         * For complex types like List and Struct, we can build a Map to conveniently set nested values
         *
         builder.withFieldWriterFactory("transaction",
         (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
         (Object context, int rowNum) -> {
         Map<String, Object> eventMap = new HashMap<>();
         eventMap.put("id", Integer.parseInt(((String[])context)[4]));
         eventMap.put("completed", Boolean.parseBoolean(((String[])context)[5]));
         BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, eventMap);
         return true;    //we don't yet support predicate pushdown on complex types
         });
         */

        //Used some basic code-gen to optimize how we generate response data.
        GeneratedRowWriter rowWriter = builder.build();

        //We read the transaction data line by line from our S3 object.
        String line;
        while ((line = s3Reader.readLine()) != null) {
            logger.info("readWithConstraint: processing line " + line);

            //The sample_data.csv file is structured as year,month,day,account_id,transaction.id,transaction.complete
            String[] lineParts = line.split(",");

            //We use the provided BlockSpiller to write our row data into the response. This utility is provided by
            //the Amazon Athena Query Federation SDK and automatically handles breaking the data into reasonably sized
            //chunks, encrypting it, and spilling to S3 if we've enabled these features.
            spiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, lineParts) ? 1 : 0);
        }
    }

    /**
     * Helper function for checking the existence of and opening S3 Objects for read.
     */
    private BufferedReader openS3File(String bucket, String key)
    {
        logger.info("openS3File: opening file " + bucket + ":" + key);
        if (amazonS3.doesObjectExist(bucket, key)) {
            S3Object obj = amazonS3.getObject(bucket, key);
            logger.info("openS3File: opened file " + bucket + ":" + key);
            return new BufferedReader(new InputStreamReader(obj.getObjectContent()));
        }
        return null;
    }
}
