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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.complex.reader.FieldReader;
//DO NOT REMOVE - this will not be _unused_ when customers go through the tutorial and uncomment
//the TODOs
import org.apache.arrow.vector.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class is part of an tutorial that will walk you through how to build a connector for your
 * custom data source. The README for this module (athena-example) will guide you through preparing
 * your development environment, modifying this example Metadatahandler, building, deploying, and then
 * using your new source in an Athena query.
 * <p>
 * More specifically, this class is responsible for providing Athena with metadata about the schemas (aka databases),
 * tables, and table partitions that your source contains. Lastly, this class tells Athena how to split up reads against
 * this source. This gives you control over the level of performance and parallelism your source can support.
 * <p>
 * For more examples, please see the other connectors in this repository (e.g. athena-cloudwatch, athena-docdb, etc...)
 */
public class ExampleMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleMetadataHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "example";

    public ExampleMetadataHandler()
    {
        super(SOURCE_TYPE);
    }

    @VisibleForTesting
    protected ExampleMetadataHandler(EncryptionKeyFactory keyFactory,
                                     AWSSecretsManager awsSecretsManager,
                                     AmazonAthena athena,
                                     String spillBucket,
                                     String spillPrefix)
    {
        super(keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
    }

    /**
     * Used to get the list of schemas (aka databases) that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @return A ListSchemasResponse which primarily contains a Set<String> of schema names and a catalog name
     * corresponding the Athena catalog that was queried.
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        logger.info("doListSchemaNames: enter - " + request);

        Set<String> schemas = new HashSet<>();

        /**
         * TODO: Add schemas, example below
         *
         schemas.add("schema1");
         *
         */

        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    /**
     * Used to get the list of tables that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog and database they are querying.
     * @return A ListTablesResponse which primarily contains a List<TableName> enumerating the tables in this
     * catalog, database tuple. It also contains the catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
    {
        logger.info("doListTables: enter - " + request);

        List<TableName> tables = new ArrayList<>();

        /**
         * TODO: Add tables for the requested schema, example below
         *
         tables.add(new TableName(request.getSchemaName(), "table1"));
         *
         */

        return new ListTablesResponse(request.getCatalogName(), tables);
    }

    /**
     * Used to get definition (field names, types, descriptions, etc...) of a Table.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @return A GetTableResponse which primarily contains:
     * 1. An Apache Arrow Schema object describing the table's columns, types, and descriptions.
     * 2. A Set<String> of partition column names (or empty if the table isn't partitioned).
     * 3. A TableName object confirming the schema and table name the response is for.
     * 4. A catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
    {
        logger.info("doGetTable: enter - " + request);

        Set<String> partitionColNames = new HashSet<>();

        /**
         * TODO: Add partitions columns, example below.
         *
         partitionColNames.add("year");
         partitionColNames.add("month");
         partitionColNames.add("day");
         *
         */

        SchemaBuilder tableSchemaBuilder = SchemaBuilder.newBuilder();

        /**
         * TODO: Generate a schema for the requested table.
         *
         tableSchemaBuilder.addIntField("year")
         .addIntField("month")
         .addIntField("day")
         .addStringField("account_id")
         .addStringField("encrypted_payload")
         .addStructField("transaction")
         .addChildField("transaction", "id", Types.MinorType.INT.getType())
         .addChildField("transaction", "completed", Types.MinorType.BIT.getType())
         //Metadata who's name matches a column name
         //is interpreted as the description of that
         //column when you run "show tables" queries.
         .addMetadata("year", "The year that the payment took place in.")
         .addMetadata("month", "The month that the payment took place in.")
         .addMetadata("day", "The day that the payment took place in.")
         .addMetadata("account_id", "The account_id used for this payment.")
         .addMetadata("encrypted_payload", "A special encrypted payload.")
         .addMetadata("transaction", "The payment transaction details.")
         //This metadata field is for our own use, Athena will ignore and pass along fields it doesn't expect.
         //we will use this later when we implement doGetTableLayout(...)
         .addMetadata("partitionCols", "year,month,day");
         *
         */

        return new GetTableResponse(request.getCatalogName(),
                request.getTableName(),
                tableSchemaBuilder.build(),
                partitionColNames);
    }

    /**
     * Used to get the partitions that must be read from the request table in order to satisfy the requested predicate.
     *
     * @param blockWriter Used to write rows (partitions) into the Apache Arrow response.
     * @param request Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @note Partitions are partially opaque to Amazon Athena in that it only understands your partition columns and
     * how to filter out partitions that do not meet the query's constraints. Any additional columns you add to the
     * partition data are ignored by Athena but passed on to calls on GetSplits.
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        // the loop structure here is simplified for demonstration only, you can expand the time range but also make
        // sure you upload corresponding data to the S3 path for new partitions.
        for (int year = 2017; year < 2018; year++) {
            for (int month = 11; month < 12; month++) {
                for (int day = 1; day < 2; day++) {

                    final int yearVal = year;
                    final int monthVal = month;
                    final int dayVal = day;
                    /**
                     * TODO: If the partition represented by this year,month,day offer the values to the block
                     * and check if they all passed constraints. The Block has been configured to automatically
                     * apply our partition pruning constraints.
                     *
                     blockWriter.writeRows((Block block, int row) -> {
                     boolean matched = true;
                     matched &= block.setValue("year", row, yearVal);
                     matched &= block.setValue("month", row, monthVal);
                     matched &= block.setValue("day", row, dayVal);
                     //If all fields matches then we wrote 1 row during this call so we return 1
                     return matched ? 1 : 0;
                     });
                     *
                     */
                }
            }
        }
    }

    /**
     * Used to split-up the reads required to scan the requested batch of partition(s).
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details of the catalog, database, table, andpartition(s) being queried as well as
     * any filter predicate.
     * @return A GetSplitsResponse which primarily contains:
     * 1. A Set<Split> which represent read operations Amazon Athena must perform by calling your read function.
     * 2. (Optional) A continuation token which allows you to paginate the generation of splits for large queries.
     * @note A Split is a mostly opaque object to Amazon Athena. Amazon Athena will use the optional SpillLocation and
     * optional EncryptionKey for pipelined reads but all properties you set on the Split are passed to your read
     * function to help you perform the read.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        logger.info("doGetSplits: enter - " + request);

        String catalogName = request.getCatalogName();
        Set<Split> splits = new HashSet<>();

        Block partitions = request.getPartitions();

        FieldReader day = partitions.getFieldReader("day");
        FieldReader month = partitions.getFieldReader("month");
        FieldReader year = partitions.getFieldReader("year");
        for (int i = 0; i < partitions.getRowCount(); i++) {
            //Set the readers to the partition row we area on
            year.setPosition(i);
            month.setPosition(i);
            day.setPosition(i);

            /**
             * TODO: For each partition in the request, create 1 or more splits. Splits
             *   are parallelizable units of work. Each represents a part of your table
             *   that needs to be read for the query. Splits are opaque to Athena aside from the
             *   spill location and encryption key. All properties added to a split are solely
             *   for your use when Athena calls your readWithContraints(...) function to perform
             *   the read. In this example we just need to know the partition details (year, month, day).
             *
             Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
             .add("year", String.valueOf(year.readInteger()))
             .add("month", String.valueOf(month.readInteger()))
             .add("day", String.valueOf(day.readInteger()))
             .build();
             splits.add(split);
             *
             */
        }

        logger.info("doGetSplits: exit - " + splits.size());
        return new GetSplitsResponse(catalogName, splits);
    }
}
