/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.glue.marketplace.connector.tpcds

import com.teradata.tpcds
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType


/** The partition which each Spark task processes.
 * The `preferredLocations` method in InputPartition doesn't need to be overridden, then omitted.
 *
 * @param scale The scale factor which specifies the amount of TPC-DS dataset.
 * @param table TPC-DS table in its table list.
 * @param numPartitions The number of concurrency for data generation in parallel.
 * @param chunkNumber Chunked dataset number. This is less than or equal to numPartitions/paralellism.
 * @param schema The schema of the requested table.
 */
class TPCDSInputPartition(
                           val scale: Int,
                           val table: tpcds.Table,
                           val numPartitions: Int,
                           val chunkNumber: Int,
                           val schema: StructType) extends InputPartition

/** The partition for single chunk dataset.
 *
 * @see [[TPCDSInputPartition]]
 * @param start The starting point which is passed to the partition reader object,
 *               and from which the partition reader returns the partial TPC-DS dataset from the iterator.
 * @param end The end point which is passed to the partition reader object,
 *            and from which the partition reader returns the partial TPC-DS dataset from the iterator.
 */
class TPCDSSingleChunkInputPartition(
                                      scale: Int,
                                      table: tpcds.Table,
                                      numPartitions: Int,
                                      chunkNumber: Int,
                                      val start: Int,
                                      val end: Int,
                                      schema: StructType
                                    )
  extends TPCDSInputPartition(scale, table, numPartitions, chunkNumber, schema)
