/*
 * Copyright 2016-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Amazon Software License (the "License"). You may not use
 * this file except in compliance with the License. A copy of the License is
 * located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.glue.marketplace.connector.tpcds

import com.teradata.tpcds

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

/** Create a Reader object.
 *
 * @param scale The scale factor which specifies the amount of TPC-DS dataset.
 * @param table TPC-DS table in its table list.
 * @param numPartitions The number of concurrency for data generation in parallel.
 * @param schema The schema of the requested table.
 */
class TPCDSScanBuilder(scale: Int, table: tpcds.Table, numPartitions: Int, schema: StructType) extends ScanBuilder {
  override def build(): Scan = new TPCDSScan(scale, table, numPartitions, schema)
}

/** Create a batch reader object.
 *
 * @see [[TPCDSScanBuilder]]
 */
class TPCDSScan(
                   scale: Int,
                   table: tpcds.Table,
                   numPartitions: Int,
                   schema: StructType)
  extends Scan with Logging {

  override def readSchema(): StructType = schema // The schema comes from TPCDSTable class

  override def toBatch: Batch =
    new TPCDSBatch(scale, table, numPartitions, schema)
}

/** Batch reader.
 * Create input partitions and partition readers.
 *
 * @see [[TPCDSScanBuilder]]
 */
class TPCDSBatch(
                scale: Int,
                table: tpcds.Table,
                numPartitions: Int,
                schema: StructType
                )
  extends Batch with Logging {
  override def planInputPartitions(): Array[InputPartition] = {
    logInfo(s"The '${table.getName}' data will be generated.")
    val inPartitionRows = new ArrayBuffer[TPCDSInputPartition]()

    // Check if the generated data will be chunked.
    // In the specification of teradata/tpcds library,
    //  if the requested table has the number of rows which is less than a specific number (this number depends on the requested table),
    //  the generated data won't be chunked, has single chunk.
    //  If dataset is generated with single chunk, the generated data whose chunk-number 2 or more returns empty iterator.
    // Then, check if the generated data is chunked or not here. This checking will be used below.
    val isChunk: Boolean = TPCDSUtils.generateChunkIterator(table, scale, numPartitions, 2).hasNext

    // The partitioning logic here has the following THREE patterns:
    // 1. If the data is chunked, the dataset will be generated in parallel based on specified numPartitions.
    // 2. If the data is not chunked, the way of partitioning depends on which is bigger between numPartitions and rows in the generated data.
    //  - 2-1. If numPartitions > the number of rows, the dataset will be generated with single partition (the number of rows is around 10,000).
    //  - 2-2. If numPartitions <= the number of rows, the dataset will be generated with partitions whose number is based on specified numPartitions.
    if(isChunk) {
      val chunkNumbers = (1 to numPartitions).toList
      chunkNumbers.foreach(
        chunkNumber => inPartitionRows.append(
          new TPCDSInputPartition(scale, table, numPartitions, chunkNumber, schema))
      )
    } else {
      logInfo("The generated data has a single chunk.")
      val rowCount = TPCDSUtils.generateChunkIterator(table, scale, numPartitions, 1).length // the row count of the requested table dataset
      rowCount match {
        case l if l < numPartitions => // Check if the specified numPartitions is bigger than the row count in the dataset of the requested table
          logInfo(s"The '$table' data will be generated with one partition because numPartitions is bigger than the row number of generated data.")
          inPartitionRows.append(
            new TPCDSInputPartition(scale, table, numPartitions, 1, schema))
        case _ => // Split single partition data into chunks based on specified numPartitions
          val rowDelta = Math.floor(rowCount / numPartitions).toInt
          for(partition <- 0 until numPartitions) {
            val until = if(partition == numPartitions - 1) rowCount else (partition + 1) * rowDelta
            inPartitionRows.append(
              new TPCDSSingleChunkInputPartition(
                  scale, table, numPartitions, 1, partition * rowDelta, until, schema))
          }
      }
    }
    inPartitionRows.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new TPCDSPartitionReaderFactory
}