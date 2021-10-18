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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

/** Create a TPC-DS partition reader factory.
 * This factory creates a specific reader based on the type of input partition.
 * If the type of input partition is neither `TPCDSSingleChunkInputPartition` nor `TPCDSInputPartition`,
 *  this factory throws the exception.
 * @see [[TPCDSBatch]]
 */
class TPCDSPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] = inputPartition match {
    case s: TPCDSSingleChunkInputPartition =>  new TPCDSSingleChunkPartitionReader(inputPartition.asInstanceOf[TPCDSSingleChunkInputPartition])
    case i: TPCDSInputPartition => new TPCDSPartitionReader(inputPartition.asInstanceOf[TPCDSInputPartition])
    case _ => throw new IllegalArgumentException(s"Failed to create partition readers due to specifying the wrong input partition.")
  }
}
