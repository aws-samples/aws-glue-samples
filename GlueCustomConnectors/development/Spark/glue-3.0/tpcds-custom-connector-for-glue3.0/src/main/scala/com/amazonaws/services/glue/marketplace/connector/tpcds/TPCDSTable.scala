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
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/** A logical representation of TPC-DS table which
 * specifies requested table schema and connector abilities (the connector supports BATCH_READ only now), and
 * creates ScanBuilder.
 *
 * @param scale The scale factor which specifies the amount of TPC-DS dataset.
 * @param tableName The table name from TPC-DS table list.
 * @param numPartitions The number of concurrency for data generation in parallel.
 */
class TPCDSTable(scale: Int, tableName: String, numPartitions: Int) extends Table with SupportsRead {
  private val table: tpcds.Table = TPCDSUtils.extractTable(tableName)

  override def name(): String = tableName

  override def schema(): StructType = {
    val columns = new ArrayBuffer[StructField]()
    for(c <- table.getColumns) columns += StructField(c.getName, TPCDSUtils.convertColumnType(c))
    new StructType(columns.toArray)
  }

  // Specify connector capabilities such as "batch read", "streaming read", "batch write" etc.
  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava // Currently only support BATCH_READ

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder =
    new TPCDSScanBuilder(scale, table, numPartitions, this.schema())
}
