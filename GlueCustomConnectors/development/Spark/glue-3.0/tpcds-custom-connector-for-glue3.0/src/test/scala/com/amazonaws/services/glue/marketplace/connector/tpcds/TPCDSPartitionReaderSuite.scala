package com.amazonaws.services.glue.marketplace.connector.tpcds

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}

import org.scalatest.FunSuite


class TPCDSPartitionReaderSuite extends FunSuite {
  private lazy val encoder: ExpressionEncoder[Row] = RowEncoder.apply(TPCDSDataSuiteUtils.customerTableSchema).resolveAndBind()

  test("TPCDSPartitionReader - next - check if the next row exists") {
    val reader = new TPCDSPartitionReader(
      new TPCDSInputPartition(1, TPCDSDataSuiteUtils.customerTable, 1, 1, TPCDSDataSuiteUtils.customerTableSchema))
    assert(reader.next())
  }

  test("TPCDSPartitionReader - get") {
    val seq = Seq(
      1L,
      "AAAAAAAABAAAAAAA",
      980124L,
      7135L,
      32946L,
      2452238L,
      2452208L,
      "Mr.",
      "Javier",
      "Lewis",
      "Y",
      9,
      12,
      1936,
      "CHILE",
      null,
      "Javier.Lewis@VFAxlnZEvOx.org",
      2452508L)
    val serializer = encoder.createSerializer()
    val row = serializer.apply(Row.fromSeq(seq))
    val reader = new TPCDSPartitionReader(
      new TPCDSInputPartition(1, TPCDSDataSuiteUtils.customerTable, 1, 1, TPCDSDataSuiteUtils.customerTableSchema))
    assert(reader.get() == row)
  }
}


class TPCDSSingleChunkPartitionReaderSuite extends FunSuite {
  private lazy val encoder: ExpressionEncoder[Row] = RowEncoder.apply(TPCDSDataSuiteUtils.customerTableSchema).resolveAndBind()

  test("TPCDSSingleChunkPartitionReader - next - check if the next row exists") {
    val reader = new TPCDSSingleChunkPartitionReader(
      new TPCDSSingleChunkInputPartition(
        1,
        TPCDSDataSuiteUtils.customerTable,
        1,
        1,
        1,
        2,
        TPCDSDataSuiteUtils.customerTableSchema))
    assert(reader.next())
  }

  test("TPCDSSingleChunkPartitionReader - get - check if a generated row is correct") {
    val seq = Seq(
      2L,
      "AAAAAAAACAAAAAAA",
      819667L,
      1461L,
      31655L,
      2452318L,
      2452288L,
      "Dr.",
      "Amy",
      "Moses",
      "Y",
      9,
      4,
      1966,
      "TOGO",
      null,
      "Amy.Moses@Ovk9KjHH.com",
      2452318L)
    val serializer = encoder.createSerializer()
    val row = serializer.apply(Row.fromSeq(seq))
    val reader = new TPCDSSingleChunkPartitionReader(
      new TPCDSSingleChunkInputPartition(1, TPCDSDataSuiteUtils.customerTable, 1, 1, 1, 2, TPCDSDataSuiteUtils.customerTableSchema))
    assert(reader.get() == row)
  }
}
