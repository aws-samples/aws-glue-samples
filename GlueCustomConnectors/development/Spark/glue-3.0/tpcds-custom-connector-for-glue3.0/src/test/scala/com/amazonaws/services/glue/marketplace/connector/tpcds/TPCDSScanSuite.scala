package com.amazonaws.services.glue.marketplace.connector.tpcds

import org.scalatest.FunSuite


class TPCDSScanSuite extends FunSuite {
  test("TPCDSScan - test generated schema") {
    val tpcdsScan = new TPCDSScan(1, TPCDSDataSuiteUtils.customerTable, 1, TPCDSDataSuiteUtils.customerTableSchema)
    assert(tpcdsScan.readSchema() == TPCDSDataSuiteUtils.customerManualSchema)
  }

  test("TPCDSBatch - test partition creation - single chunk data is generated") {
    val partition = new TPCDSBatch(1, TPCDSDataSuiteUtils.customerTable, 1, TPCDSDataSuiteUtils.customerTableSchema).planInputPartitions()
    assert(partition.length == 1)
  }

  test("TPCDSBatch - test partition creation - rows count is less than numPartitions") {
    val partition = new TPCDSBatch(1, TPCDSDataSuiteUtils.dbgenVersionTable, 1, TPCDSDataSuiteUtils.dbgenVersionTableSchema).planInputPartitions()
    assert(partition.length == 1)
  }

  test("TPCDSBath - test partition creation - single chunk will be split into partitions with numPartitions.") {
    val partition = new TPCDSBatch(1, TPCDSDataSuiteUtils.customerTable, 10, TPCDSDataSuiteUtils.customerTableSchema).planInputPartitions()
    assert(partition.length == 10)
  }

  test("TPCDSBatch - test partition creation - multiple chunk data will be generated.") {
    val partition = new TPCDSBatch(65, TPCDSDataSuiteUtils.customerTable, 10, TPCDSDataSuiteUtils.customerTableSchema).planInputPartitions()
    assert(partition.length == 10)
  }

}
