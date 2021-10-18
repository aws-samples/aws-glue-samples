package com.amazonaws.services.glue.marketplace.connector.tpcds

import org.scalatest.FunSuite

class TPCDSTableSuite extends FunSuite {
  test("test generated schema") {
    val tpcdsTable = new TPCDSTable(1, "customer", 1)
    assert(tpcdsTable.schema() == TPCDSDataSuiteUtils.customerManualSchema)
  }
}
