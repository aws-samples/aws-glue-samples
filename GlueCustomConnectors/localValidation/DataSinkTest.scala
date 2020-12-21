/*
 * Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
package test.scala

import java.sql.DriverManager

import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class DataSinkTest extends FunSuite with BeforeAndAfterEach {
  val conf = new SparkConf().setAppName("LocalTest").setMaster("local")
  var spark: SparkContext = _
  var glueContext: GlueContext = _

  val rootLogger = Logger.getRootLogger() // Remove verbose log
  rootLogger.setLevel(Level.ERROR)

  override def beforeEach(): Unit = {
    spark = new SparkContext(conf)
    glueContext = new GlueContext(spark)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    spark.stop()
  }

  test("test data sink"){
    // set test table name
    val tableName = "test_write__c"

    // set data source connection options
    val sinkOptionsMap = Map(
      "dbtable" -> tableName,
      "secretId" -> "secret",
      "className" -> "partner.jdbc.salesforce.SalesforceDriver",
      "url" -> "jdbc:salesforce:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
    )

    // set raw jdbc driver connection
    val SecurityToken = "securitytoken" // for Salesforce remote access
    val user = "user"
    val password = "password"
    val url = s"jdbc:salesforce:user=${user};Password=${password};SecurityToken=${SecurityToken};"
    Class.forName(sinkOptionsMap("className"))
    val conn = DriverManager.getConnection(url)

    try{
      cleanTb(conn, tableName)
      val sparkSession = glueContext.getSparkSession
      import sparkSession.implicits._
      var jsonStrList = List[String]()
      for( a <- 1 to 1000){
        val jsonStr = s"""{ "id__c": "$a"}"""
        jsonStrList = jsonStr :: jsonStrList
      }

      val dyf = DynamicFrame(sparkSession.read.json(jsonStrList.toDS), glueContext)
      val customSink = glueContext.getSink(
        connectionType = "custom.jdbc", // spark: "custom.spark"
        connectionOptions = JsonOptions(sinkOptionsMap))
      customSink.writeDynamicFrame(dyf)

      val count = countRowsInTb(conn, tableName)
      assert(count == 1000)
    } finally {
      cleanTb(conn, tableName)
      conn.close()
    }
  }

  def cleanTb(conn: java.sql.Connection, tbName:String): Unit ={
    val statement = conn.createStatement
    val sql = s"DELETE FROM ${tbName}"
    statement.executeUpdate(sql)
    statement.close()
  }

  def countRowsInTb(conn: java.sql.Connection, tbName:String)={
    val statement = conn.createStatement
    val sql = s"SELECT COUNT(*) AS count FROM ${tbName}"
    val result = statement.executeQuery(sql)
    var rowsCount = 0
    while(result.next()){
      rowsCount += result.getInt(1)
    }
    statement.close()
    rowsCount
  }
}