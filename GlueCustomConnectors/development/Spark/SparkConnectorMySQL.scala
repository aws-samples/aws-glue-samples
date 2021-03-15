/*
 * Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
import collection.JavaConverters._
import java.sql.{Connection, DriverManager, ResultSet}
import java.util
import java.util.Optional

import org.apache.spark.internal.Logging

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * A simple Spark DataSource V2 with read and write support, the connector will connect to
  * a local MySQL database and its employee table for reading/writing.
  */
class JdbcSourceV2 extends DataSourceV2 with ReadSupport with WriteSupport {

  override def createReader(options: DataSourceOptions): JdbcDataSourceReader =
    new JdbcDataSourceReader(
      options.get("url").get(),
      options.get("user").get(),
      options.get("password").get(),
      options.get("table").get()
    )

  override def createWriter(jobId: String, schema: StructType, mode: SaveMode,
                            options: DataSourceOptions): Optional[DataSourceWriter] = {
    Optional.of(new MysqlDataSourceWriter(
      options.get("url").get(),
      options.get("user").get(),
      options.get("password").get(),
      options.get("table").get(),
      schema)
    )
  }
}

class JdbcDataSourceReader(url: String,
                           user: String,
                           password: String,
                           table: String)
  extends DataSourceReader with SupportsPushDownRequiredColumns with SupportsPushDownFilters {
  // Assuming a fixed schema on premise.
  var requiredSchema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("emp_name", StringType),
    StructField("dep_name", StringType),
    StructField("salary", DecimalType(7, 2)),
    StructField("age", DecimalType(3, 0))
  ))

  var filters = Array.empty[Filter]
  var wheres = Array.empty[String]

  def readSchema: StructType = requiredSchema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val columns = requiredSchema.fields.map(_.name)
    Seq((1, 6), (7, 100)).map { case (minId, maxId) =>
      val partition = s"id BETWEEN $minId AND $maxId"
      new JdbcInputPartition(url, user, password, table, columns, wheres, partition)
        .asInstanceOf[InputPartition[InternalRow]]
    }.toList.asJava
  }

  def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val supported = ListBuffer.empty[Filter]
    val unsupported = ListBuffer.empty[Filter]
    val wheres = ListBuffer.empty[String]

    filters.foreach {
      case filter: EqualTo =>
        supported += filter
        wheres += s"${filter.attribute} = '${filter.value}'"
      case filter => unsupported += filter
    }

    this.filters = supported.toArray
    this.wheres = wheres.toArray
    unsupported.toArray
  }

  def pushedFilters: Array[Filter] = filters
}


class JdbcInputPartition(url: String,
                         user: String,
                         password: String,
                         table: String,
                         columns: Seq[String],
                         wheres: Seq[String],
                         partition: String)
  extends InputPartition[InternalRow] {

  def createPartitionReader(): JdbcDataReader =
    new JdbcDataReader(url, user, password, table, columns, wheres, partition)
}


class JdbcDataReader(
                      url: String,
                      user: String,
                      password: String,
                      table: String,
                      columns: Seq[String],
                      wheres: Seq[String],
                      partition: String)
  extends InputPartitionReader[InternalRow] {
  // scalastyle:off
  Class.forName("com.mysql.jdbc.Driver")
  // scalastyle:on
  private var conn: Connection = null
  private var rs: ResultSet = null

  def next(): Boolean = {
    if (rs == null) {
      conn = DriverManager.getConnection(url, user, password)

      val sqlBuilder = new StringBuilder()
      sqlBuilder ++= s"SELECT ${columns.mkString(", ")} FROM $table WHERE $partition"
      if (wheres.nonEmpty) {
        sqlBuilder ++= " AND " + wheres.mkString(" AND ")
      }
      val sql = sqlBuilder.toString

      val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY)
      stmt.setFetchSize(1000)
      rs = stmt.executeQuery()
    }

    rs.next()
  }

  def get(): InternalRow = {
    val values = columns.map {
      case "id" => rs.getInt("id")
      case "emp_name" => UTF8String.fromString(rs.getString("emp_name"))
      case "dep_name" => UTF8String.fromString(rs.getString("dep_name"))
      case "salary" => Decimal(rs.getBigDecimal("salary"))
      case "age" => Decimal(rs.getBigDecimal("age"))
    }
    InternalRow.fromSeq(values)
  }

  def close(): Unit = {
    conn.close()
  }
}

class MysqlDataSourceWriter(url: String,
                            user: String,
                            password: String,
                            table: String,
                            rddSchema: StructType) extends DataSourceWriter with Logging {

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new MysqlDataWriterFactory(url, user, password, table, rddSchema)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // logic called if all partition write job succeeds
    log.info("Write succeeded")
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // logic called if write job fails
    log.error("Write failed")
  }
}


class MysqlDataWriterFactory(url: String,
                             user: String,
                             password: String,
                             table: String,
                             rddSchema: StructType)
  extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long):
  DataWriter[InternalRow] = {
    new JdbcDataWriter(url, user, password, table, rddSchema)
  }
}

class JdbcDataWriter(url: String,
                     user: String,
                     password: String,
                     table: String,
                     rddSchema: StructType) extends DataWriter[InternalRow] {
  var connection = DriverManager.getConnection(url, user, password)
  val statement = getInsertStatement(table, rddSchema)
  val numFields = rddSchema.fields.length
  val preparedStatement = connection.prepareStatement(statement)

  override def write(record: InternalRow): Unit = {
    var i = 0
    while (i < numFields) {
      rddSchema.fields(i).dataType match {
        case IntegerType => preparedStatement.setInt(i + 1, record.getInt(i))
        case StringType => preparedStatement.setString(i + 1, record.getString(i))
        case dt: DecimalType =>
          preparedStatement.setBigDecimal(i + 1,
            record.getDecimal(i, dt.precision, dt.scale).toJavaBigDecimal)
      }
      i = i + 1
    }
    preparedStatement.addBatch()
  }

  override def commit(): WriterCommitMessage = {
    preparedStatement.executeBatch()
    closeConnection
    WriteSucceeded
  }

  override def abort(): Unit = {
    closeConnection
  }

  private def closeConnection(): Unit = {
    if (connection != null) {
      connection.close()
      connection = null
    }
  }

  private def getInsertStatement(table: String,
                                 rddSchema: StructType): String = {
    val columns = rddSchema.fields.map(x => x.name).mkString(",")
    s"INSERT INTO $table ($columns) " +
      s"VALUES(${List.fill(rddSchema.fields.length)("?").mkString(",")})"
  }
}

object WriteSucceeded extends WriterCommitMessage