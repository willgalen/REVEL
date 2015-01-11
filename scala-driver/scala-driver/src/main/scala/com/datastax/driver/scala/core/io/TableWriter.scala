package com.datastax.driver.scala.core.io

import java.io.IOException

import scala.collection._
import com.datastax.driver.core.BoundStatement
import com.datastax.driver.core.{BatchStatement, PreparedStatement, Session}
import com.datastax.driver.scala.core._
import com.datastax.driver.scala.core.conf.WriteConf
import com.datastax.driver.scala.core.utils.{CountingIterator, Logging}
import com.datastax.driver.scala.types.ColumnType

/** Writes RDD data into given Cassandra table.
  * Individual column values are extracted from RDD objects using given [[RowWriter]]
  * Then, data are inserted into Cassandra with batches of CQL INSERT statements.
  * Each RDD partition is processed by a single thread. */
class TableWriter[T] private[datastax] (
    cluster: CassandraCluster,
    tableDef: TableDef,
    rowWriter: RowWriter[T],
    writeConf: WriteConf) extends Serializable with Logging {

  import TableWriter._

  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName
  val columnNames = rowWriter.columnNames diff writeConf.optionPlaceholders
  val columns = columnNames.map(tableDef.columnByName)
  implicit val protocolVersion = cluster.withClusterDo { _.getConfiguration.getProtocolOptions.getProtocolVersionEnum }

  val defaultTTL = writeConf.ttl match {
    case x: StaticWriteOption[Int] => Some(x.value)
    case _: PerRowWriteOption[Int] => None
    case TTLOption.auto => None
  }

  val defaultTimestamp = writeConf.timestamp match {
    case x: StaticWriteOption[Long] => Some(x.value)
    case _: PerRowWriteOption[Long] => None
    case TimestampOption.auto => None
  }

  private def quote(name: String): String = "\"" + name + "\""

  private[datastax] lazy val queryTemplateUsingInsert: String = {
    val quotedColumnNames: Seq[String] = columnNames.map(quote)
    val columnSpec = quotedColumnNames.mkString(", ")
    val valueSpec = quotedColumnNames.map(":" + _).mkString(", ")

    val ttlSpec = writeConf.ttl match {
      case x: PerRowWriteOption[Int] => Some(s"TTL :${x.placeholder}")
      case x: StaticWriteOption[Int] => Some(s"TTL ${x.value}")
      case TTLOption.auto => None
    }

    val timestampSpec = writeConf.timestamp match {
      case x: PerRowWriteOption[Long] => Some(s"TIMESTAMP :${x.placeholder}")
      case x: StaticWriteOption[Long] => Some(s"TIMESTAMP ${x.value}")
      case TimestampOption.auto => None
    }

    val options = List(ttlSpec, timestampSpec).flatten
    val optionsSpec = if (options.nonEmpty) s"USING ${options.mkString(" AND ")}" else ""

    s"INSERT INTO ${quote(keyspaceName)}.${quote(tableName)} ($columnSpec) VALUES ($valueSpec) $optionsSpec".trim
  }

  private lazy val queryTemplateUsingUpdate: String = {
    val (primaryKey, regularColumns) = columns.partition(_.isPrimaryKeyColumn)
    val (counterColumns, nonCounterColumns) = regularColumns.partition(_.isCounterColumn)

    def quotedColumnNames(columns: Seq[ColumnDef]) = columns.map(_.columnName).map(quote)
    val setNonCounterColumnsClause = quotedColumnNames(nonCounterColumns).map(c => s"$c = :$c")
    val setCounterColumnsClause = quotedColumnNames(counterColumns).map(c => s"$c = $c + :$c")
    val setClause = (setNonCounterColumnsClause ++ setCounterColumnsClause).mkString(", ")
    val whereClause = quotedColumnNames(primaryKey).map(c => s"$c = :$c").mkString(" AND ")

    s"UPDATE ${quote(keyspaceName)}.${quote(tableName)} SET $setClause WHERE $whereClause"
  }

  private val isCounterUpdate =
    tableDef.allColumns.exists(_.isCounterColumn)

  private val queryTemplate: String = {
    if (isCounterUpdate)
      queryTemplateUsingUpdate
    else
      queryTemplateUsingInsert
  }

  private def prepareStatement(session: Session): PreparedStatement = {
    try session.prepare(queryTemplate) catch { case t: Throwable =>
        throw new IOException(s"Failed to prepare statement $queryTemplate: " + t.getMessage, t)
    }
  }

  /** Helper class introduced to avoid passing stmt and queryExecutor everywhere.
    * Wraps the logic of extracting data from rows, converting to proper Java Driver types,
    * grouping them in batches and finally sending into query executor. */
  private class WriteHelper(stmt: PreparedStatement, queryExecutor: QueryExecutor) {

    private val columnNames = rowWriter.columnNames.toIndexedSeq
    private val columnTypes = columnNames.map(stmt.getVariables.getType)
    private val converters = columnTypes.map(ColumnType.converterToCassandra)
    private val buffer = Array.ofDim[Any](columnNames.size)

    private def bind(row: T): BoundStatement = {
      val boundStatement = stmt.bind()
      rowWriter.readColumnValues(row, buffer)
      for (i <- 0 until columnNames.size) {
        val converter = converters(i)
        val columnName = columnNames(i)
        val columnValue = converter.convert(buffer(i))
        val columnType = columnTypes(i)
        val serializedValue =
          if (columnValue != null) columnType.serialize(columnValue, protocolVersion)
          else null
        boundStatement.setBytesUnsafe(columnName, serializedValue)
      }
      boundStatement
    }

    private def createBatch(data: Seq[T]): BatchStatement = {
      val batchStmt =
        if (isCounterUpdate)
          new BatchStatement(BatchStatement.Type.COUNTER)
        else
          new BatchStatement(BatchStatement.Type.UNLOGGED)
      for (row <- data)
        batchStmt.add(bind(row))
      batchStmt
    }

    /** Writes `MeasuredInsertsCount` rows to Cassandra and returns the maximum size of the row */
    private def measureMaxInsertSize(data: Iterator[T]): Int = {
      logDebug(s"Writing $MeasuredInsertsCount rows to $keyspaceName.$tableName and measuring maximum serialized row size...")
      var maxInsertSize = 1
      for (row <- data.take(MeasuredInsertsCount)) {
        val insert = bind(row)
        queryExecutor.executeAsync(insert)
        val size = ObjectSizeEstimator.measureSerializedSize(buffer)
        if (size > maxInsertSize)
          maxInsertSize = size
      }
      logDebug(s"Maximum serialized row size: " + maxInsertSize + " B")
      maxInsertSize
    }

    /** Returns either configured batch size or, if not set, determines the optimal batch size by writing a
      * small number of rows and estimating their size. */
    def optimumBatchSize(data: Iterator[T]): Int = {
      writeConf.batchSize match {
        case RowsInBatch(size) =>
          size
        case BytesInBatch(size) =>
          val maxInsertSize = measureMaxInsertSize(data)
          math.max(1, size / (maxInsertSize * 2))  // additional margin for data larger than usual
      }
    }

    def writeBatched(data: Iterator[T], batchSize: Int) {
      for (batch <- data.grouped(batchSize)) {
        val batchStmt = createBatch(batch)
        batchStmt.setConsistencyLevel(writeConf.consistencyLevel)
        queryExecutor.executeAsync(batchStmt)
      }
    }

    def writeUnbatched(data: Iterator[T]) {
      for (row <- data)
        queryExecutor.executeAsync(bind(row))
    }
  }

  /** Main entry point */
  def write(data: Iterator[T]): Unit = {
    cluster.withSessionDo { session =>
      val rowIterator = new CountingIterator(data)
      val startTime = System.currentTimeMillis()
      val stmt = prepareStatement(session)
      stmt.setConsistencyLevel(writeConf.consistencyLevel)
      val queryExecutor = new QueryExecutor(session, writeConf.parallelismLevel)
      val writeHelper = new WriteHelper(stmt, queryExecutor)
      val batchSize = writeHelper.optimumBatchSize(rowIterator)

      logDebug(s"Writing data partition to $keyspaceName.$tableName in batches of $batchSize rows each.")
      batchSize match {
        case 1 => writeHelper.writeUnbatched(rowIterator)
        case _ => writeHelper.writeBatched(rowIterator, batchSize)
      }

      queryExecutor.waitForCurrentlyExecutingTasks()

      if (queryExecutor.failureCount > 0)
        throw new IOException(s"Failed to write ${queryExecutor.failureCount} batches to $keyspaceName.$tableName.")

      val endTime = System.currentTimeMillis()
      val duration = (endTime - startTime) / 1000.0
      logInfo(f"Wrote ${rowIterator.count} rows in ${queryExecutor.successCount} batches to $keyspaceName.$tableName in $duration%.3f s.")
    }
  }
}

object TableWriter {

  private[io] val MeasuredInsertsCount = 128

  def apply[T : RowWriterFactory](cluster: CassandraCluster, keyspaceName: String, tableName: String,
      columnNames: ColumnSelector, writeConf: WriteConf = WriteConf()): TableWriter[T] = {

    val (tableDef, rowWriter) = unapply(cluster, keyspaceName, tableName, columnNames, writeConf)
    new TableWriter[T](cluster, tableDef, rowWriter, writeConf)
  }

  def unapply[T : RowWriterFactory](
                                   cluster: CassandraCluster,
                                   keyspaceName: String,
                                   tableName: String,
                                   columnNames: ColumnSelector,
                                   writeConf: WriteConf): (TableDef,RowWriter[T]) = {

    val tableDef = TableDef(cluster, keyspaceName, tableName)
    val options = writeConf.optionsAsColumns(tableDef.keyspaceName, tableDef.tableName)
    val selectedColumns = tableDef.selectedColumns(columnNames)
    val updatedDef = tableDef.appendRegularColumns(options)
    val rowWriter = implicitly[RowWriterFactory[T]]
      .rowWriter(updatedDef, selectedColumns ++ writeConf.optionPlaceholders)
    (tableDef, rowWriter)
  }

}
