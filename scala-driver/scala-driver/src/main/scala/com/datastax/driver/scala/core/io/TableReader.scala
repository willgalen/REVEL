package com.datastax.driver.scala.core.io

import java.io.IOException

import scala.language.existentials

import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import com.datastax.driver.scala.core.conf.{ClusterConfig, ReadConf}
import com.datastax.driver.scala.core.utils.{Logging, CountingIterator}
import com.datastax.driver.scala.core.Schema
import com.datastax.driver.scala.types.{ColumnType, TypeConverter}
import com.datastax.driver.core.{ProtocolVersion, Session, Statement}
import com.datastax.driver.scala.core._

import scala.util.control.NonFatal

class TableReader[R] private[datastax] (val connector: CassandraCluster,
                                        val keyspaceName: String,
                                        val tableName: String,
                                        val columnNames: ColumnSelector = AllColumns,
                                        val where: CqlWhereClause = CqlWhereClause.empty,
                                        val readConf: ReadConf)(
  implicit ct : ClassTag[R], @transient rtf: RowReaderFactory[R]) extends Serializable with Logging {

  private def fetchSize = readConf.fetchSize
  private def splitSize = readConf.splitSize
  private def consistencyLevel = readConf.consistencyLevel

  private def copy(columnNames: ColumnSelector = columnNames,
                   where: CqlWhereClause = where,
                   readConf: ReadConf = readConf, connector: CassandraCluster = connector): TableReader[R] =
    new TableReader(connector, keyspaceName, tableName, columnNames, where, readConf)

  /** Returns a copy of this Cassandra RDD with specified connector */
  def withCluster(other: CassandraCluster): TableReader[R] =
    copy(connector = other)

  /** Adds a CQL `WHERE` predicate(s) to the query.
    * Useful for leveraging secondary indexes in Cassandra.
    * Implicitly adds an `ALLOW FILTERING` clause to the WHERE clause, however beware that some predicates
    * might be rejected by Cassandra, particularly in cases when they filter on an unindexed, non-clustering column.*/
  def where(cql: String, values: Any*): TableReader[R] = {
    copy(where = where and CqlWhereClause(Seq(cql), values))
  }

  /** Allows to set custom read configuration, e.g. consistency level or fetch size. */
  def withReadConf(readConf: ReadConf) = {
    copy(readConf = readConf)
  }

  /** Throws IllegalArgumentException if columns sequence contains unavailable columns */
  private def checkColumnsAvailable(columns: Seq[NamedColumnRef], availableColumns: Seq[NamedColumnRef]) {
    val availableColumnsSet = availableColumns.collect {
      case ColumnName(columnName) => columnName
    }.toSet

    val notFound = columns.collectFirst {
      case ColumnName(columnName) if !availableColumnsSet.contains(columnName) => columnName
    }

    if (notFound.isDefined)
      throw new IllegalArgumentException(
        s"Column not found in selection: ${notFound.get}. " +
          s"Available columns: [${availableColumns.mkString(",")}].")
  }

  /** Filters currently selected set of columns with a new set of columns */
  private def narrowColumnSelection(columns: Seq[NamedColumnRef]): Seq[NamedColumnRef] = {
    columnNames match {
      case SomeColumns(cs @ _*) =>
        checkColumnsAvailable(columns, cs)
      case AllColumns =>
      // we do not check for column existence yet as it would require fetching schema and a call to C*
      // columns existence will be checked by C* once the RDD gets computed.
    }
    columns
  }

  /** Narrows down the selected set of columns.
    * Use this for better performance, when you don't need all the columns in the result RDD.
    * When called multiple times, it selects the subset of the already selected columns, so
    * after a column was removed by the previous `select` call, it is not possible to
    * add it back.
    *
    * The selected columns are [[NamedColumnRef]] instances. This type allows to specify columns for
    * straightforward retrieval and to read TTL or write time of regular columns as well. Implicit
    * conversions included in [[com.datastax.spark.connector]] package make it possible to provide
    * just column names (which is also backward compatible) and optional add `.ttl` or `.writeTime`
    * suffix in order to create an appropriate [[NamedColumnRef]] instance.
    */
  def select(columns: NamedColumnRef*): TableReader[R] = {
    copy(columnNames = SomeColumns(narrowColumnSelection(columns): _*))
  }

  /** Maps each row into object of a different type using provided function taking column value(s) as argument(s).
    * Can be used to convert each row to a tuple or a case class object:
    * {{{
    * cassandra.table("ks", "table").select("column1").as((s: String) => s)                 // yields CassandraSource[String]
    * cassandra.table("ks", "table").select("column1", "column2").as((_: String, _: Long))  // yields CassandraSource[(String, Long)]
    *
    * case class MyRow(key: String, value: Long)
    * cassandra.table("ks", "table").select("column1", "column2").as(MyRow)                 // yields CassandraSource[MyRow]
    * }}}*/
  def as[B : ClassTag, A0 : TypeConverter](f: A0 => B): TableReader[B] = {
    implicit val ft = new FunctionBasedRowReader1(f)
    new TableReader[B](connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter](f: (A0, A1) => B) = {
    implicit val ft = new FunctionBasedRowReader2(f)
    new TableReader[B](connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter](f: (A0, A1, A2) => B) = {
    implicit val ft = new FunctionBasedRowReader3(f)
    new TableReader[B](connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter,
  A3 : TypeConverter](f: (A0, A1, A2, A3) => B) = {
    implicit val ft = new FunctionBasedRowReader4(f)
    new TableReader[B](connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter](f: (A0, A1, A2, A3, A4) => B) = {
    implicit val ft = new FunctionBasedRowReader5(f)
    new TableReader[B](connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter](f: (A0, A1, A2, A3, A4, A5) => B) = {
    implicit val ft = new FunctionBasedRowReader6(f)
    new TableReader[B](connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6) => B) = {
    implicit val ft = new FunctionBasedRowReader7(f)
    new TableReader[B](connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter,
  A7 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7) => B) = {
    implicit val ft = new FunctionBasedRowReader8(f)
    new TableReader[B](connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter, A7: TypeConverter,
  A8 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8) => B) = {
    implicit val ft = new FunctionBasedRowReader9(f)
    new TableReader[B](connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter, A7: TypeConverter,
  A8 : TypeConverter, A9 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => B) = {
    implicit val ft = new FunctionBasedRowReader10(f)
    new TableReader[B](connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter, A7: TypeConverter, A8: TypeConverter,
  A9 : TypeConverter, A10 : TypeConverter](f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => B) = {
    implicit val ft = new FunctionBasedRowReader11(f)
    new TableReader[B](connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  def as[B : ClassTag, A0 : TypeConverter, A1 : TypeConverter, A2 : TypeConverter, A3 : TypeConverter,
  A4 : TypeConverter, A5 : TypeConverter, A6 : TypeConverter, A7: TypeConverter, A8: TypeConverter,
  A9 : TypeConverter, A10: TypeConverter, A11 : TypeConverter](
                                                                f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => B) = {
    implicit val ft = new FunctionBasedRowReader12(f)
    new TableReader[B](connector, keyspaceName, tableName, columnNames, where, readConf)
  }

  // ===================================================================
  // no references to sc allowed below this line
  // ===================================================================

  // Lazy fields allow to recover non-serializable objects after deserialization of RDD
  // Cassandra connection is not serializable, but will be recreated on-demand basing on serialized host and port.

  // Table metadata should be fetched only once to guarantee all tasks use the same metadata.
  // TableDef is serializable, so there is no problem:
  lazy val tableDef = {
    Schema.fromCassandra(connector, Some(keyspaceName), Some(tableName)).tables.headOption match {
      case Some(t) => t
      case None => throw new IOException(s"Table not found: $keyspaceName.$tableName")
    }
  }

  private lazy val rowTransformer = implicitly[RowReaderFactory[R]].rowReader(tableDef)

  private def checkColumnsExistence(columns: Seq[NamedColumnRef]): Seq[NamedColumnRef] = {
    val allColumnNames = tableDef.allColumns.map(_.columnName).toSet
    val regularColumnNames = tableDef.regularColumns.map(_.columnName).toSet

    def checkSingleColumn(column: NamedColumnRef) = {
      if (!allColumnNames.contains(column.columnName))
        throw new IOException(s"Column $column not found in table $keyspaceName.$tableName")

      column match {
        case ColumnName(_) =>

        case TTL(columnName) =>
          if (!regularColumnNames.contains(columnName))
            throw new IOException(s"TTL can be obtained only for regular columns, " +
              s"but column $columnName is not a regular column in table $keyspaceName.$tableName.")

        case WriteTime(columnName) =>
          if (!regularColumnNames.contains(columnName))
            throw new IOException(s"TTL can be obtained only for regular columns, " +
              s"but column $columnName is not a regular column in table $keyspaceName.$tableName.")
      }

      column
    }

    columns.map(checkSingleColumn)
  }


  /** Returns the names of columns to be selected from the table.*/
  lazy val selectedColumnNames: Seq[NamedColumnRef] = {
    val providedColumnNames =
      columnNames match {
        case AllColumns => tableDef.allColumns.map(col => col.columnName: NamedColumnRef).toSeq
        case SomeColumns(cs @ _*) => checkColumnsExistence(cs)
      }

    (rowTransformer.columnNames, rowTransformer.requiredColumns) match {
      case (Some(cs), None) => providedColumnNames.filter(columnName => cs.toSet(columnName.selectedAs))
      case (_, _) => providedColumnNames
    }
  }


  /** Checks for existence of keyspace, table, columns and whether the number of selected columns corresponds to
    * the number of the columns expected by the target type constructor.
    * If successful, does nothing, otherwise throws appropriate `IOException` or `AssertionError`.*/
  lazy val verify = {
    val targetType = implicitly[ClassTag[R]]

    tableDef.allColumns  // will throw IOException if table does not exist

    rowTransformer.columnNames match {
      case Some(names) =>
        val missingColumns = names.toSet -- selectedColumnNames.map(_.selectedAs).toSet
        assert(missingColumns.isEmpty, s"Missing columns needed by $targetType: ${missingColumns.mkString(", ")}")
      case None =>
    }

    rowTransformer.requiredColumns match {
      case Some(count) =>
        assert(selectedColumnNames.size >= count,
          s"Not enough columns selected for the target row type $targetType: ${selectedColumnNames.size} < $count")
      case None =>
    }
  }

  private lazy val cassandraPartitionerClassName =
    connector.withSessionDo {
      session =>
        session.execute("SELECT partitioner FROM system.local").one().getString(0)
    }

  private def quote(name: String) = "\"" + name + "\""

  private def tokenRangeToCqlQuery(range: CqlTokenRange): (String, Seq[Any]) = {
    val columns = selectedColumnNames.map(_.cql).mkString(", ")
    val filter = (range.cql +: where.predicates ).filter(_.nonEmpty).mkString(" AND ") + " ALLOW FILTERING"
    val quotedKeyspaceName = quote(keyspaceName)
    val quotedTableName = quote(tableName)
    (s"SELECT $columns FROM $quotedKeyspaceName.$quotedTableName WHERE $filter", range.values ++ where.values)
  }

  def protocolVersion(session: Session): ProtocolVersion = {
    session.getCluster.getConfiguration.getProtocolOptions.getProtocolVersionEnum
  }

  private def createStatement(session: Session, cql: String, values: Any*): Statement = {
    try {
      implicit val pv = protocolVersion(session)
      val stmt = session.prepare(cql)
      stmt.setConsistencyLevel(consistencyLevel)
      val converters = stmt.getVariables
        .map(v => ColumnType.converterToCassandra(v.getType))
        .toArray
      val convertedValues =
        for ((value, converter) <- values zip converters)
        yield converter.convert(value)
      val bstm = stmt.bind(convertedValues: _*)
      bstm.setFetchSize(fetchSize)
      bstm
    }
    catch {
      case t: Throwable =>
        throw new IOException(s"Exception during preparation of $cql: ${t.getMessage}", t)
    }
  }

  private def fetchTokenRange(session: Session, range: CqlTokenRange): Iterator[R] = {
    val (cql, values) = tokenRangeToCqlQuery(range)
    logDebug(s"Fetching data for range ${range.cql} with $cql with params ${values.mkString("[", ",", "]")}")
    val stmt = createStatement(session, cql, values: _*)
    val columnNamesArray = selectedColumnNames.map(_.selectedAs).toArray
    try {
      implicit val pv = protocolVersion(session)
      val rs = session.execute(stmt)
      val iterator = new PrefetchingResultSetIterator(rs, fetchSize)
      val result = iterator.map(rowTransformer.read(_, columnNamesArray))
      logDebug(s"Row iterator for range ${range.cql} obtained successfully.")
      result
    } catch {
      case t: Throwable =>
        throw new IOException(s"Exception during execution of $cql: ${t.getMessage}", t)
    }
  }

  // TODO general finishing of function, no partitioning here yet
  // TODO return akka stream Source(countingIterator) on spark (akka) upgrade in connector
  def stream: Stream[R] = {
    val session = connector.openSession()
    val startTime = System.currentTimeMillis
    try {
      // flatMap on iterator is lazy, therefore a query for the next token range is executed not earlier
      // than all of the rows returned by the previous query have been consumed
      val rowIterator = fetchTokenRange(session, CqlTokenRange(""))
      val countingIterator = new CountingIterator(rowIterator)

      val duration = (System.currentTimeMillis - startTime) / 1000.0
      logDebug(f"Fetched ${countingIterator.count} rows from $keyspaceName.$tableName in $duration%.3f s.")
      countingIterator.toStream
    } catch {
      case NonFatal(e) =>
        logError(s"Error reading rows from $keyspaceName.$tableName", e)
        throw e
    }
    finally session.close()
  }
}

object TableReader {

  def apply[T](conf: ClusterConfig, keyspaceName: String, tableName: String, readConf: ReadConf)
              (implicit ct: ClassTag[T], rrf: RowReaderFactory[T]): TableReader[T] =
    new TableReader[T](CassandraCluster(conf), keyspaceName, tableName, AllColumns, CqlWhereClause.empty, readConf)

  def apply[K, V](conf: ClusterConfig, keyspaceName: String, tableName: String, readConf: ReadConf)
                 (implicit keyCT: ClassTag[K], valueCT: ClassTag[V], rrf: RowReaderFactory[(K, V)]): TableReader[(K, V)] =
    new TableReader[(K, V)](CassandraCluster(conf), keyspaceName, tableName, AllColumns, CqlWhereClause.empty, readConf)
}
