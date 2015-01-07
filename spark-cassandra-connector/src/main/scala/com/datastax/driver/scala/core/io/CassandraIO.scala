package com.datastax.driver.scala.core.io

import java.io.{Serializable => JSerializable}

import com.datastax.driver.scala.core.conf.{CassandraSettings, CassandraConnectorConf, ReadConf, WriteConf}
import com.datastax.driver.scala.core.{AllColumns, Connector, ColumnSelector}
import com.datastax.driver.scala.types.ValidSourceType

import scala.reflect.ClassTag

/**
 * {{{
 *   case class SearchStatistics(keyword: String, hits: Int)
 *   val keyspace = "searches"
 *   val table = "animal_searches"
 *
 *   import com.datastax.driver.scala.core._
 *   implicit val settings = CassandraSettings() // or CassandraSettings(myvalues: Map[String,String])
 *   val cassandra = CassandraContext(settings)
 *
 *   // Write one row:
 *   Set(SearchStatistics("dolphin", 5000)).write(keyspace, table)
 *
 *   // Write three rows to the search.searches table:
 *   val searches = Seq(("cat", 1000), ("dog", 600), ("penguin", 2000))
 *   searches.write(keyspace, table)
 *
 *   // read specific table columns
 *   cassandra.table[SearchStatistics](keyspace, table)
 *     .select("keyword", "count")
 *     .stream foreach println
 *
 *   // read all table columns
 *   val results = cassandra.table("search", "searches_table")
 *                   .where("keyword = ?", "cat")
 *                   .stream.map(Statistics)
 * }}}
 * java.io.Serializable is needed if the Source is going to have any
 * methods attached that compute remotely.
 */
abstract class CassandraIO(settings: CassandraSettings) extends JSerializable {

  implicit val conf = CassandraConnectorConf(settings)

  final val DefaultReadConf = ReadConf(settings)

  final val DefaultWriteConf = WriteConf(settings)

}

class CassandraContext(settings: CassandraSettings) extends CassandraIO(settings) {

  def table[T](keyspace: String, table: String, readConf: ReadConf = DefaultReadConf)
                (implicit ct: ClassTag[T], rrf: RowReaderFactory[T], ev: ValidSourceType[T]): TableReader[T] = {
    TableReader[T](conf, keyspace, table, readConf)
  }

  def write[T](data: Iterable[T], keyspace: String, table: String,
               columns: ColumnSelector = AllColumns,
               writeConf: WriteConf = DefaultWriteConf) (implicit rwf: RowWriterFactory[T]) = {
    val writer = TableWriter[T](Connector(conf), keyspace, table, columns, writeConf)
    writer.write(data.iterator)
  }
}
