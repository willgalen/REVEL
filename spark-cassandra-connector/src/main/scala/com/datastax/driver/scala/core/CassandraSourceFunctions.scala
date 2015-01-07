package com.datastax.driver.scala.core

import com.datastax.driver.scala.core.conf.{WriteConf, CassandraSettings}
import com.datastax.driver.scala.core.io.{RowWriterFactory, CassandraContext}

class CassandraSourceFunctions[T](@transient data: Iterable[T])(implicit settings: CassandraSettings) extends Serializable {

  private val context = new CassandraContext(settings)
  import context._

  def write(keyspace: String,
            table: String,
            columns: ColumnSelector = AllColumns,
            writeConf: WriteConf = DefaultWriteConf)
           (implicit connector: Connector = Connector(settings), rwf: RowWriterFactory[T]): Unit = {
    context.write(data, keyspace, table, columns, writeConf)
  }
}
