package com.datastax.driver.scala.core

import com.datastax.driver.scala.core.conf.{WriteConf, CassandraSettings}
import com.datastax.driver.scala.core.io.{RowWriterFactory, CassandraContext}

class CassandraContextFunctions[T](@transient data: Iterable[T])(implicit settings: CassandraSettings) extends Serializable {

  private val context = new CassandraContext(settings)

  def write(keyspace: String,
            table: String,
            columns: ColumnSelector = AllColumns,
            writeConf: WriteConf = WriteConf(settings))
           (implicit connector: CassandraCluster = CassandraCluster(context.config), rwf: RowWriterFactory[T]): Unit = {
    context.write(data, keyspace, table, columns, writeConf)
  }
}
