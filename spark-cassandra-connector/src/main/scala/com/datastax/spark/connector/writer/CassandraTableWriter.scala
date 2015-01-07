package com.datastax.spark.connector.writer

import scala.collection.Iterator
import org.apache.spark.TaskContext
import com.datastax.driver.scala.core.io.{TableWriter, RowWriter, RowWriterFactory}
import com.datastax.driver.scala.core.conf.WriteConf
import com.datastax.driver.scala.core.{ColumnSelector, Connector, TableDef}

class CassandraTableWriter[T] private (connector: Connector,
                                       tableDef: TableDef,
                                       rowWriter: RowWriter[T],
                                       writeConf: WriteConf)
  extends TableWriter[T](connector, tableDef, rowWriter, writeConf) {

  /** Main entry point */
  def write(taskContext: TaskContext, data: Iterator[T]): Unit = write(data)
}

object CassandraTableWriter {

  def apply[T : RowWriterFactory](connector: Connector,
                                  keyspaceName: String,
                                  tableName: String,
                                  columnNames: ColumnSelector,
                                  writeConf: WriteConf = WriteConf.Default): CassandraTableWriter[T] = {

    val (tableDef, rowWriter) = TableWriter.unapply(connector, keyspaceName, tableName, columnNames, writeConf)
    new CassandraTableWriter[T](connector, tableDef, rowWriter, writeConf)
  }
}