package com.datastax.spark.connector.writer

import scala.collection.Iterator
import org.apache.spark.TaskContext
import com.datastax.driver.scala.core.io.{TableWriter, RowWriter, RowWriterFactory}
import com.datastax.driver.scala.core.conf.WriteConf
import com.datastax.driver.scala.core.{CassandraCluster, ColumnSelector, TableDef}

class CassandraTableWriter[T] private (cluster: CassandraCluster,
                                       tableDef: TableDef,
                                       rowWriter: RowWriter[T],
                                       writeConf: WriteConf)
  extends TableWriter[T](cluster, tableDef, rowWriter, writeConf) {

  /** Main entry point */
  def write(taskContext: TaskContext, data: Iterator[T]): Unit = write(data)
}

object CassandraTableWriter {

  def apply[T : RowWriterFactory](cluster: CassandraCluster,
                                  keyspaceName: String,
                                  tableName: String,
                                  columnNames: ColumnSelector,
                                  writeConf: WriteConf = WriteConf()): CassandraTableWriter[T] = {

    val (tableDef, rowWriter) = TableWriter.unapply(cluster, keyspaceName, tableName, columnNames, writeConf)
    new CassandraTableWriter[T](cluster, tableDef, rowWriter, writeConf)
  }
}