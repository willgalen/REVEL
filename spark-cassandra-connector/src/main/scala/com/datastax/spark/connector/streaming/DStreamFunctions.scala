package com.datastax.spark.connector.streaming

import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import com.datastax.driver.scala.core.conf.WriteConf
import com.datastax.driver.scala.core.io.RowWriterFactory
import com.datastax.driver.scala.core.{AllColumns, ColumnSelector}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.{CassandraTableWriter, WritableToCassandra}

class DStreamFunctions[T](dstream: DStream[T]) extends WritableToCassandra[T] with Serializable {

  override def sparkContext: SparkContext = dstream.context.sparkContext

  def conf = sparkContext.getConf

  /**
   * Performs [[com.datastax.spark.connector.writer.WritableToCassandra]] for each produced RDD.
   * Uses specific column names with an additional batch size.
   */
  def saveToCassandra(keyspaceName: String,
                      tableName: String,
                      columnNames: ColumnSelector = AllColumns,
                      writeConf: WriteConf = conf.writeConf)
                     (implicit connector: CassandraConnector = CassandraConnector(conf),
                      rwf: RowWriterFactory[T]): Unit = {
    val writer = CassandraTableWriter(connector, keyspaceName, tableName, columnNames, writeConf)
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write _))
  }
}
