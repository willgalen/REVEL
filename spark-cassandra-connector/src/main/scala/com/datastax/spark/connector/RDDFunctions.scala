package com.datastax.spark.connector

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.driver.scala.core.conf.WriteConf
import com.datastax.driver.scala.core.io.RowWriterFactory
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.scala.core.{AllColumns, ColumnSelector}
import com.datastax.spark.connector.writer._

/** Provides Cassandra-specific methods on `RDD` */
class RDDFunctions[T](rdd: RDD[T]) extends WritableToCassandra[T] with Serializable {

  override val sparkContext: SparkContext = rdd.sparkContext

  /**
   * Saves the data from `RDD` to a Cassandra table. Uses the specified column names.
   * @see [[com.datastax.spark.connector.writer.WritableToCassandra]]
   */
  def saveToCassandra(keyspaceName: String,
                      tableName: String,
                      columns: ColumnSelector = AllColumns,
                      writeConf: WriteConf = sparkContext.writeConf)
                     (implicit connector: CassandraConnector = CassandraConnector(sparkContext.getConf),
                      rwf: RowWriterFactory[T]): Unit = {
    val writer = CassandraTableWriter(connector, keyspaceName, tableName, columns, writeConf)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }
}
