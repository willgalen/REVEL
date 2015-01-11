package com.datastax.spark.connector.streaming

import scala.reflect.ClassTag
import org.apache.spark.streaming.StreamingContext
import com.datastax.driver.scala.core.io.{RowReaderFactory, CqlWhereClause}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.scala.core.conf.ReadConf
import com.datastax.driver.scala.core.{AllColumns, ColumnSelector}
import com.datastax.spark.connector.rdd.CassandraRDD

/** RDD representing a Cassandra table for Spark Streaming.
  * @see [[com.datastax.spark.connector.rdd.CassandraRDD]] */
class CassandraStreamingRDD[R] private[connector] (
    sctx: StreamingContext,
    connector: CassandraConnector,
    keyspace: String,
    table: String,
    columns: ColumnSelector = AllColumns,
    where: CqlWhereClause = CqlWhereClause.empty,
    readConf: ReadConf = ReadConf())(
  implicit
    ct : ClassTag[R],
    @transient rrf: RowReaderFactory[R])
  extends CassandraRDD[R](sctx.sparkContext, connector, keyspace, table, columns, where, readConf)
