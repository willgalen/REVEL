package com.datastax.spark.connector.rdd.partitioner

import java.io.IOException
import java.net.InetAddress

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}
import com.datastax.driver.scala.core.partition.{CassandraNode, TokenRange, TokenFactory, Token}
import com.datastax.driver.scala.core.utils.Logging
import com.datastax.spark.connector.cql.SparkCassandraConnector
import org.apache.cassandra.thrift.CfSplit

/** Delegates token range splitting to Cassandra server. */
class ServerSideTokenRangeSplitter[V, T <: Token[V]](
  connector: SparkCassandraConnector,
  keyspaceName: String,
  tableName: String,
  tokenFactory: TokenFactory[V, T])
  extends TokenRangeSplitter[V, T] with Logging {

  private def unthriftify(cfSplit: CfSplit, endpoints: Set[CassandraNode]): TokenRange[V, T] = {
    val left = tokenFactory.fromString(cfSplit.start_token)
    val right = tokenFactory.fromString(cfSplit.end_token)
    TokenRange(left, right, endpoints, Some(cfSplit.row_count))
  }

  private def fetchSplits(range: TokenRange[V, T], endpoint: InetAddress, splitSize: Long): Seq[TokenRange[V, T]] = {
    val startToken = tokenFactory.toString(range.start)
    val endToken = tokenFactory.toString(range.end)

    connector.withCassandraClientDo(endpoint) {
      client =>
        client.set_keyspace(keyspaceName)
        client
          .describe_splits_ex(tableName, startToken, endToken, splitSize.toInt)
          .map(unthriftify(_, range.endpoints))
    }
  }

  def split(range: TokenRange[V, T], splitSize: Long) = {
    val fetchResults =
      for (endpoint <- range.endpoints.toStream)
      yield Try(fetchSplits(range, endpoint.rpcAddress, splitSize))

    fetchResults
      .collectFirst { case Success(splits) => splits }
      .getOrElse {
        for (Failure(e) <- fetchResults)
          logError("Failure while fetching splits from Cassandra", e)
        if (range.endpoints.isEmpty)
          throw new IOException(s"Failed to fetch splits of $range because there are no replicas for the keyspace in the current datacenter.")
        else
          throw new IOException(s"Failed to fetch splits of $range from all endpoints: ${range.endpoints.mkString(", ")}")
      }
  }
}