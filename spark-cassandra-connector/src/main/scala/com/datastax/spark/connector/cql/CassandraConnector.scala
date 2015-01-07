package com.datastax.spark.connector.cql

import java.net.InetAddress

import org.apache.spark.SparkConf
import com.datastax.driver.scala.core.Connector 
import com.datastax.driver.scala.core.conf.CassandraConnectorConf

/**
 * A [[com.datastax.driver.scala.core.Connector]] that adds an optional Thrift usage layer.
 * Thrift usage is going away.
 */
class CassandraConnector(conf: CassandraConnectorConf) extends Connector(conf) {

  def createThriftClient(): CassandraClientProxy =
    createThriftClient(closestLiveHost.getAddress)

  /** Opens a Thrift client to the given host. Don't use it unless you really know what you are doing. */
  def createThriftClient(host: InetAddress): CassandraClientProxy = {
    logDebug(s"Attempting to open thrift connection to Cassandra at ${host.getHostAddress}:${conf.rpcPort}")
    CassandraClientProxy.wrap(conf, host)
  }

  def withCassandraClientDo[T](host: InetAddress)(code: CassandraClientProxy => T): T =
    closeResourceAfterUse(createThriftClient(host))(code)

  def withCassandraClientDo[T](code: CassandraClientProxy => T): T =
    closeResourceAfterUse(createThriftClient())(code)

}

object CassandraConnector {
  import com.datastax.spark.connector._

  def apply(conf: CassandraConnectorConf) =
    new CassandraConnector(conf)

  def apply(conf: SparkConf): CassandraConnector =
    apply(conf.connectorConf)

}
