package com.datastax.spark.connector.cql

import java.net.InetAddress

import org.apache.spark.SparkConf
import com.datastax.driver.scala.core.CassandraCluster
import com.datastax.driver.scala.core.conf._

/**
 * A [[com.datastax.driver.scala.core.CassandraCluster]] that adds an optional Thrift usage layer.
 * Thrift usage is going away.
 */
class CassandraConnector(config: ClusterConfig) extends CassandraCluster(config) {

  def createThriftClient(): CassandraClientProxy =
    createThriftClient(closestLiveHost.getAddress)

  /** Opens a Thrift client to the given host. Don't use it unless you really know what you are doing. */
  def createThriftClient(host: InetAddress): CassandraClientProxy = {
    logDebug(s"Attempting to open thrift connection to Cassandra at ${host.getHostAddress}:${config.rpcPort}")
    CassandraClientProxy.wrap(config, host)
  }

  def withCassandraClientDo[T](host: InetAddress)(code: CassandraClientProxy => T): T =
    closeResourceAfterUse(createThriftClient(host))(code)

  def withCassandraClientDo[T](code: CassandraClientProxy => T): T =
    closeResourceAfterUse(createThriftClient())(code)

}

object CassandraConnector {
  import com.datastax.spark.connector._
  import Cluster._

  def apply(config: ClusterConfig) = {
    new CassandraConnector(config)
  }

  def apply(conf: SparkConf): CassandraConnector =
    apply(conf.clusterConfig)

  /** Returns a CassandraCluster created from defaults with the `host` and `authConf`. */
  def apply(host: InetAddress, auth: AuthConf = NoAuthConf, nativePort: Int = DefaultNativePort, rpcPort: Int = DefaultRpcPort): CassandraConnector = {
    apply(ClusterConfig(hosts = Set(host), nativePort = nativePort, rpcPort = rpcPort, authConf = auth))
  }
}
