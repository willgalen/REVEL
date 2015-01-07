package com.datastax.driver.scala.core.conf

import java.net.InetAddress

import com.datastax.driver.scala.core.utils.Logging
import com.datastax.driver.scala.core.{SimpleConnectionFactory, CassandraConnectionFactory}

/** Stores configuration of a connection to Cassandra.
 * Provides information about cluster nodes, ports and optional credentials for authentication.
 *
 * @param hosts the contact point(s) to connect to the Cassandra cluster
 *
 * @param nativePort the cassandra native port, defaults to 9042
 *
 * @param rpcPort the cassandra thrift port, defaults to 9160
 *
 * @param authConf the [[AuthConf]] implementation authentication configuration instance
 *
 * @param connectionFactory the name of a Scala module or class implementing [[CassandraConnectionFactory]]
 *                          that allows to plugin custom code for connecting to Cassandra
 */
case class CassandraConnectorConf(hosts: Set[InetAddress],
                                  nativePort: Int,
                                  rpcPort: Int,
                                  authConf: AuthConf,
                                  connectionFactory: CassandraConnectionFactory,
                                  clusterConf: ClusterConf = ClusterConf.Default)

/** Companion object for `CassandraConnectorConf` instances. Allows for manually setting
  * connection values or reading them from system properties, which are needed for
  * establishing connections to a Cassandra cluster. */
object CassandraConnectorConf extends Logging {
  import Connection._

  final val Default = CassandraConnectorConf(
    hosts = Set(InetAddress.getLocalHost),
    nativePort = DefaultNativePort,
    rpcPort = DefaultRpcPort,
    authConf = NoAuthConf,
    connectionFactory = SimpleConnectionFactory, ClusterConf.Default)

  def apply(settings: CassandraSettings): CassandraConnectorConf =
    CassandraConnectorConf(
      settings.CassandraHosts,
      settings.NativePort,
      settings.RpcPort,
      AuthConf(settings),
      CassandraConnectionFactory(settings),
      ClusterConf(settings))

}

/** Used by the [[SimpleConnectionFactory]] to configure the cluster.
  * Attempts to acquire optional connection tuning settings from java system
  * properties. If unavailable, falls back to default settings.
  *
  * @param minReconnectDelay used in the reconnection policy for the initial delay determining how often to
  *                          try to reconnect to a dead node (default 1 s)
  *
  * @param maxReconnectDelay used in the reconnection policy as the final delay determining how often to
  *                          try to reconnect to a dead node (default 60 s)
  *
  * @param localDc used by `LocalNodeFirstLoadBalancingPolicy`: if set, attempts to use that first
  *
  * @param queryRetries the number of times to reattempt a failed query
  *
  * @param timeout the connection timeout in millis
  *
  * @param readTimeout the read timeout in millis
  */
case class ClusterConf(minReconnectDelay: Int,
                       maxReconnectDelay: Int,
                       localDc: Option[String],
                       queryRetries: Int,
                       timeout: Int,
                       readTimeout: Int)

object ClusterConf {

  final val Default = ClusterConf(
    minReconnectDelay = Cluster.DefaultReconnectDelayMin,
    maxReconnectDelay = Cluster.DefaultReconnectDelayMax,
    localDc = None,
    queryRetries = Cluster.DefaultQueryRetryCountMillis,
    timeout = Cluster.DefaultTimeoutMillis,
    readTimeout = Cluster.DefaultReadTimeoutMillis)
  
  def apply(settings: CassandraSettings): ClusterConf =
    ClusterConf(settings.ClusterReconnectDelayMin,
      settings.ClusterReconnectDelayMax,
      settings.ClusterLocalDc,
      settings.ClusterQueryRetries,
      settings.ClusterTimeout,
      settings.ClusterReadTimeout)
 
}