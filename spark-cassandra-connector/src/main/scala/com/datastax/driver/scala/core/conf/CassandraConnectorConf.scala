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
                                  clusterConf: CassandraClusterConf)

/** Companion object for `CassandraConnectorConf` instances. Allows for manually setting
  * connection values or reading them from system properties, which are needed for
  * establishing connections to a Cassandra cluster. */
object CassandraConnectorConf extends Logging {

  def apply(settings: CassandraSettings): CassandraConnectorConf =
    CassandraConnectorConf(
      hosts = settings.CassandraHosts,
      nativePort = settings.NativePort,
      rpcPort = settings.RpcPort,
      authConf = AuthConf(settings),
      connectionFactory = CassandraConnectionFactory(settings),
      clusterConf = CassandraClusterConf(settings))

  /** Returns an instance of CassandraConnectorConf with all default settings and local host. */
  def apply(host: InetAddress = InetAddress.getLocalHost,
            auth: AuthConf = NoAuthConf): CassandraConnectorConf = {
    import Connection._
    CassandraConnectorConf(Set(host), DefaultNativePort, DefaultRpcPort, auth,
      SimpleConnectionFactory, CassandraClusterConf())
  }
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
case class CassandraClusterConf(minReconnectDelay: Int,
                                maxReconnectDelay: Int,
                                localDc: Option[String],
                                queryRetries: Int,
                                timeout: Int,
                                readTimeout: Int)

object CassandraClusterConf {
  def apply(settings: CassandraSettings): CassandraClusterConf =
    CassandraClusterConf(
      minReconnectDelay = settings.ClusterReconnectDelayMin,
      maxReconnectDelay = settings.ClusterReconnectDelayMax,
      localDc = settings.ClusterLocalDc,
      queryRetries = settings.ClusterQueryRetries,
      timeout = settings.ClusterTimeout,
      readTimeout = settings.ClusterReadTimeout)

  /** Returns a `CassandraClusterConf` instance with only default settings applied. */
 def apply(): CassandraClusterConf =
    CassandraClusterConf(
      minReconnectDelay = Cluster.DefaultReconnectDelayMin,
      maxReconnectDelay = Cluster.DefaultReconnectDelayMax,
      localDc = None,
      queryRetries = Cluster.DefaultQueryRetryCountMillis,
      timeout = Cluster.DefaultTimeoutMillis,
      readTimeout = Cluster.DefaultReadTimeoutMillis)
}