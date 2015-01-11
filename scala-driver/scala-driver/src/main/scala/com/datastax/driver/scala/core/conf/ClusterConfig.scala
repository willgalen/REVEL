package com.datastax.driver.scala.core.conf

import java.net.InetAddress

import com.datastax.driver.scala.core.{SimpleConnectionFactory, CassandraConnectionFactory}

case class ClusterConfig(hosts: Set[InetAddress],
                           nativePort: Int = Cluster.DefaultNativePort,
                           rpcPort: Int = Cluster.DefaultRpcPort,
                           authConf: AuthConf = NoAuthConf,
                           connectionFactory: CassandraConnectionFactory = SimpleConnectionFactory,
                           minReconnectDelay: Int = Cluster.DefaultReconnectDelayMin,
                           maxReconnectDelay: Int = Cluster.DefaultReconnectDelayMax,
                           localDc: Option[String] = None,
                           queryRetries: Int = Cluster.DefaultQueryRetryCountMillis,
                           timeout: Int = Cluster.DefaultTimeoutMillis,
                           readTimeout: Int = Cluster.DefaultReadTimeoutMillis) {


  def update(nodes: Set[InetAddress]): ClusterConfig = copy(hosts = nodes)
}

object ClusterConfig {

  def apply(settings: CassandraSettings): ClusterConfig = ClusterConfig(
    hosts = settings.CassandraHosts,
    nativePort = settings.NativePort,
    rpcPort = settings.RpcPort,
    authConf = AuthConf(settings),
    connectionFactory = CassandraConnectionFactory(settings),
    minReconnectDelay = settings.ClusterReconnectDelayMin,
    maxReconnectDelay = settings.ClusterReconnectDelayMax,
    localDc = settings.ClusterLocalDc,
    queryRetries = settings.ClusterQueryRetries,
    timeout = settings.ClusterTimeout,
    readTimeout = settings.ClusterReadTimeout
  )

  def apply(host: InetAddress): ClusterConfig =
    ClusterConfig(hosts = Set(host))

}