package com.datastax.driver.scala.core.conf

import java.net.InetAddress

import com.datastax.driver.scala.core.CassandraConnectionFactory

case class ClusterConfig(hosts: Set[InetAddress],
                           nativePort: Int,
                           rpcPort: Int,
                           authConf: AuthConf,
                           connectionFactory: CassandraConnectionFactory,
                           minReconnectDelay: Int,
                           maxReconnectDelay: Int,
                           localDc: Option[String],
                           queryRetries: Int,
                           timeout: Int,
                           readTimeout: Int) {


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

  def apply(useDefaults: Boolean): ClusterConfig =
    ClusterConfig(CassandraSettings(useDefaults))

}