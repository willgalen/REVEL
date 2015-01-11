package com.datastax.driver.scala.core

import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.driver.core.{Cluster, SocketOptions}
import com.datastax.driver.scala.core.conf.{ClusterConfig, CassandraSettings}
import com.datastax.driver.scala.core.policies.{LocalNodeFirstLoadBalancingPolicy, MultipleRetryPolicy}
import com.datastax.driver.scala.core.utils.Reflection

/** Creates native connections to Cassandra.
  * The connector provides a DefaultConnectionFactory.
  * Other factories can be plugged in by setting `cassandra.connection.factory` option.*/
trait CassandraConnectionFactory extends Serializable {

  /** Creates and configures native Cassandra connection */
  def createCluster(conf: ClusterConfig): Cluster

}

/** Performs no authentication. Use with `AllowAllAuthenticator` in Cassandra. */
object SimpleConnectionFactory extends CassandraConnectionFactory {

  /** Returns the Cluster.Builder object used to setup Cluster instance. */
  private def clusterBuilder(conf: ClusterConfig): Cluster.Builder = {
    val options = new SocketOptions()
      .setConnectTimeoutMillis(conf.timeout)
      .setReadTimeoutMillis(conf.readTimeout)

    Cluster.builder()
      .addContactPoints(conf.hosts.toSeq: _*)
      .withPort(conf.nativePort)
      .withRetryPolicy(new MultipleRetryPolicy(conf.queryRetries))
      .withReconnectionPolicy(new ExponentialReconnectionPolicy(conf.minReconnectDelay, conf.maxReconnectDelay))
      .withLoadBalancingPolicy(new LocalNodeFirstLoadBalancingPolicy(conf.hosts, conf.localDc))
      .withAuthProvider(conf.authConf.authProvider)
      .withSocketOptions(options)
  }

  /** Creates and configures native Cassandra connection */
  def createCluster(conf: ClusterConfig): Cluster =
    clusterBuilder(conf).build()

}

object CassandraConnectionFactory {

  def apply(settings: CassandraSettings): CassandraConnectionFactory =
    settings.ConnectionFactoryFqcn.map(Reflection.findGlobalObject[CassandraConnectionFactory])
      .getOrElse(SimpleConnectionFactory)

}