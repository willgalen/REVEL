package com.datastax.driver.scala.core

import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.driver.core.{Cluster, SocketOptions}
import com.datastax.driver.scala.core.conf.{CassandraSettings, CassandraConnectorConf}
import com.datastax.driver.scala.core.policies.{LocalNodeFirstLoadBalancingPolicy, MultipleRetryPolicy}
import com.datastax.driver.scala.core.utils.Reflection

/** Creates native connections to Cassandra.
  * The connector provides a DefaultConnectionFactory.
  * Other factories can be plugged in by setting `cassandra.connection.factory` option.*/
trait CassandraConnectionFactory extends Serializable {

  /** Creates and configures native Cassandra connection */
  def createCluster(conf: CassandraConnectorConf): Cluster

}

/** Performs no authentication. Use with `AllowAllAuthenticator` in Cassandra. */
object SimpleConnectionFactory extends CassandraConnectionFactory {

  /** Returns the Cluster.Builder object used to setup Cluster instance. */
  private def clusterBuilder(conf: CassandraConnectorConf): Cluster.Builder = {
    val options = new SocketOptions()
      .setConnectTimeoutMillis(conf.clusterConf.timeout)
      .setReadTimeoutMillis(conf.clusterConf.readTimeout)

    Cluster.builder()
      .addContactPoints(conf.hosts.toSeq: _*)
      .withPort(conf.nativePort)
      .withRetryPolicy(new MultipleRetryPolicy(conf.clusterConf.queryRetries))
      .withReconnectionPolicy(new ExponentialReconnectionPolicy(conf.clusterConf.minReconnectDelay, conf.clusterConf.maxReconnectDelay))
      .withLoadBalancingPolicy(new LocalNodeFirstLoadBalancingPolicy(conf.hosts, conf.clusterConf.localDc))
      .withAuthProvider(conf.authConf.authProvider)
      .withSocketOptions(options)
  }

  /** Creates and configures native Cassandra connection */
  def createCluster(conf: CassandraConnectorConf): Cluster =
    clusterBuilder(conf).build()

}

object CassandraConnectionFactory {

  def apply(fqcn: Option[String]): CassandraConnectionFactory =
    fqcn.map(Reflection.findGlobalObject[CassandraConnectionFactory])
      .getOrElse(SimpleConnectionFactory)

  def apply(settings: CassandraSettings): CassandraConnectionFactory =
    apply(settings.ConnectionFactoryFqcn)

}