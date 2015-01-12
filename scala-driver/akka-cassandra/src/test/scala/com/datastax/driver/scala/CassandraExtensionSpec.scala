package com.datastax.driver.scala

import java.net.InetAddress

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.datastax.driver.scala.core.SimpleConnectionFactory
import com.datastax.driver.scala.core.conf.{NoAuthConf, Cluster}
import com.datastax.driver.scala.testkit.AbstractSpec

class CassandraExtensionSpec extends TestKit(ActorSystem("Cassandra")) with AbstractSpec {

  val cassandra = CassandraExtension(system)
  val config = cassandra.context.config

  override def afterAll() { system.shutdown() }

  "CassandraExtension" must {
    "have the expected configurations with default settings" in {

      config.hosts.size should be (1)
      config.hosts should contain (InetAddress.getLocalHost)
      config.nativePort should be (Cluster.DefaultNativePort)
      config.rpcPort should be (Cluster.DefaultRpcPort)
      config.authConf should be (NoAuthConf)
      config.connectionFactory should be (SimpleConnectionFactory)
      config.minReconnectDelay should be (Cluster.DefaultReconnectDelayMin)
      config.maxReconnectDelay should be (Cluster.DefaultReconnectDelayMax)
      config.localDc should be (None)
      config.queryRetries should be (Cluster.DefaultQueryRetryCountMillis)
      config.timeout should be (Cluster.DefaultTimeoutMillis)
      config.readTimeout should be (Cluster.DefaultReadTimeoutMillis)
    }
  }
}
