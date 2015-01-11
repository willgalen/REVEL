package com.datastax.driver.scala.core.conf

import java.net.InetAddress

import com.datastax.driver.scala.core.conf.Configuration.Source
import com.datastax.driver.scala.testkit.AbstractSpec
import com.typesafe.config.ConfigFactory

class CassandraSettingsSpec extends AbstractSpec {

  val host = InetAddress.getLocalHost.getHostAddress

  "CassandraSettings" must {

    "have the correct default settings when not passed in from alternate source, env or sys" in {
      val settings = CassandraSettings()
      import settings._

      CassandraHosts.size should be (1)
      CassandraHosts.head should be (InetAddress.getLocalHost)
      NativePort should be (Cluster.DefaultNativePort)
      RpcPort should be(Cluster.DefaultRpcPort)
      ConnectionFactoryFqcn should be(None)
      val auth = AuthConf(settings)
      auth.credentials should be(Map.empty)
      auth should be(NoAuthConf)

      ClusterReconnectDelayMin should be(Cluster.DefaultReconnectDelayMin)
      ClusterReconnectDelayMax should be(Cluster.DefaultReconnectDelayMax)
      ClusterLocalDc should be(None)
      ClusterQueryRetries should be(Cluster.DefaultQueryRetryCountMillis)
      ClusterTimeout should be(Cluster.DefaultTimeoutMillis)
      ClusterReadTimeout should be(Cluster.DefaultReadTimeoutMillis)

      WriteBatchSizeBytes should be(Write.DefaultBatchSizeInBytes)
      WriteBatchSizeRows should be(Write.DefaultBatchSizeRows)
      WriteConsistencyLevel should be(Write.DefaultConsistencyLevel)
      WriteParallelismLevel should be(Write.DefaultParallelismLevel)

      ReadSplitSize should be(Read.DefaultSplitSize)
      ReadFetchSize should be(Read.DefaultFetchSize)
      ReadConsistencyLevel should be(Read.DefaultConsistencyLevel)
    }
    "have the correct settings when passed in from alternate source - Config" in {
      val settings = CassandraSettings(ConfigFactory.parseString(s"""
       cassandra.Cluster.host = "$host"
       cassandra.Cluster.native.port = 9043
       """))
      import settings._

      CassandraHosts.size should be (1)
      CassandraHosts.head should be (InetAddress.getLocalHost)
      // TODO NativePort should be (9043)
    }
    "have the correct settings when passed in from alternate source - Map" in {

      val source = Map(
        "cassandra.Cluster.host" -> s"$host",
        "cassandra.Cluster.native.port" -> "9043")

      val settings = CassandraSettings(Source(source), None)
      import settings._

      CassandraHosts.size should be (1)
      CassandraHosts.head should be (InetAddress.getLocalHost)
      NativePort should be (9043)
    }
    "have the correct settings when set in the environment" in {

    }
    "have the correct settings when set in java system properties" in {

    }
  }
}
