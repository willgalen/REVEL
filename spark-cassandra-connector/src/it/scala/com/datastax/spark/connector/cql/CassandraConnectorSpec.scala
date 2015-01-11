package com.datastax.spark.connector.cql

import com.datastax.driver.scala.core.conf.Connection
import com.datastax.driver.scala.embedded._
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

class CassandraConnectorSpec extends FlatSpec with Matchers with SharedEmbeddedCassandra {

  useCassandraConfig("cassandra-default.yaml.template")

  val createKeyspaceCql = "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }"

  it should "be configurable from SparkConf" in {
    val host = EmbeddedCassandra.cassandraHost.getHostAddress
    val conf = new SparkConf(loadDefaults = true)
      .set("spark." + Connection.HostProperty, host)

    // would throw exception if connection unsuccessful
    CassandraConnector(conf).withSessionDo { session => }
  }

  it should "accept multiple hostnames in spark.cassandra.connection.host property" in {
    val goodHost = EmbeddedCassandra.cassandraHost.getHostAddress
    val invalidHost = "192.168.254.254"
    // let's connect to two addresses, of which the first one is deliberately invalid
    val conf = new SparkConf(loadDefaults = true)
      .set("spark." + Connection.HostProperty, invalidHost + "," + goodHost)

    // would throw exception if connection unsuccessful
    CassandraConnector(conf).withSessionDo { session => }
  }

  it should "connect to Cassandra with thrift" in {
    val conf = new SparkConf(loadDefaults = true)
      .set("spark." + Connection.HostProperty, EmbeddedCassandra.cassandraHost.getHostAddress)
    CassandraConnector(conf).withCassandraClientDo { client =>
      assert(client.describe_cluster_name() === "Test Cluster")
    }
  }
}
