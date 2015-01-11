package com.datastax.spark.connector.cql

import com.datastax.driver.scala.core.conf.PasswordAuthConf
import com.datastax.spark.connector.testkit.SharedEmbeddedCassandra
import org.scalatest.{FlatSpec, Matchers}

class CassandraAuthenticatedConnectorSpec extends FlatSpec with Matchers with SharedEmbeddedCassandra {

  useCassandraConfig("cassandra-password-auth.yaml.template")

  // Wait for the default user to be created in Cassandra.
  Thread.sleep(1000)

  "A CassandraConnector" should "authenticate with username and password when using native protocol" in {
    CassandraConnector(cassandraHost, PasswordAuthConf("cassandra", "cassandra")).withSessionDo { session =>
      assert(session !== null)
      assert(session.isClosed === false)
      assert(session.getCluster.getMetadata.getClusterName === "Test Cluster")
    }
  }
}
