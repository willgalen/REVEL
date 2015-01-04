package com.datastax.driver.scala.core

import com.datastax.driver.scala.core.conf.PasswordAuthConf
import com.datastax.driver.scala.testkit.EmbeddedCassandraFixture
import org.scalatest.{FlatSpec, Matchers}

class CassandraAuthenticatedConnectorSpec  extends FlatSpec with Matchers with EmbeddedCassandraFixture {

  useCassandraConfig("cassandra-password-auth.yaml.template")
  val conn = CassandraConnector(cassandraHost, PasswordAuthConf("cassandra", "cassandra"))

  // Wait for the default user to be created in Cassandra.
  Thread.sleep(1000)

  "A CassandraConnector" should "authenticate with username and password when using native protocol" in {
    conn.withSessionDo { session =>
      assert(session !== null)
      assert(session.isClosed === false)
      assert(session.getCluster.getMetadata.getClusterName === "Test Cluster")
    }
  }
}
