package com.datastax.driver.scala.testkit

import com.datastax.driver.scala.core.CassandraCluster
import com.datastax.driver.scala.embedded.EmbeddedCassandra

/** Used for IT tests. */
trait CassandraSpec extends EmbeddedCassandra {

  def clearCache(): Unit = CassandraCluster.evictCache()

}
