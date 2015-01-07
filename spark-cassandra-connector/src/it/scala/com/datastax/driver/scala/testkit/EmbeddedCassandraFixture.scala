package com.datastax.driver.scala.testkit

import com.datastax.driver.scala.core.Connector$
import com.datastax.spark.connector.embedded.EmbeddedCassandra

/** Used for IT tests. */
trait EmbeddedCassandraFixture extends EmbeddedCassandra {

  def clearCache(): Unit = Connector.evictCache()

}
