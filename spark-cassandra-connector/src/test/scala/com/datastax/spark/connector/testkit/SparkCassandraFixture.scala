package com.datastax.spark.connector.testkit

import com.datastax.driver.scala.core.CassandraConnector
import com.datastax.driver.scala.core.conf.CassandraSettings
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}
import com.datastax.spark.connector.embedded.EmbeddedCassandra

/** Basic unit test abstraction. */
trait AbstractSpec extends WordSpecLike with Matchers with BeforeAndAfter

/** Used for IT tests. */
trait SharedEmbeddedCassandra extends EmbeddedCassandra {

  val settings = new CassandraSettings()

  def clearCache(): Unit = CassandraConnector.evictCache()

}

private[connector] object TestEvent {

  case object Stop

  case object Completed

  case class WordCount(word: String, count: Int)

}
