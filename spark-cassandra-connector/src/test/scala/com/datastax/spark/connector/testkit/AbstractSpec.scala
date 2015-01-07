package com.datastax.spark.connector.testkit

import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}

/** Basic unit test abstraction. */
trait AbstractSpec extends WordSpecLike with Matchers with BeforeAndAfter

private[connector] object TestEvent {

  case object Stop

  case object Completed

  case class WordCount(word: String, count: Int)

}
