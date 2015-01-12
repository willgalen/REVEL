package com.datastax.driver.scala.testkit

import org.scalatest._

/** Basic unit test abstraction. */
trait AbstractSpec extends WordSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll

trait AbstractFlatSpec extends FlatSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll

case class KeyValueWithConversion(key: String, group: Int, value: Long)
case class KeyGroup(key: Int, group: Int)
case class Value(value: String)
case class WriteTimeClass(id: Int, value: String, writeTimeOfValue: Long)
case class TTLClass(id: Int, value: String, ttlOfValue: Int)
case class KeyValue(key: Int, group: Long, value: String)
case class KeyValueWithLongConversion(key: String, group: Int, value: Long)
case class KeyValueWithStringConversion(key: String, group: Int, value: String)
case class KeyValueWithTTL(key: Int, group: Long, value: String, ttl: Int)
case class KeyValueWithTimestamp(key: Int, group: Long, value: String, timestamp: Long)
case class CustomerId(id: String)