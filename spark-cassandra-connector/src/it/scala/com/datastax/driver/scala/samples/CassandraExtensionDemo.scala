package com.datastax.driver.scala.samples

import akka.actor.ActorSystem
import com.datastax.driver.scala.CassandraExtension
import com.datastax.driver.scala.core._
 
class CassandraExtensionDemo {

  /**
   * This can be your existing Akka ActorSystem, Spark's.. etc.
   */
  val system = ActorSystem("Cassandra")

  val connector = CassandraExtension(system)

  connector execute Seq(
    "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }",
    "CREATE TABLE IF NOT EXISTS test.key_value (key INT PRIMARY KEY, value VARCHAR)",
    "TRUNCATE test.key_value",
    "INSERT INTO test.key_value(key, value) VALUES (1, 'first row')",
    "INSERT INTO test.key_value(key, value) VALUES (2, 'second row')",
    "INSERT INTO test.key_value(key, value) VALUES (3, 'third row')"
  )

  // Read all: table test.kv and print its contents:
  cassandra.table("test", "key_value").stream foreach println

  // Get with where clause: read test.kv and print its contents:
  cassandra.table[(Int, String)]("test", "key_value").where("key = ?", 3).stream
    .foreach { case (k, v) => println(s"Existing Data: as tuples $k value $v")}

  cassandra.table[KeyValue]("test", "key_value").stream
    .foreach(kv => println(s"Existing Data: as case classes: $kv"))

  // Write two new rows to the test.kv table:
  val data = Seq((4, "fourth row"), (5, "fifth row"))
  data.write("test", "key_value")

  Set((6, "sixth row"), (7, "seventh row")).write("test", "key_value")

  // Show the two new rows were stored in test.kv table:
  cassandra.table("test", "key_value").stream foreach println

  case class KeyValue(k: Int, v: String) extends Serializable
}
