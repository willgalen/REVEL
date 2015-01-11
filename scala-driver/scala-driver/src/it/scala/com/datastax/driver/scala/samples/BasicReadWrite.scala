package com.datastax.driver.scala.samples

import java.net.InetAddress

import com.datastax.driver.scala.core.conf.ClusterConfig
import com.datastax.driver.scala.core.utils.Logging

class BasicReadWrite extends App with Logging {
  /** Import this to write to cassandra from a collection of data or read a cassandra table. */
  import com.datastax.driver.scala.core._

  val config = ClusterConfig(InetAddress.getLocalHost)

  CassandraCluster(config).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS test.key_value (key INT PRIMARY KEY, value VARCHAR)")
    session.execute("TRUNCATE test.key_value")
    session.execute("INSERT INTO test.key_value(key, value) VALUES (1, 'first row')")
    session.execute("INSERT INTO test.key_value(key, value) VALUES (2, 'second row')")
    session.execute("INSERT INTO test.key_value(key, value) VALUES (3, 'third row')")
  }

  // Read all: table test.kv and print its contents:
  val stream: Stream[CassandraRow] = cassandra.table("test", "key_value").stream
  stream foreach println

  // Get with where clause: read test.kv and print its contents:
  cassandra.table[(Int, String)]("test", "key_value").where("key = ?", 3).stream
    .foreach { case (k, v) => println(s"Existing Data: as tuples $k value $v")}

  case class KeyValue(k: Int, v: String) extends Serializable
  cassandra.table[KeyValue]("test", "key_value").stream
    .foreach(kv => println(s"Existing Data: as case classes: $kv"))

  // Write two new rows to the test.kv table:
  val data = Seq((4, "fourth row"), (5, "fifth row"))
  data.write("test", "key_value")

  Set((6, "sixth row"), (7, "seventh row")).write("test", "key_value")

  // Show the two new rows were stored in test.kv table:
  cassandra.table("test", "key_value").stream foreach println

  // write in the stream
  // Source(iterable).to(Cassandra("test", "key_value")).run()

  // read in the stream
  // CassandraStream("test", "key_value")
  //   .foreach(func)

}
