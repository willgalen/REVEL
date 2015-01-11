package com.datastax.driver.scala.core.io

import java.io.IOException
import java.net.InetAddress

import com.datastax.driver.scala.core.conf.WriteConf
import com.datastax.driver.scala.mapping.DefaultColumnMapper
import com.datastax.driver.scala.testkit._
import com.datastax.driver.scala.types.{UDTValue, TypeConverter}

import scala.collection.JavaConversions._

class TableWriterSpec extends AbstractFlatSpec with CassandraSpec {
  import com.datastax.driver.scala.core._

  useCassandraConfig("cassandra-default.yaml.template")
  val conn = CassandraCluster(cassandraHost)

  conn.withSessionDo { session =>
    session.execute("DROP KEYSPACE IF EXISTS write_test")
    session.execute("CREATE KEYSPACE IF NOT EXISTS write_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")

    for (x <- 1 to 16) {
      session.execute(s"CREATE TABLE IF NOT EXISTS write_test.key_value_$x (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))")
    }

    session.execute("CREATE TABLE IF NOT EXISTS write_test.nulls (key INT PRIMARY KEY, text_value TEXT, int_value INT)")
    session.execute("CREATE TABLE IF NOT EXISTS write_test.collections (key INT PRIMARY KEY, l list<text>, s set<text>, m map<text, text>)")
    session.execute("CREATE TABLE IF NOT EXISTS write_test.blobs (key INT PRIMARY KEY, b blob)")
    session.execute("CREATE TABLE IF NOT EXISTS write_test.counters (pkey INT, ckey INT, c1 counter, c2 counter, PRIMARY KEY (pkey, ckey))")
    session.execute("CREATE TABLE IF NOT EXISTS write_test.counters2 (pkey INT PRIMARY KEY, c counter)")
    session.execute("CREATE TABLE IF NOT EXISTS write_test.\"camelCase\" (\"primaryKey\" INT PRIMARY KEY, \"textValue\" text)")
    session.execute("CREATE TABLE IF NOT EXISTS write_test.single_column (pk INT PRIMARY KEY)")

    session.execute("CREATE TYPE write_test.address (street text, city text, zip int)")
    session.execute("CREATE TABLE IF NOT EXISTS write_test.udts(key INT PRIMARY KEY, name text, addr frozen<address>)")

  }

  private def verifyKeyValueTable(tableName: String) {
    conn.withSessionDo { session =>
      val result = session.execute("SELECT * FROM write_test." + tableName).all()
      result should have size 3
      for (row <- result) {
        Some(row.getInt(0)) should contain oneOf(1, 2, 3)
        Some(row.getLong(1)) should contain oneOf(1, 2, 3)
        Some(row.getString(2)) should contain oneOf("value1", "value2", "value3")
      }
    }
  }

  "A TableWriter" should "write a collection of tuples" in {
    val col = Seq((1, 1L, "value1"), (2, 2L, "value2"), (3, 3L, "value3"))
    col.write("write_test", "key_value_1", SomeColumns("key", "group", "value"))
    verifyKeyValueTable("key_value_1")
  }

  it should "write a collection of tuples applying proper data type conversions" in {
    val col = Seq(("1", "1", "value1"), ("2", "2", "value2"), ("3", "3", "value3"))
    col.write("write_test", "key_value_2")
    verifyKeyValueTable("key_value_2")
  }

  it should "write a collection of case class objects" in {
    val col = Seq(KeyValue(1, 1L, "value1"), KeyValue(2, 2L, "value2"), KeyValue(3, 3L, "value3"))
    col.write("write_test", "key_value_3")
    verifyKeyValueTable("key_value_3")
  }

  it should "write a collection of case class objects applying proper data type conversions" in {
    val col = Seq(
      KeyValueWithStringConversion("1", 1, "value1"),
      KeyValueWithStringConversion("2", 2, "value2"),
      KeyValueWithStringConversion("3", 3, "value3")
    )
    col.write("write_test", "key_value_4")
    verifyKeyValueTable("key_value_4")
  }

  it should "write a collection of CassandraRow objects" in {
    val col = Seq(
      CassandraRow.fromMap(Map("key" -> 1, "group" -> 1L, "value" -> "value1")),
      CassandraRow.fromMap(Map("key" -> 2, "group" -> 2L, "value" -> "value2")),
      CassandraRow.fromMap(Map("key" -> 3, "group" -> 3L, "value" -> "value3"))
    )
    col.write("write_test", "key_value_5")
    verifyKeyValueTable("key_value_5")
  }

  it should "write a collection of CassandraRow objects applying proper data type conversions" in {
    val col = Seq(
      CassandraRow.fromMap(Map("key" -> "1", "group" -> BigInt(1), "value" -> "value1")),
      CassandraRow.fromMap(Map("key" -> "2", "group" -> BigInt(2), "value" -> "value2")),
      CassandraRow.fromMap(Map("key" -> "3", "group" -> BigInt(3), "value" -> "value3"))
    )
    col.write("write_test", "key_value_6")
    verifyKeyValueTable("key_value_6")
  }

  it should "write a collection of tuples to a table with camel case column names" in {
    val col = Seq((1, "value1"), (2, "value2"), (3, "value3"))
    col.write("write_test", "camelCase", SomeColumns("primaryKey", "textValue"))
    conn.withSessionDo { session =>
      val result = session.execute("SELECT * FROM write_test.\"camelCase\"").all()
      result should have size 3
      for (row <- result) {
        Some(row.getInt(0)) should contain oneOf(1, 2, 3)
        Some(row.getString(1)) should contain oneOf("value1", "value2", "value3")
      }
    }
  }

  it should "write empty values" in {
    val col = Seq((1, 1L, None))
    col.write("write_test", "key_value_7", SomeColumns("key", "group", "value"))
    conn.withSessionDo { session =>
      val result = session.execute("SELECT * FROM write_test.key_value_7").all()
      result should have size 1
      for (row <- result) {
        row.getString(2) should be (null)
      }
    }
  }

  it should "write null values" in {
    val key = 1.asInstanceOf[AnyRef]
    val row = new CassandraRow(IndexedSeq("key", "text_value", "int_value"), IndexedSeq(key, null, null))

    Seq(row).write("write_test", "nulls")
    conn.withSessionDo { session =>
      val result = session.execute("SELECT * FROM write_test.nulls").all()
      result should have size 1
      for (r <- result) {
        r.getInt(0) shouldBe key
        r.isNull(1) shouldBe true
        r.isNull(2) shouldBe true
      }
    }
  }

  it should "write only specific column data if ColumnNames is passed as 'columnNames'" in {
    val col = Seq((1, 1L, None))
    col.write("write_test", "key_value_8", SomeColumns("key", "group"))
    conn.withSessionDo { session =>
      val result = session.execute("SELECT * FROM write_test.key_value_8").all()
      result should have size 1
      for (row <- result) {
        row.getInt(0) should be (1)
        row.getString(2) should be (null)
      }
    }
  }

  it should "distinguish (deprecated) implicit `seqToSomeColumns`" in {
    val col = Seq((2, 1L, None))
    col.write("write_test", "key_value_9", SomeColumns("key", "group"))
    conn.withSessionDo { session =>
      val result = session.execute("SELECT * FROM write_test.key_value_9").all()
      result should have size 1
      for (row <- result) {
        row.getInt(0) should be (2)
        row.getString(2) should be (null)
      }
    }
  }

  it should "write collections" in {
    val col = Seq(
      (1, Vector("item1", "item2"), Set("item1", "item2"), Map("key1" -> "value1", "key2" -> "value2")),
      (2, Vector.empty[String], Set.empty[String], Map.empty[String, String]))
    col.write("write_test", "collections", SomeColumns("key", "l", "s", "m"))

    conn.withSessionDo { session =>
      val result = session.execute("SELECT * FROM write_test.collections").all()
      result should have size 2
      val rows = result.groupBy(_.getInt(0)).mapValues(_.head)
      val row0 = rows(1)
      val row1 = rows(2)
      row0.getList("l", classOf[String]).toSeq shouldEqual Seq("item1", "item2")
      row0.getSet("s", classOf[String]).toSeq shouldEqual Seq("item1", "item2")
      row0.getMap("m", classOf[String], classOf[String]).toMap shouldEqual Map("key1" -> "value1", "key2" -> "value2")
      row1.isNull("l") shouldEqual true
      row1.isNull("m") shouldEqual true
      row1.isNull("s") shouldEqual true
    }
  }

  it should "write blobs" in {
    val col = Seq((1, Some(Array[Byte](0, 1, 2, 3))), (2, None))
    col.write("write_test", "blobs", SomeColumns("key", "b"))
    conn.withSessionDo { session =>
      val result = session.execute("SELECT * FROM write_test.blobs").all()
      result should have size 2
      val rows = result.groupBy(_.getInt(0)).mapValues(_.head)
      val row0 = rows(1)
      val row1 = rows(2)
      row0.getBytes("b").remaining shouldEqual 4
      row1.isNull("b") shouldEqual true
    }
  }

  it should "increment and decrement counters" in {
    val col1 = Seq((0, 0, 1, 1))
    col1.write("write_test", "counters", SomeColumns("pkey", "ckey", "c1", "c2"))
    conn.withSessionDo { session =>
      val result = session.execute("SELECT * FROM write_test.counters").one()
      result.getLong("c1") shouldEqual 1L
      result.getLong("c2") shouldEqual 1L
    }
    val col2 = Seq((0, 0, 1))
    col2.write("write_test", "counters", SomeColumns("pkey", "ckey", "c2"))
    conn.withSessionDo { session =>
      val result = session.execute("SELECT * FROM write_test.counters").one()
      result.getLong("c1") shouldEqual 1L
      result.getLong("c2") shouldEqual 2L
    }
  }

  it should "increment and decrement counters in batches" in {
    val rowCount = 10000
    val col = for (i <- 1 to rowCount) yield (i, 1)
    col.write("write_test", "counters2", SomeColumns("pkey", "c"))
    cassandra.table("write_test", "counters2").stream.toList.size should be(rowCount)
  }

  it should "write values of user-defined classes" in {
    TypeConverter.registerConverter(new TypeConverter[String] {
      def targetTypeTag = scala.reflect.runtime.universe.typeTag[String]
      def convertPF = { case CustomerId(id) => id }
    })

    val col = Seq((1, 1L, CustomerId("foo")))
    col.write("write_test", "key_value_10", SomeColumns("key", "group", "value"))

    conn.withSessionDo { session =>
      val result = session.execute("SELECT * FROM write_test.key_value_10").all()
      result should have size 1
      for (row <- result)
        row.getString(2) shouldEqual "foo"
    }
  }

  it should "write values of user-defined-types in Cassandra" in {
    val address = UDTValue(Map("city" -> "Warsaw", "zip" -> 10000, "street" -> "Marszałkowska"))
    val col = Seq((1, "Joe", address))
    col.write("write_test", "udts", SomeColumns("key", "name", "addr"))

    conn.withSessionDo { session =>
      val result = session.execute("SELECT key, name, addr FROM write_test.udts").all()
      result should have size 1
      for (row <- result) {
        row.getInt(0) shouldEqual 1
        row.getString(1) shouldEqual "Joe"
        row.getUDTValue(2).getString("city") shouldEqual "Warsaw"
        row.getUDTValue(2).getInt("zip") shouldEqual 10000
      }
    }
  }


  it should "write to single-column tables" in {
    val col = Seq(1, 2, 3, 4, 5).map(Tuple1.apply)
    col.write("write_test", "single_column", SomeColumns("pk"))
    conn.withSessionDo { session =>
      val result = session.execute("SELECT * FROM write_test.single_column").all()
      result should have size 5
      result.map(_.getInt(0)).toSet should be (Set(1, 2, 3, 4, 5))
    }
  }

  it should "throw IOException if table is not found" in {
    val col = Seq(("1", "1", "value1"), ("2", "2", "value2"), ("3", "3", "value3"))
    intercept[IOException] {
      col.write("write_test", "unknown_table")
    }
  }

  it should "write RDD of case class objects with default TTL" in {
    val col = Seq(KeyValue(1, 1L, "value1"), KeyValue(2, 2L, "value2"), KeyValue(3, 3L, "value3"))
    col.write("write_test", "key_value_11", writeConf = WriteConf(ttl = TTLOption.constant(100)))

    verifyKeyValueTable("key_value_11")

    conn.withSessionDo { session =>
      val result = session.execute("SELECT TTL(value) FROM write_test.key_value_11").all()
      result should have size 3
      result.foreach(_.getInt(0) should be > 50)
      result.foreach(_.getInt(0) should be <= 100)
    }
  }

  it should "write RDD of case class objects with default timestamp" in {
    val col = Seq(KeyValue(1, 1L, "value1"), KeyValue(2, 2L, "value2"), KeyValue(3, 3L, "value3"))
    val ts = System.currentTimeMillis() - 1000L
    col.write("write_test", "key_value_12", writeConf = WriteConf(timestamp = TimestampOption.constant(ts * 1000L)))

    verifyKeyValueTable("key_value_12")

    conn.withSessionDo { session =>
      val result = session.execute("SELECT WRITETIME(value) FROM write_test.key_value_12").all()
      result should have size 3
      result.foreach(_.getLong(0) should be (ts * 1000L))
    }
  }

  it should "write RDD of case class objects with per-row TTL" in {
    val col = Seq(KeyValueWithTTL(1, 1L, "value1", 100), KeyValueWithTTL(2, 2L, "value2", 200), KeyValueWithTTL(3, 3L, "value3", 300))
    col.write("write_test", "key_value_13", writeConf = WriteConf(ttl = TTLOption.perRow("ttl")))

    verifyKeyValueTable("key_value_13")

    conn.withSessionDo { session =>
      val result = session.execute("SELECT key, TTL(value) FROM write_test.key_value_13").all()
      result should have size 3
      result.foreach(row => {
        row.getInt(1) should be > (100 * row.getInt(0) - 50)
        row.getInt(1) should be <= (100 * row.getInt(0))
      })
    }
  }

  it should "write RDD of case class objects with per-row timestamp" in {
    val ts = System.currentTimeMillis() - 1000L
    val col = Seq(KeyValueWithTimestamp(1, 1L, "value1", ts * 1000L + 100L), KeyValueWithTimestamp(2, 2L, "value2", ts * 1000L + 200L), KeyValueWithTimestamp(3, 3L, "value3", ts * 1000L + 300L))
    col.write("write_test", "key_value_14", writeConf = WriteConf(timestamp = TimestampOption.perRow("timestamp")))

    verifyKeyValueTable("key_value_14")

    conn.withSessionDo { session =>
      val result = session.execute("SELECT key, WRITETIME(value) FROM write_test.key_value_14").all()
      result should have size 3
      result.foreach(row => {
        row.getLong(1) should be (ts * 1000L + row.getInt(0) * 100L)
      })
    }
  }

  it should "write RDD of case class objects with per-row TTL with custom mapping" in {
    val col = Seq(KeyValueWithTTL(1, 1L, "value1", 100), KeyValueWithTTL(2, 2L, "value2", 200), KeyValueWithTTL(3, 3L, "value3", 300))
    col.write("write_test", "key_value_15", writeConf = WriteConf(ttl = TTLOption.perRow("ttl_placeholder")))(
      conn, DefaultRowWriter.factory(new DefaultColumnMapper(Map("ttl" -> "ttl_placeholder"))))

    verifyKeyValueTable("key_value_15")

    conn.withSessionDo { session =>
      val result = session.execute("SELECT key, TTL(value) FROM write_test.key_value_15").all()
      result should have size 3
      result.foreach(row => {
        row.getInt(1) should be > (100 * row.getInt(0) - 50)
        row.getInt(1) should be <= (100 * row.getInt(0))
      })
    }
  }

  it should "write RDD of case class objects with per-row timestamp with custom mapping" in {
    val ts = System.currentTimeMillis() - 1000L
    val col = Seq(KeyValueWithTimestamp(1, 1L, "value1", ts * 1000L + 100L), KeyValueWithTimestamp(2, 2L, "value2", ts * 1000L + 200L), KeyValueWithTimestamp(3, 3L, "value3", ts * 1000L + 300L))
    col.write("write_test", "key_value_16",
      writeConf = WriteConf(timestamp = TimestampOption.perRow("timestamp_placeholder")))(
        conn, DefaultRowWriter.factory(new DefaultColumnMapper(Map("timestamp" -> "timestamp_placeholder"))))

    verifyKeyValueTable("key_value_16")

    conn.withSessionDo { session =>
      val result = session.execute("SELECT key, WRITETIME(value) FROM write_test.key_value_16").all()
      result should have size 3
      result.foreach(row => {
        row.getLong(1) should be (ts * 1000L + row.getInt(0) * 100L)
      })
    }
  }

}