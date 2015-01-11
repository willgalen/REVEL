package com.datastax.spark.connector

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.driver.scala.core.conf._
import com.datastax.driver.scala.core.conf.Configuration._
import com.datastax.driver.scala.core.io.RowReaderFactory
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.scala.core.CassandraRow
import com.datastax.driver.scala.mapping.ColumnMapper
import com.datastax.spark.connector.rdd.{ValidRDDType, CassandraRDD}

import scala.reflect.ClassTag

/** Provides Cassandra-specific methods on `SparkContext`. */
class SparkContextFunctions(@transient val sc: SparkContext) extends ConfigurationFunctions {

  /**
   * Transforms any optionally set configuration settings in `SparkConf` from `SparkContext`
   * to property names and values used in the scala driver. In this way, you can
   * leverage the implicit fallback chain of settings:
   * 1. Any settings in SparkConf from `SparkContext`
   * 2. Any settings in the environment
   * 3. Any settings in java system properties
   * 4. Default Settings
   *
   * This functionality is made available on `SparkContext` by importing `com.datastax.spark.connector._`
   * {{{
   *   import com.datastax.spark.connector._
   *
   *   val sc = new SparkContext()
   *
   *   val settings: CassandraSettings = sc.settings
   *   CassandraConnector(sc.connectorConf)
   *
   *   val rdd = sc.cassandraTable("test", "key_value")
   *
   *   sc.parallelize(Seq((4, "fourth row"), (5, "fifth row")))
   *     .saveToCassandra("test", "key_value", SomeColumns("key", "value"), sc.writeConf)
   *
   * }}}
   */
  def conf: SparkConf = sc.getConf

  /** Returns a view of a Cassandra table as `CassandraRDD`.
    * This method is made available on `SparkContext` by importing `com.datastax.spark.connector._`
    *
    * Depending on the type parameter passed to `cassandraTable`, every row is converted to one of the following:
    *   - an [[CassandraRow]] object (default, if no type given)
    *   - a tuple containing column values in the same order as columns selected by
    *   [[com.datastax.spark.connector.rdd.CassandraRDD#select CassandraRDD#select]]
    *   - object of a user defined class, populated by appropriate [[ColumnMapper ColumnMapper]]
    *
    * Example:
    * {{{
    *   CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
    *   CREATE TABLE test.words (word text PRIMARY KEY, count int);
    *   INSERT INTO test.words (word, count) VALUES ('foo', 20);
    *   INSERT INTO test.words (word, count) VALUES ('bar', 20);
    *   ...
    * }}}
    * {{{
    *   // Obtaining RDD of CassandraRow objects:
    *   val rdd1 = sc.cassandraTable("test", "words")
    *   rdd1.first.getString("word")  // foo
    *   rdd1.first.getInt("count")    // 20
    *
    *   // Obtaining RDD of tuples:
    *   val rdd2 = sc.cassandraTable[(String, Int)]("test", "words").select("word", "count")
    *   rdd2.first._1  // foo
    *   rdd2.first._2  // 20
    *
    *   // Obtaining RDD of user defined objects:
    *   case class WordCount(word: String, count: Int)
    *   val rdd3 = sc.cassandraTable[WordCount]("test", "words")
    *   rdd3.first.word  // foo
    *   rdd3.first.count // 20
    * }}}
    */
  def cassandraTable[T](keyspace: String, table: String)
                       (implicit connector: CassandraConnector = CassandraConnector(sc.getConf),
                        ct: ClassTag[T], rrf: RowReaderFactory[T],
                        ev: ValidRDDType[T]) =
    new CassandraRDD[T](sc, connector, keyspace, table, readConf = sc.readConf)
}

/** Transforms any optionally set configuration settings in `SparkConf` to
  * property names and values used in the scala driver. In this way, you can
  * leverage the implicit fallback chain of settings:
  * 1. Any settings in SparkConf
  * 2. Any settings in the environment
  * 3. Any settings in java system properties
  * 4. Default Settings
  *
  * This functionality is made available on `SparkConf` by importing `com.datastax.spark.connector._`
  * {{{
  *   import com.datastax.spark.connector._
  *
  *   val conf = new SparkConf()
  *
  *   val settings: CassandraSettings = conf.settings
  *
  *   CassandraConnector(conf) // one way to create it
  *   CassandraConnector(conf.clusterConfig) // another way
  *
  *   val rdd = sc.cassandraTable("test", "key_value")
  *
  *   sc.parallelize(Seq((4, "fourth row"), (5, "fifth row")))
  *     .saveToCassandra("test", "key_value", SomeColumns("key", "value"), conf.writeConf)
  * }}}
  */
class SparkConfFunctions(@transient val conf: SparkConf) extends ConfigurationFunctions

trait ConfigurationFunctions extends Serializable {

  def conf: SparkConf

  def settings: CassandraSettings = CassandraSettings(Source(filtered), Some("spark."))

  def clusterConfig: ClusterConfig = ClusterConfig(settings)

  def writeConf: WriteConf = WriteConf(settings)

  def readConf: ReadConf = ReadConf(settings)

  private def filtered: Map[String,String] =
    conf.getAll.toMap.filterKeys(namespace)

  private def namespace(value: String) =
    value.startsWith("spark.cassandra.") || value.startsWith("cassandra.")
}
