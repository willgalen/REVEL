package com.datastax.spark.connector.rdd

import com.datastax.spark.connector.testkit.AbstractFlatSpec
import org.apache.spark.SparkConf
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.scala.core.conf.Read
import com.datastax.spark.connector._

class ReadConfSpec extends AbstractFlatSpec {
  import Read._

  "ReadConf" should "be configured with proper defaults" in {
    val conf = new SparkConf(false)
    val readConf = conf.readConf
    readConf.splitSize should be(DefaultSplitSize)
    readConf.fetchSize should be(DefaultFetchSize)
    readConf.consistencyLevel should be(DefaultConsistencyLevel)
  }
   
  it should "allow to set split size" in {
    val conf = new SparkConf(false).set("spark.cassandra.input.split.size", "10000")
    conf.readConf.splitSize should be(10000)
  }

  it should "allow to set fetch size" in {
    val conf = new SparkConf(false).set("spark.cassandra.input.page.row.size", "1200")
    conf.readConf.fetchSize should be(1200)
  }

  it should "allow to set consistency level" in {
    val conf = new SparkConf(false).set("spark.cassandra.input.consistency.level", "THREE")
    conf.readConf.consistencyLevel should be(ConsistencyLevel.THREE)
  }
}
