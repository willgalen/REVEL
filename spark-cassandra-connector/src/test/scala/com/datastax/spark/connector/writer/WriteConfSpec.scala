package com.datastax.spark.connector.writer

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.scala.core.conf.Write
import com.datastax.driver.scala.core.{BytesInBatch, RowsInBatch}
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}
import com.datastax.spark.connector._

class WriteConfSpec extends FlatSpec with Matchers {
  import Write._

  "WriteConf" should "be configured with proper defaults" in {
    val conf = new SparkConf(false)
    val writeConf = conf.writeConf

    writeConf.batchSize should be(BytesInBatch(DefaultBatchSizeInBytes))
    writeConf.consistencyLevel should be(DefaultConsistencyLevel)
    writeConf.parallelismLevel should be(DefaultParallelismLevel)
  }

  it should "allow to set consistency level" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.consistency.level", "THREE")
    val writeConf = conf.writeConf

    writeConf.consistencyLevel should be(ConsistencyLevel.THREE)
  }

  it should "allow to set parallelism level" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.concurrent.writes", "17")
    val writeConf = conf.writeConf

    writeConf.parallelismLevel should be(17)
  }

  it should "allow to set batch size in bytes" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.size.bytes", "12345")
    val writeConf = conf.writeConf

    writeConf.batchSize should be(BytesInBatch(12345))
  }

  it should "allow to set batch size in bytes when rows are set to auto" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.size.bytes", "12345")
      .set("spark.cassandra.output.batch.size.rows", "auto")
    val writeConf = conf.writeConf

    writeConf.batchSize should be(BytesInBatch(12345))
  }

  it should "allow to set batch size in rows" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.size.rows", "12345")
    val writeConf = conf.writeConf

    writeConf.batchSize should be(RowsInBatch(12345))
  }
}
