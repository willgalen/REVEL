package com.datastax.driver.scala.core.conf

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.scala.core.{BytesInBatch, RowsInBatch}
import org.scalatest.{FlatSpec, Matchers}

class WriteConfTest extends FlatSpec with Matchers {
  import Write._

  "WriteConf" should "be configured with proper defaults" in {
    validate(WriteConf.Default)
    validate(WriteConf(CassandraSettings()))

    def validate(conf: WriteConf): Unit = {
      conf.batchSize should be(BytesInBatch(DefaultBatchSizeInBytes))
      conf.consistencyLevel should be(DefaultConsistencyLevel)
      conf.parallelismLevel should be(DefaultParallelismLevel)
    }
  }

  it should "allow to set consistency level" in {
    val settings = CassandraSettings(Map(ConsistencyLevelProperty -> "THREE"))
    WriteConf(settings).consistencyLevel should be(ConsistencyLevel.THREE)
  }

  it should "allow to set parallelism level" in {
    val settings = CassandraSettings(Map(ParallelismLevelProperty -> "17"))
    WriteConf(settings).parallelismLevel should be(17)
  }

  it should "allow to set batch size in bytes" in {
    val settings = CassandraSettings(Map(BatchSizeInBytesProperty -> "12345"))
    WriteConf(settings).batchSize should be(BytesInBatch(12345))
  }

  it should "allow to set batch size in bytes when rows are set to auto" in {
    val settings = CassandraSettings(Map(
      BatchSizeInBytesProperty ->"12345", BatchSizeInRowsProperty -> "auto"))
    WriteConf(settings).batchSize should be(BytesInBatch(12345))
  }

  it should "allow to set batch size in rows" in {
    val settings = CassandraSettings(Map(BatchSizeInRowsProperty -> "12345"))
    WriteConf(settings).batchSize should be(RowsInBatch(12345))
  }
}
