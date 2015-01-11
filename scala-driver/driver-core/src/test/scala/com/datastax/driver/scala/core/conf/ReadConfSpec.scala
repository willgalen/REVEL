package com.datastax.driver.scala.core.conf

import com.datastax.driver.core.ConsistencyLevel
import org.scalatest.{FlatSpec, Matchers}

class ReadConfSpec extends FlatSpec with Matchers {
   import Read._

   "ReadConf" should "be configured with proper defaults" in {
     validate(ReadConf(CassandraSettings(useDefaults = true)))
     validate(ReadConf(CassandraSettings()))

     def validate(conf: ReadConf): Unit = {
       conf.splitSize should be(DefaultSplitSize)
       conf.fetchSize should be(DefaultFetchSize)
       conf.consistencyLevel should be(DefaultConsistencyLevel)
     }
   }
  it should "allow to set split size" in {
    val settings = CassandraSettings(Map(SplitSizeProperty -> "10000"))
    ReadConf(settings).splitSize should be(10000)
  }
  it should "allow to set fetch size" in {
    val settings = CassandraSettings(Map(FetchSizeProperty -> "1200"))
    ReadConf(settings).fetchSize should be(1200)
  }
  it should "allow to set consistency level" in {
    val settings = CassandraSettings(Map(ConsistencyLevelProperty -> "THREE"))
    ReadConf(settings).consistencyLevel should be(ConsistencyLevel.THREE)
  }
}
