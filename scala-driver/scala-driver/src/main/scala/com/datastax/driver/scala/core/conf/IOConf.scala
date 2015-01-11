package com.datastax.driver.scala.core.conf

import com.datastax.driver.core.{ConsistencyLevel, DataType}
import com.datastax.driver.scala.core._
import com.datastax.driver.scala.core.io.{PerRowWriteOption, TimestampOption, TTLOption, WriteOption}
import com.datastax.driver.scala.types.ColumnType

sealed trait IOConf {
  /** The consistency level for a read or write. */
  def consistencyLevel: ConsistencyLevel
}

/** Read settings
  * @param splitSize approx number of Cassandra partitions to be read in a single task
  *
  * @param fetchSize number of CQL rows fetched per round trip to Cassandra, default 1000
  *
  * @param consistencyLevel consistency level for reads, default LOCAL_ONE;
  *                         higher consistency level will disable data-locality
  */
case class ReadConf(splitSize: Int = Read.DefaultSplitSize,
                    fetchSize: Int = Read.DefaultFetchSize,
                    consistencyLevel: ConsistencyLevel = Read.DefaultConsistencyLevel) extends IOConf

object ReadConf {
  /** Returns a [[com.datastax.driver.scala.core.conf.ReadConf]] with optional settings that
    * fall back to java system properties. If unavailable, falls back to default. */
  def apply(settings: CassandraSettings): ReadConf =
    ReadConf(splitSize = settings.ReadSplitSize,
      fetchSize = settings.ReadFetchSize,
      consistencyLevel = settings.ReadConsistencyLevel)

}

/** Write settings.
  *
  * @param batchSize approx. number of bytes to be written in a single batch or
  *                  exact number of rows to be written in a single batch;
  *
  * @param consistencyLevel consistency level for writes, default LOCAL_ONE
  *
  * @param parallelismLevel number of batches to be written in parallel
  *
  * @param ttl       the default TTL value which is used when it is defined (in seconds)
  *
  * @param timestamp the default timestamp value which is used when it is defined (in microseconds)
  */

case class WriteConf(batchSize: BatchSize = BatchSize.Automatic,
                     consistencyLevel: ConsistencyLevel = Write.DefaultConsistencyLevel,
                     parallelismLevel: Int = Write.DefaultParallelismLevel,
                     ttl: TTLOption = TTLOption.auto,
                     timestamp: TimestampOption = TimestampOption.auto) extends IOConf {

  private[driver] val optionPlaceholders: Seq[String] = Seq(ttl, timestamp).collect {
    case PerRowWriteOption(placeholder) => placeholder
  }

  private[driver] val optionsAsColumns: (String, String) => Seq[ColumnDef] = { (keyspace, table) =>
    def toRegularColDef(opt: WriteOption[_], dataType: DataType) = opt match {
      case PerRowWriteOption(placeholder) =>
        Some(ColumnDef(keyspace, table, placeholder, RegularColumn, ColumnType.fromDriverType(dataType)))
      case _ => None
    }

    Seq(toRegularColDef(ttl, DataType.cint()), toRegularColDef(timestamp, DataType.bigint())).flatten
  }
}

object WriteConf {
  /** Returns a [[com.datastax.driver.scala.core.conf.WriteConf]] with optional settings that
    * fall back to java system properties. If unavailable, falls back to default. */
  def apply(settings: CassandraSettings): WriteConf = WriteConf(
      batchSize = BatchSize(settings.WriteBatchSizeBytes, settings.WriteBatchSizeRows),
      consistencyLevel = settings.WriteConsistencyLevel,
      parallelismLevel = settings.WriteParallelismLevel)

}
