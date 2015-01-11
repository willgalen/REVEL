package com.datastax.driver.scala.core

import org.apache.commons.configuration.ConfigurationException
import com.datastax.driver.scala.core.conf.Write

sealed trait BatchSize
case class RowsInBatch(batchSize: Int) extends BatchSize
case class BytesInBatch(batchSize: Int) extends BatchSize
object BatchSize {
  private val Number = "([0-9]+)".r
  val Automatic = BytesInBatch(Write.DefaultBatchSizeInBytes)

  @deprecated("Use com.datastax.driver.scala.core.RowsInBatch instead of a number", "1.1")
  implicit def intToFixedBatchSize(batchSize: Int): RowsInBatch = RowsInBatch(batchSize)

  def apply(batchSizeInBytes: Int, batchSizeInRows: String): BatchSize = {
    val Number = "([0-9]+)".r
    batchSizeInRows match {
      case "auto"    => BytesInBatch(batchSizeInBytes)
      case Number(x) => RowsInBatch(x.toInt)
      case other     =>
        throw new ConfigurationException(
          s"Invalid value of cassandra.output.batch.size.rows: $other. Number or 'auto' expected")
    }
  }
}