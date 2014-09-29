package com.datastax.spark.connector.streaming


import java.util.concurrent.Executors

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import com.datastax.spark.connector.rdd.{CassandraRDD, CqlWhereClause}
import com.datastax.spark.connector.rdd.partitioner.CassandraPartition
import com.datastax.spark.connector.streaming._

import scala.reflect.ClassTag

object CassandraUtils {

  /**
   * Asynchronously stream the data from cassandra input stream. Returns a `ReceiverInputDStream`.
   * Creates an input stream from Cassandra based on the CassandraRDD[T]'s keyspace, table.
   * Uses a custom CassandraReceiver.
   *
   * {{{
   *     val stream: ReceiverInputDStream[T] =
   *       CassandraUtils.createStream[WeatherData](ssc, rdd, storageLevel)
   * }}}
   *
   * @param ssc         StreamingContext object
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream[T: ClassTag](ssc: StreamingContext,
                                rdd: CassandraRDD[T],
                                storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): ReceiverInputDStream[T] =
    new CassandraInputDStream[T](ssc, rdd, storageLevel)

}

private[connector] class CassandraInputDStream[T: ClassTag](@transient ssc: StreamingContext,
                                                            rdd: CassandraRDD[T],
                                                            storageLevel: StorageLevel)
  extends ReceiverInputDStream[T](ssc) with Logging {

  /** Using parens (not needed for Scala since this is paramaterless) from the parent for Java. */
  def getReceiver(): Receiver[T] = new CassandraReceiver[T](ssc, rdd, storageLevel).asInstanceOf[Receiver[T]]

}

private[connector] class CassandraReceiver[T: ClassTag](@transient ssc: StreamingContext,
                                                        rdd: CassandraRDD[T],
                                                        storageLevel: StorageLevel)
  extends Receiver[T](storageLevel) with Logging {

  /* Must start new thread to receive data, as onStart() must be non-blocking.
   * Call store(...) in those threads to store received data into Spark's memory.
   * Call stop(...), restart(...) or reportError(...) on any thread based on how
   *   different errors needs to be handled.
   */
  def onStart() {
    logInfo("Starting Cassandra Consumer Stream.")

    val nodeCpuCount = Runtime.getRuntime.availableProcessors()
    val executorPool = Executors.newFixedThreadPool(nodeCpuCount) // TODO also offer configurable

    try {
      val streams = rdd.toDStreams(ssc, storageLevel)
      streams.foreach { stream => executorPool.submit(new Runnable {
        override def run(): Unit = {
          /* TODO fix store() type clarity
          try for (data <- stream) store(data) catch {
            case e: Throwable => logError("Error handling message; exiting", e)
          }*/
        }
      })}
    } finally {
      executorPool.shutdown() // Terminates the threads
    }
  }

  /** Override this to specify a preferred location (hostname). */
  //override lazy val preferredLocation : Option[T] = None

  def onStop() {
    // Cleanup stuff (stop threads, close sockets, etc.) to stop receiving data.
  }
}

// several stream options to use
class CassandraStream[T: ClassTag]() extends Iterable[T] {
  override def iterator: Iterator[T] = Iterator.empty
}