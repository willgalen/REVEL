package com.datastax.spark.connector

import scala.language.implicitConversions

import scala.reflect.ClassTag
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

package object streaming {

  implicit def toStreamingContextFunctions(ssc: StreamingContext): SparkContextFunctions =
    new StreamingContextFunctions(ssc)

  implicit def toDStreamFunctions[T: ClassTag](ds: DStream[T]): DStreamFunctions[T] =
    new DStreamFunctions[T](ds)

}
