package com.datastax.driver.scala.core.partition

import scala.language.existentials

trait TokenFactory[V, T <: Token[V]] {
  def minToken: T
  def maxToken: T
  def totalTokenCount: BigInt
  def fromString(string: String): T
  def toString(token: T): String
}

object TokenFactory {

  type V = t forSome {type t}
  type T = t forSome {type t <: Token[V]}

  implicit object Murmur3TokenFactory extends TokenFactory[Long, LongToken] {
    val minToken = LongToken(Long.MinValue)
    val maxToken = LongToken(Long.MaxValue)
    val totalTokenCount = BigInt(maxToken.value) - BigInt(minToken.value)
    def fromString(string: String) = LongToken(string.toLong)
    def toString(token: LongToken) = token.value.toString
  }

  implicit object RandomPartitionerTokenFactory extends TokenFactory[BigInt, BigIntToken] {
    val minToken = BigIntToken(-1)
    val maxToken = BigIntToken(BigInt(2).pow(127))
    val totalTokenCount = maxToken.value - minToken.value
    def fromString(string: String) = BigIntToken(BigInt(string))
    def toString(token: BigIntToken) = token.value.toString()
  }

  def apply(fqcn: String): TokenFactory[V, T] =
    (fqcn match {
      case null => throw new IllegalStateException("'partitionerClassName' must not be null.")
      case "org.apache.cassandra.dht.Murmur3Partitioner" => Murmur3TokenFactory
      case "org.apache.cassandra.dht.RandomPartitioner" => RandomPartitionerTokenFactory
      case _ => throw new IllegalArgumentException(s"Unsupported partitioner: $fqcn")
    }).asInstanceOf[TokenFactory[V, T]]
}




