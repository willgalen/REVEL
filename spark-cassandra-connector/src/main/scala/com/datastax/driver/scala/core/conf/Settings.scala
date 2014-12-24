package com.datastax.driver.scala.core.conf

import java.net.InetAddress

import scala.collection.immutable

final class CassandraSettings extends Serializable {
  import CassandraPropertyNames._

  lazy val CassandraConnectionHosts: immutable.Set[String] =
    withFallback[String](CassandraConnectionHostProperty,
      InetAddress.getLocalHost.getHostAddress).split(",").toSet[String]

  lazy val CassandraConnectionRpcPort = withFallback[Int](CassandraConnectionRpcPortProperty, 9160)
  lazy val CassandraConnectionNativePort = withFallback[Int](CassandraConnectionNativePortProperty, 9042)

  lazy val CassandraUserName: Option[String] = sys.props.get(CassandraUserNameProperty)
  lazy val CassandraPassword: Option[String] = sys.props.get(CassandraPasswordProperty)
  lazy val AuthConfFactoryClassName: Option[String] = sys.props.get(AuthConfFactoryProperty)

  lazy val ConnectionFactoryFQCN: Option[String] = sys.props.get(ConnectionFactoryProperty)

  /** Attempts to acquire from java system properties, falls back to the provided default. */
  def withFallback[T](key: String, default: T): T =
    sys.props.get(key).map (_.asInstanceOf[T]).getOrElse(default)
}

object CassandraPropertyNames extends Serializable {

  val CassandraConnectionHostProperty = "cassandra.connection.host"
  val CassandraConnectionRpcPortProperty = "cassandra.connection.rpc.port"
  val CassandraConnectionNativePortProperty = "cassandra.connection.native.port"
  val ConnectionFactoryProperty = "cassandra.connection.factory"
  val CassandraUserNameProperty = "cassandra.auth.username"
  val CassandraPasswordProperty = "cassandra.auth.password"
  val AuthConfFactoryProperty = "cassandra.auth.conf.factory"
  
  val MinReconnectionDelay = "cassandra.connection.reconnection_delay_ms.min"//, "1000"
  val MaxReconnectionDelay = "cassandra.connection.reconnection_delay_ms.max"//, "60000"
  val LocalDC = "cassandra.connection.local_dc"
  val RetryCount = "cassandra.query.retry.count"//, "10"
  val ConnectTimeout = "cassandra.connection.timeout_ms"//, "5000"
  val ReadTimeout = "cassandra.read.timeout_ms"//, "12000"


}
