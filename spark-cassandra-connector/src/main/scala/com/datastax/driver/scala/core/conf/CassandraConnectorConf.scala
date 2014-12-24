package com.datastax.driver.scala.core.conf

import java.net.InetAddress

import scala.collection.immutable
import scala.util.control.NonFatal
import com.datastax.driver.scala.core.utils.Logging
import com.datastax.driver.scala.core.{CassandraConnectionFactory, DefaultConnectionFactory}

/** Stores configuration of a connection to Cassandra.
  * Provides information about cluster nodes, ports and optional credentials for authentication. */
case class CassandraConnectorConf(
  hosts: Set[InetAddress],
  nativePort: Int = CassandraConnectorConf.DefaultNativePort,
  rpcPort: Int = CassandraConnectorConf.DefaultRpcPort,
  authConf: AuthConf = NoAuthConf,
  connectionFactory: CassandraConnectionFactory = DefaultConnectionFactory)

/** A factory for `CassandraConnectorConf` objects.
  * Allows for manually setting connection properties or reading them from `SparkConf` object.
  * By embedding connection information in `SparkConf`, `SparkContext` can offer Cassandra specific methods
  * which require establishing connections to a Cassandra cluster.*/
object CassandraConnectorConf extends Logging {
  import CassandraPropertyNames._

  def apply(host: InetAddress, nativePort: Int, rpcPort: Int, authConf: AuthConf = NoAuthConf): CassandraConnectorConf =
    CassandraConnectorConf(Set(host), nativePort, rpcPort, authConf)

  def apply(username: Option[String], password: Option[String], connectionFactoryFqcn: Option[String]): CassandraConnectorConf = {
    import settings._

    val hosts = resolveHosts(CassandraConnectionHosts)
    val rpcPort = CassandraConnectionRpcPort
    val nativePort = CassandraConnectionNativePort
    val authConf = AuthConf(username, password)
    val connectionFactory = CassandraConnectionFactory(connectionFactoryFqcn)
    CassandraConnectorConf(hosts, nativePort, rpcPort, authConf, connectionFactory)
  }

  def resolveHosts(hostNames: Set[String]): Set[InetAddress] =
    for {
      hostName <- hostNames
      hostAddress <- resolve(hostName)
    } yield hostAddress

  private def resolve(hostName: String): Option[InetAddress] =
    try Some(InetAddress.getByName(hostName)) catch {
      case NonFatal(e) =>
        logError(s"Unknown host '$hostName'", e)
        None
    }


  def hosts(hostString: Option[String]): immutable.Set[String] =
    withFallback[String](hostString, CassandraConnectionHostProperty,
      InetAddress.getLocalHost.getHostAddress).split(",").toSet[String]

  def rpcPort(port: Option[Int]): Int =
    withFallback[Int](port, CassandraConnectionRpcPortProperty, 9160)

  def nativePort(port: Option[Int]): Int =
    withFallback[Int](port, CassandraConnectionNativePortProperty, 9042)

  def userName(user: Option[String]): Option[String] =
    user orElse sys.props.get(CassandraUserNameProperty)

  def password(pass: Option[String]): Option[String] =
    pass orElse sys.props.get(CassandraPasswordProperty)

  def authConfFactoryFqcn(fqcn: Option[String]): Option[String] = sys.props.get(AuthConfFactoryProperty)

  def ConnectionFactoryFQCN: Option[String] = sys.props.get(ConnectionFactoryProperty)

  /** Attempts to acquire from java system properties, falls back to the provided default. */
  def withFallback[T](option: Option[T] = None, key: String, default: T): T =
    option getOrElse sys.props.get(key).map(_.asInstanceOf[T]).getOrElse(default)

}
