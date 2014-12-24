package com.datastax.spark.connector

import java.net.InetAddress

import com.datastax.driver.scala.core.{CassandraConnectionFactory, CassandraConnector}
import com.datastax.driver.scala.core.conf.{AuthConf, CassandraConnectorConf}
import org.apache.spark.SparkConf
import com.datastax.driver.scala.core.conf._

/**
 * Provides and manages connections to Cassandra.
 * @see [[CassandraConnector]]
 */
class SparkCassandraConnector(conf: CassandraConnectorConf) extends CassandraConnector(conf)

object SparkCassandraConnector {
  import CassandraConnectorConf._
  import CassandraPropertyNames._

  private final val prefix = "spark."

  /** Returns a CassandraConnector created from properties found in the `SparkConf` object */
  def apply(conf: SparkConf): SparkCassandraConnector = {
    val user = conf.getOption(prefix + CassandraUserNameProperty) orElse sys.props.get(prefix + CassandraUserNameProperty)
    val pass = conf.getOption(prefix + CassandraPasswordProperty) orElse sys.props.get(prefix + CassandraPasswordProperty)
    apply(conf, AuthConf(user, pass))
  }


  def apply(conf: SparkConf): CassandraConnectorConf = {
    import settings._
    val cassandra = conf.getAll.filter(_._1.startsWith(prefix)).map { case (k,v) => (k.replace("spark.", ""),v)}

    val hostsStr = conf.get(CassandraConnectionHost, InetAddress.getLocalHost.getHostAddress)
    val hosts = for {
      hostName <- hostsStr.split(",").toSet[String]
      hostAddress <- resolveHost(hostName)
    } yield hostAddress

    val rpcPort = conf.getInt(CassandraConnectionRpcPortProperty, DefaultRpcPort)
    val nativePort = conf.getInt(CassandraConnectionNativePortProperty, DefaultNativePort)
    val authConf = AuthConf.fromSparkConf(conf)
    val connectionFactory = CassandraConnectionFactory.fromSparkConf(conf)
    CassandraConnectorConf(hosts, nativePort, rpcPort, authConf, connectionFactory)
  }

  /** Returns a CassandraConnector created from properties found in the `SparkConf`. */
  def apply(conf: SparkConf, auth: AuthConf): CassandraConnector = {
    val hostString = conf.get(prefix + CassandraConnectionHostProperty, InetAddress.getLocalHost.getHostAddress)
    val hosts = resolveHosts(hostString.split(",").toSet[String])
    val rpcPort = conf.getInt(prefix + CassandraConnectionRpcPortProperty, DefaultRpcPort)
    val nativePort = conf.getInt(prefix + CassandraConnectionNativePortProperty, DefaultNativePort)
    val connectionFactory = conf.getOption(prefix + ConnectionFactoryProperty)

    val config = CassandraConnectorConf(hosts, nativePort, rpcPort, connectionFactory, auth)
    new SparkCassandraConnector(config)
  }
}
