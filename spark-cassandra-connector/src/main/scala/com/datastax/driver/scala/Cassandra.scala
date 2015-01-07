package com.datastax.driver.scala

import java.net.InetAddress

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._
import akka.actor._
import com.datastax.driver.core.{KeyspaceMetadata, Metadata, Host}
import com.datastax.driver.scala.core.Connector
import com.datastax.driver.scala.core.conf.{CassandraConnectorConf, CassandraSettings}
import com.datastax.driver.scala.core.io.CassandraContext
import com.datastax.driver.scala.core.policies.LocalNodeFirstLoadBalancingPolicy
import com.datastax.driver.scala.core.utils.Logging

/**
 * An Akka Extension for Cassandra.
 */
object Cassandra extends ExtensionId[Cassandra] with ExtensionIdProvider {

  override def get(system: ActorSystem): Cassandra = super.get(system)

  override def lookup = Cassandra

  override def createExtension(system: ExtendedActorSystem): Cassandra = new Cassandra(system)

}

class Cassandra private(val system: ExtendedActorSystem) extends Extension with Logging {

  final val settings = CassandraSettings()

  private val conf = CassandraConnectorConf(settings)

  private def clusterMetadata: Metadata = Connector(conf).withClusterDo(_.getMetadata)

  /** Known cluster hosts. */
  def clusterHosts: Set[Host] = clusterMetadata.getAllHosts.toSet

  def keyspace(name: String): Option[KeyspaceMetadata] = Option(name) map {
    keyspace => clusterMetadata.getKeyspace(name)
    log.error("'name' for keyspace must be set.")
    clusterMetadata.getKeyspace(name)
  }

  /** Finds the DCs of the contact points and returns hosts in those DC(s) from `clusterHosts`.
    * Guarantees to return at least the hosts pointed by `contactPoints`, even if their
    * DC information is missing. Other hosts with missing DC information are not considered. */
  def nodes(contactPoints: Set[InetAddress] = conf.hosts): Set[InetAddress] =
    LocalNodeFirstLoadBalancingPolicy
      .nodesInTheSameDC(contactPoints, clusterHosts).map(_.getAddress)

  final val context = new CassandraContext(settings)

  system.registerOnTermination { shutdown() }

  def execute(cql: String)(implicit conn: Connector = Connector(conf)): Unit = {
    conn.withSessionDo(_.execute(cql))
  }

  def execute(cql: Seq[String]): Unit = {
    val conn = Connector(conf)
    conn.withSessionDo { session => cql.foreach(execute) }
  }

  def context(keyspace: String, table: String): CassandraContext = context

  private[datastax] def shutdown(): Unit = {
    log.info(s"Cassandra connector shutting down.")
  }
}