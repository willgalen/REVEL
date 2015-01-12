package com.datastax.driver.scala

import java.net.{InetSocketAddress, InetAddress}

import akka.actor._
import com.datastax.driver.core.{Configuration, KeyspaceMetadata, Metadata, Host}
import com.datastax.driver.scala.core.CassandraCluster
import com.datastax.driver.scala.core.conf.ReadConf
import com.datastax.driver.scala.core.io.{RowReaderFactory, TableReader, CassandraContext}
import com.datastax.driver.scala.core.utils.Logging
import com.datastax.driver.scala.types.ValidSourceType

import scala.reflect.ClassTag

/**
 * An Akka Extension for Cassandra.
 * Reading from a table:
 * {{{
 *   val system = ActorSystem("Cassandra")
 *   val cassandra = CassandraExtension(system)
 *
 *   val stream: Stream[CassandraRow] = cassandra.table("keyspace", "table")
 *
 *   val stream: Stream[MyCaseClass] = cassandra.table[MyCaseClass]("keyspace", "table")
 *
 *   val stream: Stream[MyCaseClass] = cassandra.table[MyCaseClass]("keyspace", "table").select(...).where(...).stream
 *
 * }}}
 *
 * Writing data:
 * {{{
 *   import com.datastax.driver.scala.core._
 *
 *   Seq((4, "fourth row"), (5, "fifth row")).write(data, "test", "key_value")
 *
 * }}}
 *
 * You can also do things like get the cassandra cluster topology
 * {{{
 *   CassandraExtension(system).clusterHosts
 * }}}
 *
 * Or you can get the closest live host
 * {{{
 *   val proximalHost: Host = CassandraExtension(system).closestLiveHost
 * }}}
 *
 */
object CassandraExtension extends ExtensionId[CassandraExtension] with ExtensionIdProvider {

  override def get(system: ActorSystem): CassandraExtension = super.get(system)

  override def lookup = CassandraExtension

  override def createExtension(system: ExtendedActorSystem): CassandraExtension = new CassandraExtension(system)

}

final class CassandraExtension private(val system: ExtendedActorSystem) extends Extension with Logging {


  val context = CassandraContext()
  import context._

  val config = context.config

  private def clusterMetadata: Metadata = context.clusterMetadata

  def execute(cql: String)(implicit cluster: CassandraCluster = CassandraCluster(config)): Unit =
    context execute cql

  def execute(cql: Seq[String])(implicit cluster: CassandraCluster = CassandraCluster(config)): Unit =
    context execute cql

  import com.datastax.driver.scala.core._

  def stream[T](keyspaceName: String, tableName: String, readConf: ReadConf = DefaultReadConf): Stream[T] = {
    table[T](keyspaceName, tableName).stream
  }

  /**
   * If [T] is not specified, returns a [[com.datastax.driver.scala.core.io.TableReader]]
   * for iterating through [[CassandraRow]].
   *
   * {{{table("", "").select("columnName1", "columnName2").where("x = ?", value").stream}}}
   */
  def table[T](keyspaceName: String, tableName: String, readConf: ReadConf = DefaultReadConf)
              (implicit ct: ClassTag[T], rrf: RowReaderFactory[T], ev: ValidSourceType[T]): TableReader[T] = {
    cassandra.table[T](keyspaceName, tableName, readConf)
  }
/* def table[T](keyspace: String, table: String, readConf: ReadConf = DefaultReadConf)
              (implicit ct: ClassTag[T], rrf: RowReaderFactory[T], ev: ValidSourceType[T]): TableReader[T] = {
    TableReader[T](config, keyspace, table, readConf)
  }
*/
  def clusterConfiguration: Configuration = context.clusterConfiguration

  /** Returns a current set of known cluster hosts. */
  def clusterHosts: Set[Host] = context.clusterHosts

  private[datastax] def remotes: Set[InetSocketAddress] =
    clusterHosts.map(a => new InetSocketAddress(a.getAddress, config.nativePort))

  def keyspace(name: String): KeyspaceMetadata = context.keyspace(name)

  /** Finds the DCs of the contact points and returns hosts in those DC(s) from `clusterHosts`.
    * Guarantees to return at least the hosts pointed by `contactPoints`, even if their
    * DC information is missing. Other hosts with missing DC information are not considered. */
  def nodesInSameDC(contactPoints: Set[InetAddress] = config.hosts): Set[InetAddress] =
    context.nodesInSameDC(contactPoints)

  /** Returns the local node, if it is one of the cluster nodes. Otherwise returns any node.
    * Sorts nodes in the following order:
    * 1. local host
    * 2. live nodes in the same DC as `contactPoints`
    * 3. down nodes in the same DC as `contactPoints`
    *
    * Nodes within a group are ordered randomly.
    * Nodes from other DCs are not included.*/
  def closestLiveHost: Host = context.closestLiveHost

  system.registerOnTermination { context.shutdown() }

}
