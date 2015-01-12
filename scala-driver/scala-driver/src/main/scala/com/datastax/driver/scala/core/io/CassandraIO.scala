package com.datastax.driver.scala.core.io

import java.io.{Serializable => JSerializable}
import java.net.InetAddress

import com.datastax.driver.scala.core.policies.LocalNodeFirstLoadBalancingPolicy
import com.datastax.driver.scala.core.utils.Logging

import scala.collection.JavaConversions._
import com.datastax.driver.core.{KeyspaceMetadata, Host, Configuration, Metadata}
import com.datastax.driver.scala.core.conf._
import com.datastax.driver.scala.core.{CassandraCluster, AllColumns, ColumnSelector}
import com.datastax.driver.scala.types.ValidSourceType

import scala.reflect.ClassTag

/**
 * {{{
 *   case class SearchStatistics(keyword: String, hits: Int)
 *   val keyspace = "searches"
 *   val table = "animal_searches"
 *
 *   import com.datastax.driver.scala.core._
 *   implicit val settings = CassandraSettings() // or CassandraSettings(myvalues: Map[String,String])
 *   val cassandra = CassandraContext(settings)
 *
 *   // Write one row:
 *   Set(SearchStatistics("dolphin", 5000)).write(keyspace, table)
 *
 *   // Write three rows to the search.searches table:
 *   val searches = Seq(("cat", 1000), ("dog", 600), ("penguin", 2000))
 *   searches.write(keyspace, table)
 *
 *   // read specific table columns
 *   cassandra.table[SearchStatistics](keyspace, table)
 *     .select("keyword", "count")
 *     .stream foreach println
 *
 *   // read all table columns
 *   val results = cassandra.table("search", "searches_table")
 *                   .where("keyword = ?", "cat")
 *                   .stream.map(Statistics)
 * }}}
 * java.io.Serializable is needed if the Source is going to have any
 * methods attached that compute remotely.
 */
abstract class CassandraIO(settings: CassandraSettings) extends JSerializable {

  implicit val config = ClusterConfig(settings)

  final val DefaultReadConf = ReadConf(settings)

  final val DefaultWriteConf = WriteConf(settings)

  def table[T](keyspace: String, table: String, readConf: ReadConf = DefaultReadConf)
              (implicit ct: ClassTag[T], rrf: RowReaderFactory[T], ev: ValidSourceType[T]): TableReader[T] = {
    TableReader[T](config, keyspace, table, readConf)
  }

  def write[T](data: Iterable[T], keyspace: String, table: String,
               columns: ColumnSelector = AllColumns,
               writeConf: WriteConf = DefaultWriteConf) (implicit rwf: RowWriterFactory[T]) = {
    val writer = TableWriter[T](CassandraCluster(config), keyspace, table, columns, writeConf)
    writer.write(data.iterator)
  }

}

class CassandraContext(settings: CassandraSettings) extends CassandraIO(settings) with Logging {

  private[datastax] def clusterMetadata: Metadata = CassandraCluster(config).metadata

  def clusterConfiguration: Configuration = CassandraCluster(config).configuration

  /** Returns a current set of known cluster hosts. */
  def clusterHosts: Set[Host] = clusterMetadata.getAllHosts.toSet

  def keyspace(name: String): KeyspaceMetadata =
    clusterMetadata.getKeyspace(name)

  /** Finds the DCs of the contact points and returns hosts in those DC(s) from `clusterHosts`.
    * Guarantees to return at least the hosts pointed by `contactPoints`, even if their
    * DC information is missing. Other hosts with missing DC information are not considered. */
  def nodesInSameDC(contactPoints: Set[InetAddress] = config.hosts): Set[InetAddress] =
    LocalNodeFirstLoadBalancingPolicy.nodesInTheSameDC(contactPoints, clusterHosts).map(_.getAddress)

  /** Returns the local node, if it is one of the cluster nodes. Otherwise returns any node.
    * Sorts nodes in the following order:
    * 1. local host
    * 2. live nodes in the same DC as `contactPoints`
    * 3. down nodes in the same DC as `contactPoints`
    *
    * Nodes within a group are ordered randomly.
    * Nodes from other DCs are not included.*/
  def closestLiveHost: Host = CassandraCluster(config).closestLiveHost

  def execute(cql: String*)(implicit cluster: CassandraCluster = CassandraCluster(config)): Unit =
    cluster.withSessionDo { session => cql.foreach(session.execute) }

  private[datastax] def shutdown(): Unit = {
    log.info(s"Cassandra shutting down.")
    CassandraCluster.evictCache()
    CassandraCluster.sessionCache.shutdown()
  }

}

object CassandraContext {
  import com.datastax.driver.scala.core._
  def apply(): CassandraContext = new CassandraContext(DefaultSettings)

}