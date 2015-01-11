package com.datastax.driver.scala.core.conf

import java.net.{InetAddress, UnknownHostException}

import scala.util.control.NonFatal
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.scala.core.conf.Configuration._
import com.datastax.driver.scala.core.utils.Logging

/**
 * The Cassandra settings. Attempts to acquire from optional overrides from an alternate source,
 * or from the environment and finally from system properties. If a property is
 * not available from any of those resources, falls back to the provided default.
 *
 * This class allows property name namespace filtering and conversion,
 * i.e. `spark.cassandra.Cluster.host` to `cassandra.Cluster.host`,
 * and is [[Serializable]].
 *
 * Implicit fallback chain of settings:
 * 1. Alternate source if provided, defaults to [[Source.None]]
 * 2. Any settings in the environment
 * 3. Any settings in java system properties
 * 4. Default Settings
 *
 * The connection options are:
 * - `cassandra.Cluster.host`:               The contact point to connect to the Cassandra cluster
 * - `cassandra.Cluster.rpc.port`:           Cassandra thrift port, defaults to 9160
 * - `cassandra.Cluster.native.port`:        Cassandra native port, defaults to 9042
 * - `cassandra.Cluster.factory`:           The name of a Scala module or class implementing [[com.datastax.driver.scala.core.CassandraConnectionFactory]]
 * that allows to plugin custom code for connecting to Cassandra
 * - `cassandra.auth.username`:                 The login for password authentication
 * - `cassandra.auth.password`:                 The password for password authentication
 * - `cassandra.auth.conf.factory`:             The name of a Scala module or class implementing [[AuthConf]] that allows to plugin custom authentication configuration
 *
 * Additionally this object uses the following System properties:
 * - `cassandra.Cluster.keep_alive_ms`: the number of milliseconds to keep unused `Cluster` object before destroying it (default 100 ms)
 * - `cassandra.Cluster.reconnection_delay_ms.min`: initial delay determining how often to try to reconnect to a dead node (default 1 s)
 * - `cassandra.Cluster.reconnection_delay_ms.max`: final delay determining how often to try to reconnect to a dead node (default 60 s)
 * - `cassandra.query.retry.count`: how many times to reattempt a failed query
 *
 * - `cassandra.input.split.size`:        approx number of Cassandra partitions
 * - `cassandra.input.page.row.size`:     number of CQL rows fetched per roundtrip, default 1000
 *
 * @param alternate optional settings from an outside source
 *
 */
final class CassandraSettings private(val alternate: Source, prefix: Option[String], useDefaults: Boolean = false) extends FallbackSettings {

  protected def get(key: String): Option[String] = {
    if (useDefaults) None
    else {
      val k = prefix.map(_ + key) getOrElse key
      alternate.get(k) orElse sys.env.get(k) orElse sys.props.toMap.get(k)
    }
  }

  val CassandraHosts: Set[InetAddress] =
    get(Cluster.HostProperty).getOrElse(InetAddress.getLocalHost.getHostAddress)
     .split(",").toSet[String].map(InetAddressFromString(_))
  val NativePort: Int = toInt(Cluster.PortProperty, Cluster.DefaultNativePort)
  val RpcPort: Int = toInt(Cluster.RpcPortProperty, Cluster.DefaultRpcPort)
  val ConnectionFactoryFqcn: Option[String] = get(Cluster.FactoryFqcnProperty)
  val AuthUserName: Option[String] = get(Cluster.AuthUserNameProperty)
  val AuthPassword: Option[String] = get(Cluster.AuthPasswordProperty)
  val AuthConfFqcn: Option[String] = get(Cluster.AuthConfFqcnProperty)

  val ClusterReconnectDelayMin: Int = toInt(Cluster.ReconnectDelayMinProperty, Cluster.DefaultReconnectDelayMin)
  val ClusterReconnectDelayMax: Int = toInt(Cluster.ReconnectDelayMaxProperty, Cluster.DefaultReconnectDelayMax)
  val ClusterLocalDc: Option[String] =
    get(Cluster.LocalDcProperty)
  val ClusterQueryRetries: Int = toInt(Cluster.QueryRetryCountMillisProperty, Cluster.DefaultQueryRetryCountMillis)
  val ClusterTimeout: Int = toInt(Cluster.TimeoutMillisProperty, Cluster.DefaultTimeoutMillis)
  val ClusterReadTimeout: Int = toInt(Cluster.ReadTimeoutMillisProperty, Cluster.DefaultReadTimeoutMillis)

  val WriteBatchSizeBytes: Int = toInt(Write.BatchSizeInBytesProperty,Write.DefaultBatchSizeInBytes)
  val WriteBatchSizeRows: String = get(Write.BatchSizeInRowsProperty) getOrElse Write.DefaultBatchSizeRows
  val WriteConsistencyLevel: ConsistencyLevel = toConsistencyLevel(Write.ConsistencyLevelProperty, Write.DefaultConsistencyLevel)
  val WriteParallelismLevel: Int = toInt(Write.ParallelismLevelProperty, Write.DefaultParallelismLevel)

  val ReadSplitSize: Int = toInt(Read.SplitSizeProperty, Read.DefaultSplitSize)
  val ReadFetchSize: Int = toInt(Read.FetchSizeProperty, Read.DefaultFetchSize)
  val ReadConsistencyLevel: ConsistencyLevel = toConsistencyLevel(Read.ConsistencyLevelProperty, Read.DefaultConsistencyLevel)

  private def toInt(key: String, default: Int):Int = get(key).map(_.toInt) getOrElse default
  private def toConsistencyLevel(key: String, default: ConsistencyLevel): ConsistencyLevel =
    ConsistencyLevel.valueOf(get(key) getOrElse default.name())
}

object CassandraSettings {
  import com.typesafe.config.Config
  import scala.collection.JavaConverters._

  /** Creates a new instance of CassandraSettings using the default fallback
    * chain and `cassandra.*` namespace for property names. */
  def apply(useDefaults: Boolean = false): CassandraSettings = {
    new CassandraSettings(Source.None, None, useDefaults)
  }

  def apply(config: Config): CassandraSettings = {
    apply(config.root.unwrapped.asScala.map { case (k, v) => k -> v.toString}.toMap)
  }

  def apply(source: Map[String, String]): CassandraSettings = {
    apply(Source(source), None)
  }

  def apply(source: Source, prefix: Option[String]): CassandraSettings = {
    new CassandraSettings(source, prefix)
  }
}

object Configuration {

  @SerialVersionUID(1L)
  sealed trait Settings extends Serializable
  object InetAddressFromString extends Logging {

    def unapply(host: String): Option[InetAddress] =
      try Some(InetAddress.getByName(host)) catch {
        case NonFatal(e) => logError(s"Unknown host '$host'", e); None
      }

    def unapply(hosts: Set[String]): Set[InetAddress] =
      for (hostName <- hosts; address <- unapply(hostName)) yield address

    /** Try to construct CassandraHosts from the given String or throw a java.net.UnknownHostException. */
    def apply(hostString: String): InetAddress = hostString match {
      case InetAddressFromString(host) => host
      case _ => throw new UnknownHostException(s"Unknown host '$hostString'")
    }
  }

  @SerialVersionUID(1L)
  sealed trait FilteredSettings extends Settings {
    def get(key: String): Option[String] = values.get(key)

    def values: Map[String, String]
  }

  /** Settings from a source that may or may not be set in that source. */
  trait OptionalSettings extends FilteredSettings

  /** To Accept settings from another source before environment then system properties are
    * checked for that key-value, such as a `SparkConf`, where conf.getAll.toMap.filterKeys...
    * can filter settings for *.cassandra.* and strip them of the prefix "spark.".
    */
  case class Source(values: Map[String, String]) extends OptionalSettings

  object Source {
    val None = Source(Map.empty[String, String])
  }

  trait FallbackSettings extends Settings {
    def alternate: Source

    protected def get(key: String): Option[String]
  }

}

/* Cluster Policies & Connection Tuning */
object Cluster {
  /* Connection */
  val HostProperty = "cassandra.Cluster.host"
  val RpcPortProperty = "cassandra.Cluster.rpc.port"
  val PortProperty = "cassandra.Cluster.native.port"
  val FactoryFqcnProperty = "cassandra.Cluster.factory"
  val AuthUserNameProperty = "cassandra.auth.username"
  val AuthPasswordProperty = "cassandra.auth.password"
  val AuthConfFqcnProperty = "cassandra.auth.conf.factory"
  val KeepAliveMillisProperty = "cassandra.Cluster.keep_alive_ms"

  val DefaultRpcPort = 9160
  val DefaultNativePort = 9042

  /* Cluster */
  val ReconnectDelayMinProperty = "cassandra.Cluster.reconnection_delay_ms.min"
  val ReconnectDelayMaxProperty = "cassandra.Cluster.reconnection_delay_ms.max"
  val LocalDcProperty = "cassandra.Cluster.local_dc"
  val TimeoutMillisProperty = "cassandra.Cluster.timeout_ms"
  val ReadTimeoutMillisProperty = "cassandra.read.timeout_ms"
  val QueryRetryCountMillisProperty = "cassandra.query.retry.count"

  val DefaultReconnectDelayMin = 1000
  val DefaultReconnectDelayMax = 60000
  val DefaultQueryRetryCountMillis = 10
  val DefaultTimeoutMillis = 5000
  val DefaultReadTimeoutMillis = 12000
}

/* Write Tuning */
object Write {
  val BatchSizeInBytesProperty = "cassandra.output.batch.size.bytes"
  val ConsistencyLevelProperty = "cassandra.output.consistency.level"
  val BatchSizeInRowsProperty = "cassandra.output.batch.size.rows"
  val ParallelismLevelProperty = "cassandra.output.concurrent.writes"

  val DefaultBatchSizeInBytes = 16 * 1024
  val DefaultBatchSizeRows = "auto"
  val DefaultConsistencyLevel = ConsistencyLevel.LOCAL_ONE
  val DefaultParallelismLevel = 8
}

/* Read Tuning */
object Read {
  val FetchSizeProperty = "cassandra.input.page.row.size"
  val SplitSizeProperty = "cassandra.input.split.size"
  val ConsistencyLevelProperty = "cassandra.input.consistency.level"

  val DefaultSplitSize = 100000
  val DefaultFetchSize = 1000
  val DefaultConsistencyLevel = ConsistencyLevel.LOCAL_ONE
}