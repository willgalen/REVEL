package com.datastax.driver.scala

import scala.language.implicitConversions

import com.datastax.driver.scala.core.conf.CassandraSettings
import com.datastax.driver.scala.core.io.CassandraContext

/**
 * The root package of Cassandra connector.
 * Offers useful implicit conversions that add Cassandra-specific methods.
 */
package object core {

  final val DefaultSettings = CassandraSettings()

  implicit def cassandra(implicit settings: CassandraSettings = DefaultSettings): CassandraContext =
    new CassandraContext(settings)

  implicit def toCassandraContextFunctions[T](data: Iterable[T])(implicit settings: CassandraSettings = DefaultSettings): CassandraContextFunctions[T] =
    new CassandraContextFunctions[T](data)(settings)

  implicit def toNamedColumnRef(columnName: String): NamedColumnRef = ColumnName(columnName)

  implicit class ColumnNameFunctions(val columnName: String) extends AnyVal {
    def writeTime: WriteTime = WriteTime(columnName)
    def ttl: TTL = TTL(columnName)
  }

}
