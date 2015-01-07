package com.datastax.driver.scala

import scala.language.implicitConversions

import com.datastax.driver.scala.core.conf.CassandraSettings
import com.datastax.driver.scala.core.io.CassandraContext

/**
 * The root package of Cassandra connector.
 * Offers useful implicit conversions that add Cassandra-specific methods.
 */
package object core {

  implicit def cassandra(implicit settings: CassandraSettings): CassandraContext =
    new CassandraContext(settings)

  implicit def toCassandraSourceFunctions[T](data: Iterable[T])(implicit settings: CassandraSettings): CassandraSourceFunctions[T] =
    new CassandraSourceFunctions[T](data)(settings)

  implicit def toNamedColumnRef(columnName: String): NamedColumnRef = ColumnName(columnName)

  implicit class ColumnNameFunctions(val columnName: String) extends AnyVal {
    def writeTime: WriteTime = WriteTime(columnName)
    def ttl: TTL = TTL(columnName)
  }

}
