package com.datastax.driver.scala.core.io

import com.datastax.driver.scala.core.{CassandraRow, TableDef}
import com.datastax.driver.scala.mapping
import com.datastax.driver.scala.mapping.ColumnMapper

/** Creates instances of [[RowWriter]] objects for the given row type `T`.
  * `RowWriterFactory` is the trait you need to implement if you want to support row representations
  * which cannot be simply mapped by a [[mapping.ColumnMapper ColumnMapper]].*/
trait RowWriterFactory[T] {

  /** Creates a new `RowWriter` instance.
    * @param table target table the user wants to write into
    * @param columnNames columns selected by the user; the user might wish to write only a subset of columns */
  def rowWriter(table: TableDef, columnNames: Seq[String]): RowWriter[T]
}

/** Provides a low-priority implicit `RowWriterFactory` able to write objects of any class for which
  * a [[mapping.ColumnMapper ColumnMapper]] is defined.*/
trait LowPriorityRowWriterFactoryImplicits {
  implicit def defaultRowWriterFactory[T : ColumnMapper]: RowWriterFactory[T] = DefaultRowWriter.factory
}

/** Provides an implicit `RowWriterFactory` for saving [[CassandraRow CassandraRow]] objects.*/
object RowWriterFactory extends LowPriorityRowWriterFactoryImplicits {
  implicit val genericRowWriterFactory = CassandraRowWriter.Factory
}