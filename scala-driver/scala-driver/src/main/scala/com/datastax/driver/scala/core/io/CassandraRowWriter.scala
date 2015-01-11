package com.datastax.driver.scala.core.io

import com.datastax.driver.scala.core.{CassandraRow, TableDef}

/** A [[com.datastax.driver.scala.core.io.RowWriter]] that can write
  * [[com.datastax.driver.scala.core.CassandraRow]] objects.*/
class CassandraRowWriter(table: TableDef, selectedColumns: Seq[String]) extends RowWriter[CassandraRow] {

  val columnNames = selectedColumns

  override def readColumnValues(data: CassandraRow, buffer: Array[Any]) = {
    for ((c, i) <- columnNames.zipWithIndex)
      buffer(i) = data.getRaw(c)
  }
}


object CassandraRowWriter {

  object Factory extends RowWriterFactory[CassandraRow] {
    override def rowWriter(table: TableDef, columnNames: Seq[String]) =
      new CassandraRowWriter(table, columnNames)
  }

}
