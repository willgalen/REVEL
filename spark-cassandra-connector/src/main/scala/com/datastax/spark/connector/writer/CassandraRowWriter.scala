package com.datastax.spark.connector.writer

import com.datastax.spark.connector.{ColumnRef, CassandraRow}
import com.datastax.spark.connector.cql.TableDef

/** A [[RowWriter]] that can write [[CassandraRow]] objects.*/
class CassandraRowWriter(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]) extends RowWriter[CassandraRow] {

  val columnNames = selectedColumns.map(_.columnName)

  override def readColumnValues(data: CassandraRow, buffer: Array[Any]) = {
    for ((c, i) <- columnNames.zipWithIndex)
      buffer(i) = data.getRaw(c)
  }
}


object CassandraRowWriter {

  object Factory extends RowWriterFactory[CassandraRow] {
    override def rowWriter(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]) =
      new CassandraRowWriter(table, selectedColumns)
  }

}
