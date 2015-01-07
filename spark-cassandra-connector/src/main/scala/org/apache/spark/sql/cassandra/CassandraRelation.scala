package org.apache.spark.sql.cassandra

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst
import com.datastax.driver.scala.core.{TableDef, ColumnDef}
import com.datastax.driver

private[cassandra] case class CassandraRelation
  (tableDef: TableDef, alias: Option[String])(@transient cc: CassandraSQLContext)
  extends LeafNode {

  val keyspaceName          = tableDef.keyspaceName
  val regularColumns        = tableDef.regularColumns.toList.map(columnToAttribute)
  val indexedColumns        = tableDef.regularColumns.filter(_.isIndexedColumn).map(columnToAttribute)
  val partitionColumns      = tableDef.partitionKey.map(columnToAttribute)
  val clusterColumns        = tableDef.clusteringColumns.map(columnToAttribute)
  val allColumns            = tableDef.regularColumns ++ tableDef.partitionKey ++ tableDef.clusteringColumns
  val columnNameByLowercase = allColumns.map(c => (c.columnName.toLowerCase, c.columnName)).toMap
  var projectAttributes     = tableDef.allColumns.map(columnToAttribute)

  def columnToAttribute(column: ColumnDef): AttributeReference = {
    // Since data can be dumped in randomly with no validation, everything is nullable.
    val catalystType = ColumnDataType.catalystDataType(column.columnType, nullable = true)
    val qualifiers = tableDef.tableName +: alias.toSeq
    new AttributeReference(column.columnName, catalystType, nullable = true)(qualifiers = qualifiers)
  }

  override def output: Seq[Attribute] = projectAttributes

  @transient override lazy val statistics = Statistics(
    sizeInBytes = {
      BigInt(cc.conf.getLong(keyspaceName + "." + tableName + ".size.in.bytes", cc.defaultSizeInBytes))
    }
  )
  def tableName = tableDef.tableName
}

object ColumnDataType {

  private val primitiveTypeMap = Map[driver.scala.types.ColumnType[_], catalyst.types.DataType](

    driver.scala.types.TextType       -> catalyst.types.StringType,
    driver.scala.types.AsciiType      -> catalyst.types.StringType,
    driver.scala.types.VarCharType    -> catalyst.types.StringType,

    driver.scala.types.BooleanType    -> catalyst.types.BooleanType,

    driver.scala.types.IntType        -> catalyst.types.IntegerType,
    driver.scala.types.BigIntType     -> catalyst.types.LongType,
    driver.scala.types.CounterType    -> catalyst.types.LongType,
    driver.scala.types.FloatType      -> catalyst.types.FloatType,
    driver.scala.types.DoubleType     -> catalyst.types.DoubleType,

    driver.scala.types.VarIntType     -> catalyst.types.DecimalType, // no native arbitrary-size integer type
    driver.scala.types.DecimalType    -> catalyst.types.DecimalType,

    driver.scala.types.TimestampType  -> catalyst.types.TimestampType,
    driver.scala.types.InetType       -> catalyst.types.StringType,
    driver.scala.types.UUIDType       -> catalyst.types.StringType,
    driver.scala.types.TimeUUIDType   -> catalyst.types.StringType,
    driver.scala.types.BlobType       -> catalyst.types.ByteType,
  
    // TODO: This mapping is useless, it is here only to avoid lookup failure if a table contains a UDT column. 
    // It is not possible to read UDT columns in SparkSQL now. 
    driver.scala.types.UserDefinedTypeStub -> catalyst.types.StructType(Seq.empty)
  )

  def catalystDataType(cassandraType: com.datastax.driver.scala.types.ColumnType[_], nullable: Boolean): catalyst.types.DataType = {
    cassandraType match {
      case com.datastax.driver.scala.types.SetType(et)      => catalyst.types.ArrayType(primitiveTypeMap(et), nullable)
      case com.datastax.driver.scala.types.ListType(et)     => catalyst.types.ArrayType(primitiveTypeMap(et), nullable)
      case com.datastax.driver.scala.types.MapType(kt, vt)  => catalyst.types.MapType(primitiveTypeMap(kt), primitiveTypeMap(vt), nullable)
      case _                                => primitiveTypeMap(cassandraType)
    }
  }
}
