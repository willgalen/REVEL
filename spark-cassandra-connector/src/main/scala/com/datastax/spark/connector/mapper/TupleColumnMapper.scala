package com.datastax.spark.connector.mapper

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

import com.datastax.spark.connector.{ColumnName, NamedColumnRef, ColumnRef, ColumnIndex}
import com.datastax.spark.connector.cql.{RegularColumn, PartitionKeyColumn, ColumnDef, TableDef}
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util.Reflect

class TupleColumnMapper[T <: Product : TypeTag : ClassTag] extends ColumnMapper[T] {

  override val classTag: ClassTag[T] = implicitly[ClassTag[T]]

  private def indexedColumnRefs(n: Int) =
    (0 until n).map(ColumnIndex)

  override def columnMap(tableDef: TableDef, aliases: Map[String, String]): ColumnMap = {

    val GetterRegex = "_([0-9]+)".r
    val cls = implicitly[ClassTag[T]].runtimeClass

    val methodNames = cls.getMethods().map(_.getName)
    for ( (alias, name) <- aliases )
    {
      if (alias != name && !methodNames.contains(alias))
       throw new IllegalArgumentException(
         s"""Found Alias: $alias
           |Tuple provided does not have a getter for that alias.'""".stripMargin)
    }

    val constructor =
      indexedColumnRefs(cls.getConstructors()(0).getParameterTypes.length)

    val getters = if (aliases.isEmpty || aliases.forall{case (k,v) => k == v}) {
      for (name@GetterRegex(id) <- cls.getMethods.map(_.getName))
        yield (name, ColumnIndex(id.toInt - 1))
    }.toMap else {
      for (name@GetterRegex(id) <- cls.getMethods.map(_.getName) if aliases.contains(name))
        yield (name, ColumnName(aliases(name)))
    }.toMap

    val setters =
      Map.empty[String, ColumnRef]

    SimpleColumnMap(constructor, getters, setters)
  }

  override def newTable(keyspaceName: String, tableName: String): TableDef = {
    val tpe = implicitly[TypeTag[T]].tpe
    val ctorSymbol = Reflect.constructor(tpe).asMethod
    val ctorMethod = ctorSymbol.typeSignatureIn(tpe).asInstanceOf[MethodType]
    val ctorParamTypes = ctorMethod.params.map(_.typeSignature)
    require(ctorParamTypes.nonEmpty, "Expected a constructor with at least one parameter")

    val columnTypes = ctorParamTypes.map(ColumnType.fromScalaType)
    val columns = {
      for ((columnType, i) <- columnTypes.zipWithIndex) yield {
        val columnName = "_" + (i + 1).toString
        val columnRole = if (i == 0) PartitionKeyColumn else RegularColumn
        ColumnDef(columnName, columnRole, columnType)
      }
    }
    TableDef(keyspaceName, tableName, Seq(columns.head), Seq.empty, columns.tail)
  }
}
