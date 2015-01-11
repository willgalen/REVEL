package com.datastax.spark.connector.util

import java.util.{Map => JavaMap}

import scala.reflect.runtime.universe._

import scala.collection.JavaConversions._
import scala.reflect._
import scala.reflect.api.{Mirror, TypeCreator, _}
import com.datastax.driver.scala.core.CassandraRow
import com.datastax.driver.scala.core.io.{RowReaderFactory, RowWriterFactory}
import com.datastax.driver.scala.mapping.ColumnMapper
import com.datastax.spark.connector.mapper.JavaBeanColumnMapper
import com.datastax.driver.scala.core.utils.Reflection

/** A helper class to make it possible to access components written in Scala from Java code. */
object JavaApiHelper {

  def mirror = Reflection.mirror

  /** Returns a `TypeTag` for the given class. */
  def getTypeTag[T](clazz: Class[T]): TypeTag[T] = TypeTag.synchronized {
    TypeTag.apply(mirror, new TypeCreator {
      override def apply[U <: Universe with Singleton](m: Mirror[U]): U#Type = {
        m.staticClass(clazz.getName).toTypeConstructor
      }
    })
  }

  /** Returns a `TypeTag` for the given class and type parameters. */
  def getTypeTag[T](clazz: Class[_], typeParams: TypeTag[_]*): TypeTag[T] = TypeTag.synchronized {
    TypeTag.apply(mirror, new TypeCreator {
      override def apply[U <: Universe with Singleton](m: Mirror[U]) = {
        val ct = m.staticClass(clazz.getName).toTypeConstructor.asInstanceOf[m.universe.Type]
        val tpt = typeParams.map(_.in(m).tpe.asInstanceOf[m.universe.Type]).toList
        m.universe.appliedType(ct, tpt).asInstanceOf[U#Type]
      }
    })
  }

  /** Returns a `ClassTag` of a given runtime class. */
  def getClassTag[T](clazz: Class[T]): ClassTag[T] = ClassTag(clazz)

  /** Returns a runtime class of a given `TypeTag`. */
  def getRuntimeClass[T](typeTag: TypeTag[T]): Class[T] = Reflection.getRuntimeClass[T](typeTag)

  /** Returns a runtime class of a given `ClassTag`. */
  def getRuntimeClass[T](classTag: ClassTag[T]): Class[T] = classTag.runtimeClass.asInstanceOf[Class[T]]

  /** Converts a Java `Map` to a Scala immutable `Map`. */
  def toScalaMap[K, V](map: JavaMap[K, V]): Map[K, V] = Map(map.toSeq: _*)

  /** Converts an array to a Scala `Seq`. */
  def toScalaSeq[T](array: Array[T]): Seq[T] = akka.japi.Util.immutableSeq(array)

  /** Converts a Java `Iterable` to Scala `Seq`. */
  def toScalaSeq[T](iterable: java.lang.Iterable[T]): Seq[T] = akka.japi.Util.immutableSeq(iterable)

  /** Returns the default `RowWriterFactory` initialized with the given `ColumnMapper`. */
  def defaultRowWriterFactory[T](mapper: ColumnMapper[T]) = {
    RowWriterFactory.defaultRowWriterFactory(mapper)
  }

  /** Returns the `JavaBeanColumnMapper` instance for the given `ClassTag` and column mapping. */
  def javaBeanColumnMapper[T](classTag: ClassTag[T], columnNameOverride: JavaMap[String, String]): ColumnMapper[T] =
    new JavaBeanColumnMapper[T](toScalaMap(columnNameOverride))(classTag)

  /** Returns the default `RowReaderFactory`. */
  def genericRowReaderFactory: RowReaderFactory[CassandraRow] = RowReaderFactory.GenericRowReader$

}
