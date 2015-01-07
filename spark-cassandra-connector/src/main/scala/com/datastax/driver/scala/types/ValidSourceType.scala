package com.datastax.driver.scala.types

import java.io.{Serializable => JavaSerializable}

import scala.annotation.implicitNotFound

@implicitNotFound("Not a valid source type. There should exists either a type converter for the type or the type should implement Serializable")
trait ValidSourceType[T]

object ValidSourceType {
  implicit def withTypeConverterAsValidRDDType[T](implicit tc: TypeConverter[T]): ValidSourceType[T] = null

  implicit def javaSerializableAsValidRDDType[T <: JavaSerializable]: ValidSourceType[T] = null
}