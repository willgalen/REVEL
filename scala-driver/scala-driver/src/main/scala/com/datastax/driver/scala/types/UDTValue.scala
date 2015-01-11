package com.datastax.driver.scala.types

import com.datastax.driver.core.{ProtocolVersion, UDTValue => DriverUDTValue}
import com.datastax.driver.scala.core.{AbstractGettableData, ScalaGettableData}

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

final class UDTValue(val fieldNames: IndexedSeq[String], val fieldValues: IndexedSeq[AnyRef])
  extends ScalaGettableData with Serializable

object UDTValue {

  def apply(value: DriverUDTValue)(implicit protocolVersion: ProtocolVersion): UDTValue = {
    val fields = value.getType.getFieldNames.toIndexedSeq
    val values = fields.map(AbstractGettableData.get(value, _))
    new UDTValue(fields, values)
  }

  def apply(map: Map[String, Any]): UDTValue =
    new UDTValue(map.keys.toIndexedSeq, map.values.map(_.asInstanceOf[AnyRef]).toIndexedSeq)

  val UDTValueTypeTag = implicitly[TypeTag[UDTValue]]

  implicit object UDTValueConverter extends NullableTypeConverter[UDTValue] {
    def targetTypeTag = UDTValueTypeTag
    def convertPF = {
      case x: UDTValue => x
    }
  }
}
