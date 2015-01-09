package com.datastax.spark.connector.japi

import scala.reflect.runtime.universe._
import com.datastax.driver.scala.types.{TypeConverter, NullableTypeConverter}

final class UDTValue(val fieldNames: IndexedSeq[String], val fieldValues: IndexedSeq[AnyRef])
  extends JavaGettableData with Serializable

object UDTValue {

  val UDTValueTypeTag = implicitly[TypeTag[UDTValue]]

  implicit object UDTValueConverter extends NullableTypeConverter[UDTValue] {
    def targetTypeTag = UDTValueTypeTag

    def convertPF = {
      case x: UDTValue => x
      case x: UDTValue =>
        new UDTValue(x.fieldNames, x.fieldValues)
    }
  }

  TypeConverter.registerConverter(UDTValueConverter)

}