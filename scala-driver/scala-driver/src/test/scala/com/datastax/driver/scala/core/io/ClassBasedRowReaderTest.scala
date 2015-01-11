package com.datastax.driver.scala.core.io

import com.datastax.driver.scala.core.TableDef
import org.apache.commons.lang3.SerializationUtils
import org.junit.Test

case class TestClass(a: String, b: Int, c: Option[Long])

class ClassBasedRowReaderTest {

  private val tableDef = TableDef("test", "table", Nil, Nil, Nil)

  @Test
  def testSerialize() {
    val reader = new ClassBasedRowReader[TestClass](tableDef)
    SerializationUtils.roundtrip(reader)
  }

}
