package com.datastax.driver.scala.core.utils

import com.datastax.driver.scala.core.{SimpleConnectionFactory, CassandraConnectionFactory}
import org.scalatest.{FlatSpec, Matchers}
import com.datastax.driver.scala.core.conf.ClusterConfig

class ReflectionUtilSpec extends FlatSpec with Matchers {

  "ReflectionUtil.findGlobalObject" should "be able to find SimpleConnectionFactory" in {
    val factory = Reflection.findGlobalObject[CassandraConnectionFactory](
      "com.datastax.driver.scala.core.SimpleConnectionFactory")
    factory should be(SimpleConnectionFactory)
  }

  it should "be able to instantiate a singleton object based on Java class name" in {
    val obj = Reflection.findGlobalObject[String]("java.lang.String")
    obj should be ("")
  }

  it should "cache Java class instances" in {
    val obj1 = Reflection.findGlobalObject[String]("java.lang.String")
    val obj2 = Reflection.findGlobalObject[String]("java.lang.String")
    obj1 shouldBe theSameInstanceAs (obj2)
  }

  it should "throw IllegalArgumentException when asked for a Scala object of wrong type" in {
    intercept[IllegalArgumentException] {
      Reflection.findGlobalObject[ClusterConfig](
        "com.datastax.driver.scala.core.conf.ReadConf")
    }
  }

  it should "throw IllegalArgumentException when asked for class instance of wrong type" in {
    intercept[IllegalArgumentException] {
      Reflection.findGlobalObject[Integer]("java.lang.String")
    }
  }

  it should "throw IllegalArgumentException when object does not exist" in {
    intercept[IllegalArgumentException] {
      Reflection.findGlobalObject[ClusterConfig]("NoSuchObject")
    }
  }

}
