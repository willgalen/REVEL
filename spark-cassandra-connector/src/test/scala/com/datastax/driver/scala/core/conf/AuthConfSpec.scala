package com.datastax.driver.scala.core.conf

import com.datastax.driver.scala.core.conf.Configuration.Source
import com.datastax.driver.scala.testkit.AbstractSpec

class AuthConfSpec extends AbstractSpec {
  "AuthConf" must {
    "have the correct default auth settings" in {
      val settings = CassandraSettings()
      val auth = DefaultAuthConfFactory.authConf(settings.AuthUserName, settings.AuthPassword)
      auth.credentials.size should be(0)
      auth.credentials.get("username") should be(None)
      auth.credentials.get("password") should be(None)
    }
    "have the correct auth settings when passed in from alternate source" in {
      val source = Source(Map(
        Connection.AuthUserNameProperty -> "cassandra",
        Connection.AuthPasswordProperty -> "cassandra",
        Connection.AuthConfFqcnProperty -> classOf[PasswordAuthConf].getName))
      val settings = CassandraSettings(source, None)

      val auth = DefaultAuthConfFactory.authConf(settings.AuthUserName, settings.AuthPassword)
      auth.credentials.size should be(2)
      auth.credentials("username") should be("cassandra")
      auth.credentials("password") should be("cassandra")
    }
  }
}
