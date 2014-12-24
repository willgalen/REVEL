package com.datastax.driver.scala.core.conf

import com.datastax.driver.core.{AuthProvider, PlainTextAuthProvider}
import com.datastax.driver.scala.core.util.ReflectionUtil
import com.datastax.driver.scala.core.utils.ReflectionUtil

/** Stores credentials used to authenticate to a Cassandra cluster and uses them
  * to configure a Cassandra connection.
  * This driver provides implementations [[NoAuthConf]] for no authentication
  * and [[PasswordAuthConf]] for password authentication. Other authentication
  * configurators can be plugged in by setting `cassandra.auth.conf.class`
  * option. See [[AuthConf]]. */
trait AuthConf extends Serializable {

  /** Returns auth provider to be passed to the `Cluster.Builder` object. */
  def authProvider: AuthProvider

  /** Returns auth credentials to be set in the Thrift authentication request. */
  def thriftCredentials: Map[String, String]
}

/** Performs no authentication. Use with `AllowAllAuthenticator` in Cassandra. */
case object NoAuthConf extends AuthConf {
  override def authProvider = AuthProvider.NONE

  override def thriftCredentials: Map[String, String] = Map.empty
}

/** Performs plain-text password authentication. Use with `PasswordAuthenticator` in Cassandra. */
case class PasswordAuthConf(user: String, password: String) extends AuthConf {
  override def authProvider = new PlainTextAuthProvider(user, password)

  override def thriftCredentials: Map[String, String] = Map("username" -> user, "password" -> password)
}

/** Entry point for obtaining `AuthConf` object when establishing connections to Cassandra.
  * Supports no authentication or password authentication. Password authentication is
  * enabled when both `cassandra.auth.username` and `cassandra.auth.password` options are set.*/
object AuthConf {
  import settings._

  /** Attempts to read credentials from java system properties. */
  def apply: AuthConf = apply(CassandraUserName, CassandraPassword)

  def apply(username: Option[String], password: Option[String]): AuthConf = {
    def default: AuthConf = {
      val credentials =
        for (usr <- username;
             pwd <- password) yield (usr, pwd)

      credentials match {
        case Some((user, pass)) => PasswordAuthConf(user, pass)
        case None => NoAuthConf
      }
    }

    AuthConfFactoryClassName
      .map(ReflectionUtil.findGlobalObject[AuthConf])
      .getOrElse(default)

  }
}



