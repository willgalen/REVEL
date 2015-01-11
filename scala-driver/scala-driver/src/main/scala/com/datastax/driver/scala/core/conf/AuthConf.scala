package com.datastax.driver.scala.core.conf

import com.datastax.driver.core.{AuthProvider, PlainTextAuthProvider}
import com.datastax.driver.scala.core.utils.Reflection

/** Stores credentials used to authenticate to a Cassandra cluster and uses them
  * to configure a Cassandra connection. This driver provides implementations [[NoAuthConf]]
  * for no authentication and [[PasswordAuthConf]] for password authentication.
  * Other authentication configurators can be plugged in by setting `cassandra.auth.conf.class`
  * option. See [[AuthConf]]. */
trait AuthConf extends Serializable {

  /** Returns auth provider to be passed to the `Cluster.Builder` object. */
  def authProvider: AuthProvider

  /** Returns auth credentials to be set in an authentication request. */
  def credentials: Map[String, String]
}

/** Performs no authentication. Use with `AllowAllAuthenticator` in Cassandra. */
case object NoAuthConf extends AuthConf {
  override def authProvider = AuthProvider.NONE

  override def credentials: Map[String, String] = Map.empty
}

/** Performs plain-text password authentication. Use with `PasswordAuthenticator` in Cassandra. */
case class PasswordAuthConf(user: String, password: String) extends AuthConf {
  override def authProvider = new PlainTextAuthProvider(user, password)

  override def credentials: Map[String, String] = Map("username" -> user, "password" -> password)
}

/** Obtains authentication configuration. */
trait AuthConfFactory {
  def authConf(user: Option[String], pass: Option[String]): AuthConf
}

/** Default `AuthConfFactory` that supports no authentication or password authentication.
  * Password authentication is enabled when both `cassandra.auth.username` and `cassandra.auth.password`
  * options are present.*/
object DefaultAuthConfFactory extends AuthConfFactory {

  /** If both `username` and `password` are available, returns a [[PasswordAuthConf]]. */
  def authConf(username: Option[String], password: Option[String]): AuthConf = {
    val credentials = for {
      usr <- username
      pwd <- password
    } yield (usr, pwd)

    credentials match {
      case Some((user, pass)) => PasswordAuthConf(user, pass)
      case None => NoAuthConf
    }
  }
}

/** Entry point for obtaining `AuthConf` object from optional configurations.
  * Used when establishing connections to Cassandra. The actual `AuthConf` creation
  * is delegated to the [[AuthConfFactory]] pointed by `cassandra.auth.conf.factory` property. */
object AuthConf {

  def apply(settings: CassandraSettings): AuthConf = {
    import settings._
    AuthConfFqcn.map(Reflection.findGlobalObject[AuthConfFactory])
      .getOrElse(DefaultAuthConfFactory)
      .authConf(AuthUserName, AuthPassword)
  }

  private[datastax] def unapply(auth: AuthConf): Map[String, String] = {
    auth match {
      case NoAuthConf =>
        Map(Cluster.AuthConfFqcnProperty -> NoAuthConf.getClass.getName)
      case PasswordAuthConf(user,pass) =>
        Map(
          Cluster.AuthConfFqcnProperty -> classOf[PasswordAuthConf].getName,
          Cluster.AuthUserNameProperty -> user,
          Cluster.AuthPasswordProperty -> pass)
      case other => other.credentials.collect {
        case (k,v) if k == "username" => Cluster.AuthUserNameProperty -> v
        case (k,v) if k == "password" => Cluster.AuthPasswordProperty -> v
      } + (Cluster.AuthConfFqcnProperty -> other.getClass.getName)
    }

  }

}
