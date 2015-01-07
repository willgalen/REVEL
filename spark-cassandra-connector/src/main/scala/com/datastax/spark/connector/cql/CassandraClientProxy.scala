package com.datastax.spark.connector.cql

import java.lang.reflect.{InvocationTargetException, Proxy, Method, InvocationHandler}
import java.net.InetAddress

import com.datastax.driver.scala.core.conf.CassandraConnectorConf
import org.apache.cassandra.thrift.{AuthenticationRequest, TFramedTransportFactory, Cassandra}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TTransport

/** Extends `Cassandra.Iface` with `close` method to close the underlying thrift transport */
trait CassandraClientProxy extends Cassandra.Iface {
  def close()
}

private class ClientProxyHandler(client: Cassandra.Iface, transport: TTransport) extends InvocationHandler {
  
  override def invoke(proxy: scala.Any, method: Method, args: Array[AnyRef]): AnyRef = method match {
    case m if m.getName == "close" =>
      transport.close()
      null
    case m =>
      try m.invoke(client, args: _*) catch {
        case e: InvocationTargetException => throw e.getCause
      }
  }
}

object CassandraClientProxy {

  /** Returns a proxy to the thrift client that provides closing the underlying transport by calling `close` method.
    * Without this method we'd have to keep references to two objects: the client and the transport. */
  def wrap(conf: CassandraConnectorConf, host: InetAddress): CassandraClientProxy = {
    val (client, transport) = createThriftClient(conf, host)
    val classLoader = getClass.getClassLoader
    val interfaces = Array[Class[_]](classOf[CassandraClientProxy])
    val invocationHandler = new ClientProxyHandler(client, transport)
    Proxy.newProxyInstance(classLoader, interfaces, invocationHandler).asInstanceOf[CassandraClientProxy]
  }

  /** INTERNAL API.
    * Thrift going away eventually.
    * Creates and configures a Thrift client. To be removed in the near future,
    * when the dependency from Thrift will be completely dropped. */
  private def createThriftClient(conf: CassandraConnectorConf, hostAddress: InetAddress): (Cassandra.Iface, TTransport)= {
    import scala.collection.JavaConversions._
    var transport: TTransport = null
    try {
      val transportFactory = new TFramedTransportFactory()
      transport = transportFactory.openTransport(hostAddress.getHostAddress, conf.rpcPort)
      val client = new Cassandra.Client(new TBinaryProtocol(transport))
      val creds = conf.authConf.credentials
      if (creds.nonEmpty) {
        client.login(new AuthenticationRequest(creds))
      }
      (client, transport)
    }
    catch {
      case e: Throwable =>
        Option(transport) map (_.close)
        throw e
    }
  }
}
