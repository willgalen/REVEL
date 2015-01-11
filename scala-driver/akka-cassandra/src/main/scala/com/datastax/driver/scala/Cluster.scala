package com.datastax.driver.scala

import java.net.InetSocketAddress

import akka.io.{IO, Tcp}
import akka.io.Tcp.Connect

object Cluster {

  def fu(remote: InetSocketAddress) = IO(Tcp) ! Connect(remote)
}
