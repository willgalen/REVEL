/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.spark.connector.util

import org.apache.log4j.{LogManager, PropertyConfigurator}
import org.apache.spark.SparkContext
import org.slf4j.Logger
import org.slf4j.impl.StaticLoggerBinder
import com.datastax.driver.scala.core.utils.{Logging => ScalaDriverLogging}
/**
 * Spark Logging is marked as DeveloperApi.
 * This is a copy of it for compatibility.
 */
trait Logging extends ScalaDriverLogging {

  override protected def initializeLogging(): Unit = {
    // Don't use a logger in here, as this is itself occurring during initialization of a logger
    // If Log4j 1.2 is being used, but is not initialized, load a default properties file
    val binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
    // This distinguishes the log4j 1.2 binding, currently
    // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
    // org.apache.logging.slf4j.Log4jLoggerFactory
    val usingLog4j12 = "org.slf4j.impl.Log4jLoggerFactory".equals(binderClass)
    val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4j12Initialized && usingLog4j12) {
      val defaultLogProps = "org/apache/spark/log4j-defaults.properties"
      Option(getSparkClassLoader.getResource(defaultLogProps)) match {
        case Some(url) =>
          PropertyConfigurator.configure(url)
          System.err.println(s"Using Spark's default log4j profile: $defaultLogProps")
        case None =>
          System.err.println(s"Spark was unable to load $defaultLogProps")
      }
    }
    ScalaDriverLogging.initialized = true

    // Force a call into slf4j to initialize it. Avoids this happening from multiple threads
    // and triggering this: http://mailman.qos.ch/pipermail/slf4j-dev/2010-April/002956.html
    log
  }

  /**
   * Get the ClassLoader which loaded Spark.
   */
  def getSparkClassLoader = SparkContext.getClass.getClassLoader
}
