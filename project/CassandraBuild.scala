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

import sbt._
import sbt.Keys._

object CassandraBuild extends Build {
  import DriverSettings._

  lazy val root = (project in file("."))
    .settings(name := "root").settings(parentSettings:_*).aggregate(driver, akka, embeddedCassandra)

  lazy val driver = Project(
    id = "scala-driver",
    base = path("scala-driver"),
    settings = moduleSettings(DriverDependencies.driver),
    dependencies = Seq(CassandraBuild.embeddedCassandra % "test->test;it->it,test;")
  ) configs (IntegrationTest)

  lazy val akka = Project(
    id = "akka-cassandra",
    base = path("akka-cassandra"),
    settings = moduleSettings(DriverDependencies.akka),
    dependencies = Seq(driver, embeddedCassandra % "test->test;it->it,test;")
  ) configs (IntegrationTest)

  lazy val embeddedCassandra = Project(
    id = "embedded-cassandra",
    base = path("embedded-cassandra"),
    settings = moduleSettings(DriverDependencies.embedded)
  ) configs (IntegrationTest)


  // while in the connector
  def path(subdir: String) = file("scala-driver") / subdir

  def moduleSettings(modules: Seq[ModuleID]) =
    DriverSettings.defaultSettings ++ Seq(libraryDependencies ++= modules)

}

object DriverDependencies {

  object Versions {
    val Akka            = "2.3.4"
    val AkkaStream      = "0.11"
    val Cassandra       = "2.1.2"
    val CassandraDriver = "2.1.3"
    val CommonsConfig   = "1.9"
    val CommonsIO       = "2.4"
    val CommonsLang3    = "3.3.2"
    val Config          = "1.2.1"
    val Guava           = "14.0.1"
    val JDK             = "1.7"
    val JodaC           = "1.2"
    val JodaT           = "2.3"
    val Paranamer       = "2.7"
    val Scala           = "2.10.4"
    val ScalaTest       = "2.2.2"
    val Scalactic       = "2.2.2"
    val Slf4j           = "1.7.7"
  }

  import Versions._

  object Compile {

    val cassandraClient     = "org.apache.cassandra"    % "cassandra-clientutil"   % Cassandra exclude("com.google.guava", "guava") // ApacheV2
    val cassandraDriver     = "com.datastax.cassandra"  % "cassandra-driver-core"  % CassandraDriver // ApacheV2
    val commonsLang3        = "org.apache.commons"      % "commons-lang3"          % CommonsLang3    // ApacheV2
    val commonsConfig       = "commons-configuration"   % "commons-configuration"  % CommonsConfig   // ApacheV2
    val config              = "com.typesafe"            % "config"                 % Config          // ApacheV2
    val jodaC               = "org.joda"                % "joda-convert"           % JodaC
    val jodaT               = "joda-time"               % "joda-time"              % JodaT
    val paranamer           = "com.thoughtworks.paranamer" % "paranamer"           % Paranamer
    val reflect             = "org.scala-lang"          % "scala-reflect"          % Scala
    val slf4jApi            = "org.slf4j"               % "slf4j-api"              % Slf4j           // MIT

    object CassandraAkka {
      val akkaActor           = "com.typesafe.akka"       %% "akka-actor"          % Akka            // ApacheV2
      val akkaCluster         = "com.typesafe.akka"       %% "akka-cluster"        % Akka            // ApacheV2
      val akkaRemote          = "com.typesafe.akka"       %% "akka-remote"         % Akka            // ApacheV2
      val akkaSlf4j           = "com.typesafe.akka"       %% "akka-slf4j"          % Akka            // ApacheV2
      val akkaStream          = "com.typesafe.akka"       %% "akka-stream-experimental"    % AkkaStream  // ApacheV2
      val akkaHttpCore        = "com.typesafe.akka"       %% "akka-http-core-experimental" % AkkaStream  // ApacheV2
    }

    object Embedded {
      val cassandra           = "org.apache.cassandra"    % "cassandra-all"         % Cassandra      // ApacheV2
    }

    object Test {
      val akkaTestKit         = "com.typesafe.akka"       %% "akka-testkit"         % Akka            % "test,it" // ApacheV2
      val commonsIO           = "commons-io"              % "commons-io"            % CommonsIO       % "test,it" // ApacheV2
      val scalatest           = "org.scalatest"           %% "scalatest"            % ScalaTest       % "test,it" // ApacheV2
      val scalactic           = "org.scalactic"           %% "scalactic"            % Scalactic       % "test,it" // ApacheV2
      val scalaCompiler       = "org.scala-lang"          % "scala-compiler"        % Scala
      val mockito             = "org.mockito"             % "mockito-all"           % "1.10.19"       % "test,it" // MIT
    }
  }

  import Compile._

  val testkit = Seq(Test.scalatest, Test.scalaCompiler, Test.scalactic, Test.mockito)

  val driver = testkit ++ Seq(cassandraDriver, cassandraClient, commonsConfig,
    commonsLang3, config, jodaC, jodaT, paranamer, reflect, slf4jApi)

  val embedded = driver ++ Seq(Embedded.cassandra)

  val akka = testkit ++ {
    import CassandraAkka._
    Seq(akkaActor, akkaRemote, akkaSlf4j, akkaCluster, akkaStream, akkaHttpCore, slf4jApi, Test.akkaTestKit)
  }
}

object DriverSettings extends Build {
  import scala.language.postfixOps

  import sbt._
  import sbt.Keys._
  import sbtrelease.ReleasePlugin._
  import com.typesafe.tools.mima.plugin.MimaKeys._
  import com.typesafe.tools.mima.plugin.MimaPlugin._
  import scala.collection.mutable
  import net.virtualvoid.sbt.graph.Plugin.graphSettings
  import com.scalapenos.sbt.prompt._
  import SbtPrompt.autoImport._

  lazy val buildSettings = Seq(
    name := "Apache Cassandra Community Scala Driver",
    normalizedName := "scala-driver",
    description := "A Scala community driver that exposes Cassandra tables and executes CQL queries for scala applications.",
    organization := "com.datastax.driver.scala",
    organizationHomepage := Some(url("http://www.datastax.com/")),
    version in ThisBuild := "1.0.0-SNAPSHOT",
    scalaVersion := Versions.Scala,
    homepage := Some(url("http://github.com/datastax/scala-driver")),
    licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    promptTheme := theme
  )

  val parentSettings = buildSettings ++ Seq(
    publishArtifact := false,
    publish := {}
  )

  override lazy val settings = super.settings ++ buildSettings

  lazy val defaultSettings = graphSettings ++ mimaSettings ++ releaseSettings ++ testSettings ++ Seq(
    scalacOptions in (Compile, doc) ++= Seq("-implicits","-doc-root-content", "rootdoc.txt"),
    scalacOptions ++= Seq("-encoding", "UTF-8", s"-target:jvm-${Versions.JDK}", "-deprecation", "-feature", "-language:_", "-unchecked", "-Xlint"),
    javacOptions in (Compile, doc) := Seq("-encoding", "UTF-8", "-source", Versions.JDK),
    javacOptions in Compile ++= Seq("-encoding", "UTF-8", "-source", Versions.JDK, "-target", Versions.JDK, "-Xlint:unchecked", "-Xlint:deprecation"),
    ivyLoggingLevel in ThisBuild := UpdateLogging.Quiet,
    parallelExecution in ThisBuild := false,
    parallelExecution in Global := false,
    autoAPIMappings := true
  )

  lazy val mimaSettings = mimaDefaultSettings ++ Seq(
    previousArtifact := None
  )

  val testOptionSettings = Seq(
    Tests.Argument(TestFrameworks.ScalaTest, "-oDF")
  )

  val tests = inConfig(Test)(Defaults.testTasks) ++ inConfig(IntegrationTest)(Defaults.itSettings)

  lazy val testArtifacts = Seq(
    artifactName in (Test,packageBin) := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
      baseDirectory.value.name + "-test_" + sv.binary + "-" + module.revision + "." + artifact.extension
    },
    artifactName in (IntegrationTest,packageBin) := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
      baseDirectory.value.name + "-it_" + sv.binary + "-" + module.revision + "." + artifact.extension
    },
    publishArtifact in (Test,packageBin) := true,
    publishArtifact in (IntegrationTest,packageBin) := true,
    publish in (Test,packageBin) := {},
    publish in (IntegrationTest,packageBin) := {}
  )

  lazy val testSettings = tests ++ testArtifacts ++ Seq(
    javaOptions in run ++= Seq("-Djava.library.path=./sigar","-Xms128m", "-Xmx1024m", "-XX:+UseConcMarkSweepGC"),
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,
    testOptions in Test ++= testOptionSettings,
    testOptions in IntegrationTest ++= testOptionSettings,
    fork in Test := true,
    fork in IntegrationTest := true,
    (compile in IntegrationTest) <<= (compile in Test, compile in IntegrationTest) map { (_, c) => c },
    managedClasspath in IntegrationTest <<= Classpaths.concat(managedClasspath in IntegrationTest, exportedProducts in Test)
  )

  import com.scalapenos.sbt.prompt.SbtPrompt.autoImport._

  lazy val theme = PromptTheme(List(
      text("[SBT] ", fg(green)),
      userName(fg(26)),
      text("@", fg(green)),
      hostName(fg(26)),
      text(" on ", fg(000)),
      gitBranch(clean = fg(green), dirty = fg(blue)),
      text(" in ", fg(000)),
      currentProject(fg(magenta)),
      text(":  ", fg(green))
    ))
}