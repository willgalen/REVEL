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

object ProjectTemplates extends Build {
  import Settings._

  /* Project Templates */
  def AssemblyProject(name: String, cpd: Seq[ClasspathDep[ProjectReference]] = Seq.empty): Project =
    Project(name, file(name), settings = assembledSettings ++ Seq(libraryDependencies ++= Dependencies.connector),
      dependencies = cpd) configs (IntegrationTest, ClusterIntegrationTest)

  def UtilityProject(name: String, cpd: Seq[ClasspathDep[ProjectReference]], modules: Seq[ModuleID]): Project =
    Project(name, file(name),
      settings = defaultSettings ++ Seq(libraryDependencies ++= modules),
      dependencies = cpd
    ) configs (IntegrationTest, ClusterIntegrationTest)

  def DemoProject(name: String, modules: Seq[ModuleID], cpd: Seq[ClasspathDep[ProjectReference]]): Project =
    Project(id = name, base = file(s"spark-cassandra-connector-demos/$name"),
      settings = demoSettings ++ Seq(libraryDependencies ++= modules), dependencies = cpd)

  def RootProject(name: String, dir: sbt.File, contains: Seq[ProjectReference]): Project =
    Project(id = name, base = dir, settings = parentSettings, aggregate = contains)

}
