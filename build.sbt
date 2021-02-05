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

name := "itachi"

version := "0.0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.1.0"

libraryDependencies += "org.apache.spark" %% s"spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% s"spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% s"spark-catalyst" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% s"spark-hive" % sparkVersion % "provided"

libraryDependencies += "org.scalatest" %% s"scalatest" % "3.2.3" % "test"
libraryDependencies += "org.scalacheck" %% s"scalacheck" % "1.14.2" % "test"

fork in Test := true
parallelExecution in Test := true