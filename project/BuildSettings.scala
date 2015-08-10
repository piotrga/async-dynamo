/*
 * Copyright 2012-2015 2ndlanguage Limited.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
import sbt._
import Keys._

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq[Setting[_]](
    organization  := "com.github.piotrga",
    version       := "2.0.0-SNAPSHOT",
    description   := "Asynchronous Scala client for Amazon DynamoDB",
    scalaVersion  := "2.10.1",
    //crossScalaVersions := Seq("2.10.1", "2.11.4"),
    scalacOptions := Seq("-deprecation", "-feature", "-encoding", "utf8"),
    resolvers     ++= Dependencies.resolutionRepos
  )

  // Publish settings
  // TODO: update with ivy credentials etc when we start using Nexus
  lazy val publishSettings = Seq[Setting[_]](
    // Enables publishing to maven repo
    publishMavenStyle := true,

    publishTo <<= version { version =>
      val basePath = "target/repo/%s".format {
        if (version.trim.endsWith("SNAPSHOT")) "snapshots/" else "releases/"
      }
      Some(Resolver.file("Local Maven repository", file(basePath)) transactional())
    }
  )

  lazy val buildSettings = basicSettings ++ publishSettings
}