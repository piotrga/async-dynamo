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

object Dependencies {
  val resolutionRepos = Seq(
  )

  object V {
    // Java
    val awsSdk      = "1.7.5"
    // Scala
    val akkaActor   = "2.3.12"
    // Test
    val scalatest   = "2.2.4"
    val log4j       = "1.2.17"
  }

  object Libraries {
    // Java
    val awsSdk      = "com.amazonaws"              %  "aws-java-sdk"         % V.awsSdk
    // Scala
    val akkaActor   = "com.typesafe.akka"          %% "akka-actor"           % V.akkaActor
    // Test
    val scalatest   = "org.scalatest"              %% "scalatest"            % V.scalatest   % "test"
    val log4j       = "log4j"                      %  "log4j"                % V.log4j       % "test"
  }
}
