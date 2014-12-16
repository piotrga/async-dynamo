import sbt._

object Dependencies {
  val resolutionRepos = Seq(
  )

  object V {
    // Java
    val awsSdk      = "1.7.5"
    // Scala
    val akkaActor   = "2.2.3"
    // Test
    val scalatest   = "1.9.1"
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