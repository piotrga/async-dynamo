organization := "asyncdynamo"

name := "async-dynamo"

scalaVersion := "2.10.1"

//resolvers += Resolver.file("piotrga", file(sys.env("PIOTRGA_GITHUB_REPO")))
resolvers += "piotrga-remote" at "https://raw.github.com/piotrga/piotrga.github.com/master/maven-repo"

// Libraries
libraryDependencies ++= Seq(
    "com.amazonaws" % "aws-java-sdk" % "1.4.6",
    "com.typesafe.akka" %% "akka-actor" % "2.2.0-RC2"
)

// Test libraries
libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "1.9.1" % "test",
    "log4j" % "log4j" % "1.2.17" % "test",
    "monitoring" %% "monitoring" % "1.4.0" % "test"
)

