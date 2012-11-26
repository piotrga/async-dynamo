organization := "asyncdynamo"

name := "async-dynamo"

scalaVersion := "2.9.2"

//resolvers += Resolver.file("piotrga", file(sys.env("PIOTRGA_GITHUB_REPO")))
resolvers += "piotrga-remote" at "https://raw.github.com/piotrga/piotrga.github.com/master/maven-repo"

// Libraries
libraryDependencies ++= Seq(
    "com.amazonaws" % "aws-java-sdk" % "1.3.5",
    "com.typesafe.akka" % "akka-actor" % "2.0"
)

// Test libraries
libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "1.6.1" % "test",
    "log4j" % "log4j" % "1.2.17" % "test",
    "monitoring" %% "monitoring" % "1.2.2" % "test"
)

