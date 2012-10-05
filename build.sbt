organization := "com.zeebox"

name := "async-dynamo"

scalaVersion := "2.9.1"

resolvers ++= Seq(
        "Nexus Snapshots" at "http://nexus.zeebox.com:8080/nexus/content/groups/public-snapshots",
        "Nexus" at "http://nexus.zeebox.com:8080/nexus/content/groups/public"
)

// Libraries
libraryDependencies ++= Seq(
    "com.amazonaws" % "aws-java-sdk" % "1.3.5",
    "se.scalablesolutions.akka" % "akka-actor" % "1.3"
)

// Test libraries
libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "1.6.1" % "test"
)
