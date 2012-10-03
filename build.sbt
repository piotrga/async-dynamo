import sbt.Package.ManifestAttributes

organization := "com.zeebox"

name := "async-dynamo"

version := "0.11.1"

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

publishMavenStyle := true

publishTo <<= (version) { version: String =>
  val nexus = "http://nexus.zeebox.com:8080/nexus/content/repositories/"
  if (version.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "snapshots/")
  else                                   Some("releases"  at nexus + "releases/")
}

credentials += Credentials(Path.userHome / ".ivy2" / "zeebox.credentials")

packageOptions <<= (Keys.version, Keys.name, Keys.artifact) map {
  (version: String, name: String, artifact: Artifact) =>
    Seq(ManifestAttributes(
      "Implementation-Vendor" -> "Zeebox",
      "Implementation-Title" -> "async-dynamo",
      "Version" -> version,
      "Build-Number" -> Option(System.getenv("GO_PIPELINE_COUNTER")).getOrElse("NOT_GO_BUILD"),
      "Group-Id" -> name,
      "Artifact-Id" -> artifact.name,
      "Git-SHA1" -> Git.hash,
      "Git-Branch" -> Git.branch,
      "Build-Jdk" -> System.getProperty("java.version"),
      "Built-When" -> (new java.util.Date).toString,
      "Build-Machine" -> java.net.InetAddress.getLocalHost.getHostName
    )
  )
}
