import sbtrelease._

import sbtrelease.ReleasePlugin.ReleaseKeys._

import sbt.Package.ManifestAttributes

// RELEASE PLUGIN
releaseSettings

nextVersion := { ver => Version(ver).map(_.bumpBugfix.asSnapshot.string).getOrElse(versionFormatError) }

// PUBLISHING
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
      //"Implementation-Title" -> name,
      "Version" -> version,
      "Build-Number" -> Option(System.getenv("GO_PIPELINE_COUNTER")).getOrElse("NOT_GO_BUILD"),
      //"Group-Id" -> organization,
      "Artifact-Id" -> artifact.name,
      "Git-SHA1" -> Git.hash,
      "Git-Branch" -> Git.branch,
      "Build-Jdk" -> System.getProperty("java.version"),
      "Built-When" -> (new java.util.Date).toString,
      "Build-Machine" -> java.net.InetAddress.getLocalHost.getHostName
    )
  )
}
