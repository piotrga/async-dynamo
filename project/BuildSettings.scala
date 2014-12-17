import sbt._
import Keys._

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq[Setting[_]](
    organization  := "com.github.piotrga",
    version       := "1.7.1",
    description   := "Asynchronous Scala client for Amazon DynamoDB",
    scalaVersion  := "2.10.1",
    crossScalaVersions := Seq("2.10.1", "2.11.4"),
    scalacOptions := Seq("-deprecation", "-feature", "-encoding", "utf8"),
    resolvers     ++= Dependencies.resolutionRepos
  )

  // Publish settings
  // TODO: update with ivy credentials etc when we start using Nexus
  lazy val publishSettings = Seq[Setting[_]](
   
    crossPaths := false,
    publishTo <<= version { version =>
      val keyFile = (Path.userHome / ".ssh" / "admin_keplar.osk")
      val basePath = "/var/www/maven.snplow.com/prod/public/%s".format {
        if (version.trim.endsWith("SNAPSHOT")) "snapshots/" else "releases/"
      }
      Some(Resolver.sftp("SnowPlow Analytics Maven repository", "prodbox", 8686, basePath) as ("admin", keyFile))
    }
  )

  lazy val buildSettings = basicSettings ++ publishSettings
}