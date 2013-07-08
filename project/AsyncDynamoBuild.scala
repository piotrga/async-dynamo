import sbt._
import Keys._

object AsyncDynamoBuild extends Build {

  import Dependencies._
  import BuildSettings._

  // Configure prompt to show current project
  override lazy val settings = super.settings :+ {
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
  }

  // Define our project, with basic project information and library dependencies
  lazy val project = Project("async-dynamo", file("."))
    .settings(buildSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        Libraries.awsSdk,
        Libraries.akkaActor,
        Libraries.scalatest,
        Libraries.log4j,
        Libraries.monitoring
      )
    )
}
