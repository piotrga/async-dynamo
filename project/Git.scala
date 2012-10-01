import sbt._

object Git {
  object devnull extends ProcessLogger {
    def info (s: => String) {}
    def error (s: => String) {}
    def buffer[T] (f: => T): T = f
  }

  def silentExec(cmd: String) = cmd lines_! devnull

  def branch =
    silentExec("git status -sb").headOption getOrElse "-" stripPrefix "## "

  def hash =
    silentExec("git log --pretty=format:%H -n1").headOption getOrElse "-"
}