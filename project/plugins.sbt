resolvers += Resolver.url(
  "bintray-sbt-plugin-releases",
  url("http://dl.bintray.com/content/sbt/sbt-plugin-releases"))(
  Resolver.ivyStylePatterns)

addSbtPlugin("me.lessis" % "bintray-sbt" % "0.3.0")

resolvers += Classpaths.sbtPluginReleases
resolvers += Classpaths.typesafeReleases

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "0.99.5.1")
addSbtPlugin("com.sksamuel.scoverage" %% "sbt-coveralls" % "0.0.5")
addSbtPlugin("com.typesafe.sbt" % "sbt-pgp" % "0.8.3")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.0")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.0")
addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

// Add the following to have Git manage your build versions
resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.6.4")

