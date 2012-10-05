Building
========
This library is build with SBT:
sbt clean test

IntelliJ and SBT
=================
Generating IntelliJ project files:
sbt gen-idea

IMPORTANT: You need to run sbt gen-idea every time you change the dependencies.
If you want to refresh the snapshot dependencies (WHICH I TRY TO AVOID) run:
sbt clean update
Click on Synchronize icon in IntelliJ - it should pick it up.

VERSIONING
===========
major.minor.patch-SNAPSHOT
ie.
0.12.1
or
0.12.2-SNAPSHOT

Please increment patch (release plugin does that) if the change is backward compatible.
Otherwise please bump the minor version.

Please do not depend on SNAPSHOTs as they promote chaos and lack of determinism.

RELEASING
==========
Since we are not expecting many changes in this library we SHOULD not depend on snapshot versions.
It is much easier to apply this policy to the library.

In order to release a new version run:
 - sbt release
 - confirm or amend the release version
 - confirm next development version