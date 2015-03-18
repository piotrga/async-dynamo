2014-12-17  Alex Dean  <alex@snowplowanalytics.com>

	Replaced .gitignore with canonical Scala one (closes #21)

	Changed package to com.github.piotrga.asyncdynamo (closes #22)

2014-12-17  Alexander Dean  <alex@snowplowanalytics.com>

	To match version in project build

2014-12-17  Alex Dean  <alex@snowplowanalytics.com>

	Added cross-build for Scala 2.11.4 (closes #18)

2014-12-17  tim  <glidester@gmail.com>

	Adding "-feature" to build settings and clearing all feature warnings

	Fix deprecation warnings in DynamoObject. Fixes GitHub Issue #15

	Fix compiler warning in operations test

2014-12-16  Alex Dean  <alex@snowplowanalytics.com>

	Bumped SBT to 0.13.2 (closes #20)

	Moved User Guide into wiki (closes #19)

	Merge branch 'feature/project.scala' into reboot

	Removed unknown monitoring dependency

	Reverted versions back to reboot's versions

2014-12-16  Alex Dean  <alexander.dean@keplarllp.com>

	Tidied formatting and added license and copyright to bottom
	Conflicts:
		README.md

	Moved to standard Snowplow configuration ready for release
	Conflicts:
		build.sbt
		project/plugins.sbt
		release.sbt
		version.sbt

2014-12-16  Alex Dean  <alex@snowplowanalytics.com>

	Added Travis support to project (closes #16)

2014-12-16  tim  <glidester@gmail.com>

	Cherry-picking @bkempe extensibility commit
	Making Dynamo actor extensible and allowing null consumedCapacityUnits as per DynamoDB spec

	Conflicts:

		src/main/scala/asyncdynamo/Dynamo.scala
		src/main/scala/asyncdynamo/TracingAmazonDynamoDB.scala
		version.sbt

2014-12-16  tim  <glidester@gmail.com>

	Cherry-picking the last of my fork's changes
	Remove redundant ListAll operation. Scan does this perfectly well on its own
	Added new feature for Delete operations to optionally return the deleted item
	Adding 'overwriteExisting' flag to Save operation to allow for persisting only if PK values do not already exist in the table
	Fix silly bug for with range tables and improve tests for Save operation

	More cherry-picks. See below for details:
	Adding in Scan support
	Adding BatchDeleteByID operation
	Adding 'Update' operation
	Make update operation return the updated version of the entity
	Add support for retrieving and updating an entry using a hash + range key
	Add support for querying on and creating tables with local and global secondary indexes
	Clearup ambiguity  in tests for new blocking .Update

	remove commented out code block

	Cherry-picking my version of the upgrade to dynamodbv2. This is where my code departs from @madsmith's version (so maybe contentious).
	!!BROKEN!! WIP converting to dynamodbv2
	First working version passing tests
	WIP BROKEN

	Upgrading aws-java-sdk library to 1.7.5

	Second set of cherry-picks from forks, all uncontentious stuff.
	Fix warning - a pure expression does nothing in statement position
	Fix Warning - This catches all Throwables. If this is really intended, use `case _ : Throwable` to clear this warning.
	Fix deprecation warning

	First set of cherry-picks from other forks to get to a working sbt compile & test state.
	Upgrading idea plugin
	Fix scala version esclation warning
	Fix feature warnings
	Stop publishing to remote repro
	`
	Suppress warning on overly broad catch
	Update versions of akka and aws
	Fix sbt dependencies resolution errors
	Implement missing setRegion method

2013-04-22  Piotr Gabryanczyk  <piotrga@gmail.com>

	Update README.md

	Update README.md

2013-03-01  Piotr Gabryanczyk  <piotrga@gmail.com>

	docs

	Setting version to 1.6.1-SNAPSHOT

	Setting version to 1.6.0

	docs

	migration to scala 2.10 and akka 2.1.1

2013-01-11  Piotr Gabryanczyk  <piotrga@gmail.com>

	Setting version to 1.5.5-SNAPSHOT

	Setting version to 1.5.4

	withFilter for iteratees

2013-01-09  Piotr Gabryanczyk  <piotrga@gmail.com>

	Setting version to 1.5.4-SNAPSHOT

	Setting version to 1.5.3

	Merge branch 'master' of github.com:piotrga/async-dynamo

	added support for numeric range attributes

2013-01-04  Piotr Gabryanczyk  <piotrga@gmail.com>

	Update doc/user_guide.md

	Update README.md

	Update README.md

	Update README.md

	Update README.md

	Update README.md

	Update README.md

2012-12-03  Piotr Gabryanczyk  <piotrga@gmail.com>

	Setting version to 1.5.3-SNAPSHOT

	Setting version to 1.5.2

	added durations of actual dynamo requests

2012-11-26  Piotr Gabryanczyk  <piotrga@gmail.com>

	renamed piotrga repo

	Setting version to 1.5.1

2012-11-25  Piotr Gabryanczyk  <piotrga@gmail.com>

	iteratees!!!

2012-11-19  Piotr Gabryanczyk  <piotrga@gmail.com>

	Setting version to 1.5.1-SNAPSHOT

	Setting version to 1.5.0

	tracing tablename - silly...

	Setting version to 1.4.1-SNAPSHOT

	Setting version to 1.4.0

	tracing tablename - silly...

2012-11-18  Piotr Gabryanczyk  <piotrga@gmail.com>

	Setting version to 1.3.2-SNAPSHOT

	Setting version to 1.3.1

	unit consumptions stats

2012-11-11  Piotr Gabryanczyk  <piotrga@gmail.com>

	Setting version to 1.3.1-SNAPSHOT

	Setting version to 1.3.0

	ThrottlingRecoveryStrategy

2012-11-09  Piotr Gabryanczyk  <piotrga@gmail.com>

	- full control over retrying provisioning exceeded failures

2012-11-08  Piotr Gabryanczyk  <piotrga@gmail.com>

	throughput event

	event stream

	Setting version to 1.2.2-SNAPSHOT

	Setting version to 1.2.1

	Smallest mailbox router

2012-11-07  Piotr Gabryanczyk  <piotrga@gmail.com>

	Setting version to 1.2.1-SNAPSHOT

	Setting version to 1.2.0

	Added configuration for: - timeout - max retries
	Set max connections per actor to 1

2012-11-05  Piotr Gabryanczyk  <piotrga@gmail.com>

	Setting version to 1.1.2-SNAPSHOT

	Setting version to 1.1.1

	dynamo-connection-dispatcher

2012-11-02  Piotr Gabryanczyk  <piotrga@gmail.com>

	Setting version to 1.1.1-SNAPSHOT

	Setting version to 1.1.0

	APL 2.0 to resources

	APL 2.0

	added license

2012-11-01  Piotr Gabryanczyk  <piotrga@gmail.com>

	- optional range condition in Query - better error messages

2012-10-31  Piotr Gabryanczyk  <piotrga@gmail.com>

	Setting version to 1.0.3-SNAPSHOT

	Setting version to 1.0.2

2012-10-30  Piotr Gabryanczyk  <piotrga@gmail.com>

	new operations: Query DeleteByRange

	new operations: Query DeleteByRange

2012-10-29  Piotr Gabryanczyk  <piotrga@gmail.com>

	Setting version to 1.0.1-SNAPSHOT

	Setting version to 1.0.0

	moving to asyncdynamo

	- moving repos - key

2012-10-27  Piotr Gabryanczyk  <piotrga@gmail.com>

	Update doc/user_guide.md

	simplification of docs

	fixed the monadic example

	cosmetics

	user guide link

	user guide link

	user guide link

	user guide link

	user guide link

	user guide - ...

	user guide - basic ops

	user guide - first draft

2012-10-26  Piotr Gabryanczyk  <piotrga@gmail.com>

	Setting version to 1.1.1-SNAPSHOT

	Setting version to 1.1.0

	docs

	docs

	docs

	docs

	docs

	docs

	docs

	docs

	more docs

	merge

2012-10-25  Piotr Gabryanczyk  <piotrga@gmail.com>

	ignore

	docs

	docs

	docs

	docs + nonblocking/blocking

2012-10-23  Piotr Gabryanczyk  <piotrga@gmail.com>

	Setting version to 1.0.1-SNAPSHOT

	Setting version to 1.0.0

	tests for admin operations

	Unit tests pass after migration to Akka 2

	sbt.sh

	Setting version to 0.12.1-SNAPSHOT

	Setting version to 0.12.0

	- DbOperation is a monad - DynamicDynamoObjects

2012-10-21  Piotr Gabryanczyk  <piotrga@gmail.com>

	few more arities

	generating DynamoObject

2012-10-05  Piotr Gabryanczyk  <piotrga@gmail.com>

	readme

	cleanup in build

	releasing

2012-10-03  Piotr Gabryanczyk  <piotrga@gmail.com>

	0.11.2-SNAPSHOT

	0.11.1 - added DeleteById

2012-10-02  Piotr Gabryanczyk  <piotrga@gmail.com>

	0.11.0 - removed timeouts

	removing default timeouts

2012-10-01  Piotr Gabryanczyk  <piotrga@gmail.com>

	0.10.0-SNAPSHOT

	admin opps

	admin opps

	reshuffling + resilience test

	init

2012-09-30  Piotr Gabryanczyk  <piotrga@gmail.com>

	first commit
