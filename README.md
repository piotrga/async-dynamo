What is async-dynamo?
=====================
async-dynamo is an asynchronous scala client for Amazon Dynamo database. It is based on Akka library and provides asynchronous API.

Quick Start
===========
For detailed information please read [User Guide][user_guide].

SBT
---
Add the following to your build.sbt file for Scala 2.10:
```scala
resolvers += "piotrga" at "https://raw.github.com/piotrga/piotrga.github.com/master/maven-repo/"

libraryDependencies += "asyncdynamo" % "async-dynamo_2.10" % "1.6.0"
```
or for scala 2.9.2

```
libraryDependencies += "asyncdynamo" % "async-dynamo_2.9.2" % "1.5.4"
```

Example
-------
```scala

import asyncdynamo._
import nonblocking._
import scala.concurrent.duration._
import akka.util.Timeout

object QuckStart extends App{
  implicit val dynamo = Dynamo( DynamoConfig( System.getProperty("amazon.accessKey"), System.getProperty("amazon.secret"), tablePrefix = "devng_", endpointUrl = System.getProperty("dynamo.url", "https://dynamodb.eu-west-1.amazonaws.com") ), connectionCount = 3)
  implicit val timeout = Timeout(10 seconds)

  try{
    case class Person(id :String, name: String, email: String)
    implicit val personDO = DynamoObject.of3(Person) // make Person dynamo-enabled

    if (! TableExists[Person]()) //implicit kicks in to execute operation as blocking
      CreateTable[Person](5,5).blockingExecute(dynamo, 1 minute) // overriding implicit timeout

    val julian = Person("123", "Julian", "julian@gmail.com")
    val saved : Option[Person] = Save(julian) andThen Read[Person](julian.id) // implicit automatically executes and blocks for convenience
    assert(saved == Some(julian))

  } finally dynamo ! 'stop
}
```

Asynchronous version
--------------------
```scala
val operation = for {
  _ <- Save(julian)
  saved <- Read[Person]("123")
  _ <- DeleteById[Person]("123")
} yield saved

(operation executeOn dynamo)
  .onSuccess { case person => println("Saved [%s]" format person)}
  .onComplete{ case _ => dynamo ! 'stop }
```

Explicit type class definition
------------------------------
If you need more flexibility when mapping your object to Dynamo table you can define the type class yourself, i.e.
```scala
    case class Account(id: String, balance: Double, lastModified: Date)

    implicit val AccoundDO : DynamoObject[Account] = new DynamoObject[Account]{
        val table = "account"
        def toDynamo( a : Account)  = Map( "id" -> a.id,
                  "balance" -> a.balance.toString,
                  "lastModified" -> formatter.toString(a.lastModified )

        def fromDynamo(f: Map[String, AttributeValue]) =
            Account( f("id").getS, f("balance").getS.toDouble, formatter.parse(f("lastModified").getS) )
    }
```

Documentation
=============
For detailed information please read [User Guide][user_guide].

Information for developers
==========================

Building
--------
This library is build with SBT.

### AWS Credentials
In order for tests to be able to connect to Dynamo you have to open Amazon AWS account and pass the AWS credentials to scala via properties.
The easiest way to do this is to add them to SBT_OPTS variable, i.e.

    export SBT_OPTS="$SBT_OPTS -Damazon.accessKey=... -Damazon.secret=..."

To build async-dynamo run:

     sbt clean test

IntelliJ and SBT
----------------
Generating IntelliJ project files:

    sbt gen-idea

_IMPORTANT: You need to run `sbt gen-idea` every time you change the dependencies._
If you want to refresh the snapshot dependencies (WHICH I TRY TO AVOID) run:

    sbt clean update
Click on Synchronize icon in IntelliJ - it should pick it up.

VERSIONING
----------
major.minor.patch-SNAPSHOT
ie.
0.12.1
or
0.12.2-SNAPSHOT

Please increment patch (release plugin does that) if the change is backward compatible.
Otherwise please bump the minor version.

Please do not depend on SNAPSHOTs as they promote chaos and lack of determinism.

RELEASING
---------
Since we are not expecting many changes in this library we SHOULD not depend on snapshot versions.
It is much easier to apply this policy to the library.

In order to release a new version:
 - run `sbt release`
 - confirm or amend the release version
 - confirm next development version

[user_guide]: doc/user_guide.md "User Guide"
