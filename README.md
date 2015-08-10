# async-dynamo

[ ![Build Status] [travis-image] ] [travis]
[ ![Release] [release-image] ] [releases]
[ ![License] [license-image] ] [license]

async-dynamo is an asynchronous Scala client for [Amazon DynamoDB] [dynamodb]. It is based on the Akka actor framework and provides an asynchronous API.

## Quick Start

For detailed information please read [User Guide][user_guide].

### SBT

Add this to your `built.sbt` file:

```scala
resolvers += "Snowplow Repo" at "http://maven.snplow.com/releases/"

libraryDependencies += "com.github.piotrga" %% "async-dynamo" % "2.0.0"
```

### Example

```scala

import com.github.piotrga.asyncdynamo._
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

### Asynchronous version

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

### Explicit type class definition

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

## Documentation

For detailed information please read [User Guide][user_guide].

## Information for developers

### Building

This library is build with SBT.

### AWS Credentials

In order for tests to be able to connect to Dynamo you have to open Amazon AWS account and pass the AWS credentials to scala via properties.
The easiest way to do this is to add them to SBT_OPTS variable, i.e.

    export SBT_OPTS="$SBT_OPTS -Damazon.accessKey=... -Damazon.secret=..."

To build async-dynamo run:

     sbt clean test

### IntelliJ and SBT

Generating IntelliJ project files:

    sbt gen-idea

_IMPORTANT: You need to run `sbt gen-idea` every time you change the dependencies._

If you want to refresh the snapshot dependencies (WHICH I TRY TO AVOID) run:

    sbt clean update

Click on Synchronize icon in IntelliJ - it should pick it up.

## Versioning

major.minor.patch-SNAPSHOT
ie.
0.12.1
or
0.12.2-SNAPSHOT

Please increment patch (release plugin does that) if the change is backward compatible.
Otherwise please bump the minor version.

Please do not depend on SNAPSHOTs as they promote chaos and lack of determinism.

## Releasing

Since we are not expecting many changes in this library we **should** not depend on snapshot versions.
It is much easier to apply this policy to the library.

In order to release a new version:
 - run `sbt release`
 - confirm or amend the release version
 - confirm next development version

## Copyright and license

Copyright 2012-2015 2ndlanguage Limited. This product includes software developed at 2ndlanguage Limited.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[travis-image]: https://travis-ci.org/zzztimbo/async-dynamo.png?branch=master
[travis]: https://travis-ci.org/zzztimbo/async-dynamo

[release-image]: http://img.shields.io/badge/release-2.0.0-blue.svg?style=flat
[releases]: https://github.com/zzztimbo/async-dynamo/releases

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[dynamodb]: http://aws.amazon.com/dynamodb/
[user_guide]: https://github.com/zzztimbo/async-dynamo/wiki/User-Guide
