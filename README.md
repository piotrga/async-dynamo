What is async-dynamo?
=====================
async-dynamo is asynchronous client for Amazon Dynamo database. It is based on Akka library and provides asynchronous API.

Quick Start
===========
First let's start Dynamo client:

    implicit val dynamo = Dynamo(
    DynamoConfig(
      System.getProperty("amazon.accessKey"),
      System.getProperty("amazon.secret"),
      tablePrefix = "devng_",
      endpointUrl = System.getProperty("dynamo.url", "https://dynamodb.eu-west-1.amazonaws.com")
    ),
    connectionCount = 3)

Now let's create a Person case class and tell Dynamo how to save it:

    case class Person(id :String, name: String, email: String)
    implicit val personDO = DynamoObject.of3(Person) // make Person dynamo-enabled

Let's create a table in Dynamo:

    if (! TableExists[Person]()) //implicit kicks in to convert DbOperation[T] to T
      CreateTable[Person](5,5).blockingExecute(dynamo, 1 minute) // explicit blocking call to set custom timeout

And finally let's do some Dynamo operations:

     val julian = Person("123", "Julian", "julian@gmail.com")
     val saved : Option[Person] = Save(julian) andThen Read[Person](julian.id) // implicit automatically executes and blocks for convenience
     assert(saved == Some(julian))

Here is a full example:
```scala
    package com.zeebox

    import com.zeebox.dynamo._
    import nonblocking._
    import akka.util.duration._
    import akka.util.Timeout

    object QuckStart extends App{
      implicit val dynamo = Dynamo( DynamoConfig( System.getProperty("amazon.accessKey"), System.getProperty("amazon.secret"), tablePrefix = "devng_", endpointUrl = System.getProperty("dynamo.url", "https://dynamodb.eu-west-1.amazonaws.com") ), connectionCount = 3)
      implicit val timeout = Timeout(10 seconds)

      try{
        case class Person(id :String, name: String, email: String)
        implicit val personDO = DynamoObject.of3(Person) // make Person dynamo-enabled

        if (! TableExists[Person]()) //implicit kicks in to convert DbOperation[T] to T
          CreateTable[Person](5,5).blockingExecute(dynamo, 1 minute) // explicit blocking call to set custom timeout

        val julian = Person("123", "Julian", "julian@gmail.com")
        val saved : Option[Person] = Save(julian) andThen Read[Person](julian.id) // implicit automatically executes and blocks for convenience
        assert(saved == Some(julian))

      } finally dynamo ! 'stop
    }
```

Information for developers
==========================

Building
--------
This library is build with SBT:

     sbt clean test

IntelliJ and SBT
----------------
Generating IntelliJ project files:

    sbt gen-idea

_IMPORTANT: You need to run sbt gen-idea every time you change the dependencies._
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
 - run

    sbt release
 - confirm or amend the release version
 - confirm next development version