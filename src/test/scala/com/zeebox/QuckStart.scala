package com.zeebox

import com.zeebox.dynamo._
import nonblocking._
import akka.util.duration._
import akka.util.Timeout

object QuckStart extends App{
  implicit val dynamo = Dynamo(
    DynamoConfig(
      System.getProperty("amazon.accessKey"),
      System.getProperty("amazon.secret"),
      tablePrefix = "devng_",
      endpointUrl = System.getProperty("dynamo.url", "https://dynamodb.eu-west-1.amazonaws.com")
    ),
    connectionCount = 3)

  implicit val timeout = Timeout(10 seconds)

  try{
    case class Person(id :String, name: String, email: String)
    implicit val personDO = DynamoObject.of3(Person) // make Person dynamo-enabled

    if (! TableExists[Person]()) //implicit kicks in to convert DbOperation[T] to T
      CreateTable[Person](5,5).blockingExecute(dynamo, 1 minute) // this is long so explicit blocking call

    val julian = Person("123", "Julian", "julian@gmail.com")
    val saved : Option[Person] = Save(julian) andThen Read[Person](julian.id) // implicit automatically executes and blocks for convenience
    assert(saved == Some(julian))
  } finally dynamo ! 'stop
}
