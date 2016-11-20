/*
 * Copyright 2012-2015 2ndlanguage Limited.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.github.piotrga.asyncdynamo

// Scala
import concurrent.duration._
import concurrent.ExecutionContext.Implicits.global
import language.postfixOps
import util.Success

// Akka
import akka.util.Timeout

// This project
import nonblocking._

object QuckStart extends App {
  implicit val dynamo = Dynamo(DynamoConfig(System.getProperty("amazon.accessKey"), System.getProperty("amazon.secret"), tablePrefix = "devng_", endpointUrl = System.getProperty("dynamo.url", "https://dynamodb.eu-west-1.amazonaws.com")), connectionCount = 3)
  implicit val timeout = Timeout(10 seconds)

  case class Person(id: String, name: String, email: String)
  implicit val personDO = DynamoObject.of3(Person) // make Person dynamo-enabled

  val julian = Person("123", "Julian", "julian@gmail.com")

  case class Account(id: String, balance: Double)
  implicit val xxx: DynamoObject[Account] = null

  def transfer(amount: Double, fromId: String, toId: String) = for {
    accountFrom <- Read[Account](fromId).map(_.getOrElse(notFoundError(fromId)))
    accountTo <- Read[Account](toId).map(_.getOrElse(notFoundError(toId)))
    accountFromAfter <- Save(accountFrom.copy(balance = accountFrom.balance - amount))
    accountToAfter <- Save(accountTo.copy(balance = accountTo.balance + amount))
  } yield (accountFromAfter, accountToAfter)

  def notFoundError(id: Any): Nothing = sys.error("Account [%s] not found" format id)

  def sync() {
    try {
      if (!TableExists[Person]()) //implicit kicks in to convert DbOperation[T] to T
        CreateTable[Person](5, 5).blockingExecute(dynamo, 1 minute) // this is takes long, so explicit blocking call

      val saved: Option[Person] = Save(julian) andThen Read[Person](julian.id) // implicit automatically executes and blocks for convenience
      assert(saved == Some(julian))

    } finally dynamo ! 'stop
  }

  def async() {
    val operation = for {
      _ <- Save(julian)
      saved <- Read[Person]("123")
      _ <- DeleteById[Person]("123")
    } yield saved

    (operation executeOn dynamo)
      .andThen { case Success(person) => println("Saved [%s]" format person) }
      .andThen { case _ => dynamo ! 'stop }
  }
}
