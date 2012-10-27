
package com.zeebox

import com.zeebox.dynamo._
import nonblocking._
import akka.util.duration._
import akka.util.Timeout

object QuckStart extends App{
  implicit val dynamo = Dynamo(DynamoConfig(System.getProperty("amazon.accessKey"), System.getProperty("amazon.secret"), tablePrefix = "devng_", endpointUrl = System.getProperty("dynamo.url", "https://dynamodb.eu-west-1.amazonaws.com")), connectionCount = 3)
  implicit val timeout = Timeout(10 seconds)

  case class Person(id :String, name: String, email: String)
  implicit val personDO = DynamoObject.of3(Person) // make Person dynamo-enabled

  val julian = Person("123", "Julian", "julian@gmail.com")

  case class Account(id:String, balance:Double)

  def transfer(amount: Double, fromId: String, toId: String) = for{
    accountFrom <- Read[Account](fromId).map(_.getOrElse(notFoundError(fromId)))
    accountTo <- Read[Account](toId).map(_.getOrElse(notFoundError(toId)))
    accountFromAfter <- Save(accountFrom.copy(balance = accountFrom.balance - amount))
    accountToAfter <- Save(accountTo.copy(balance = accountTo.balance + amount))
  } yield (accountFromAfter, accountToAfter )


  def notFoundError(id: Any): Nothing = sys.error("Account [%s] not found" format id)

  def sync(){
    try{
      if (! TableExists[Person]()) //implicit kicks in to convert DbOperation[T] to T
        CreateTable[Person](5,5).blockingExecute(dynamo, 1 minute) // this is takes long, so explicit blocking call

      val saved : Option[Person] = Save(julian) andThen Read[Person](julian.id) // implicit automatically executes and blocks for convenience
      assert(saved == Some(julian))

    } finally dynamo ! 'stop
  }

  def async(){
    val operation = for {
      _ <- Save(julian)
      saved <- Read[Person]("123")
      _ <- DeleteById[Person]("123")
    } yield saved

    (operation executeOn dynamo)
      .onSuccess { case person => println("Saved [%s]" format person)}
      .onComplete{ case _ => dynamo ! 'stop }
  }
}
