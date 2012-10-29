package asyncdynamo

import org.scalatest.matchers.MustMatchers
import org.scalatest.FreeSpec
import akka.util.duration._
import asyncdynamo.{DynamoObject, DynamoSupport}

import asyncdynamo.nonblocking.{Read, Save, CreateTable, TableExists}

class DynamoObjectTest extends FreeSpec with MustMatchers with DynamoSupport{

  case class Person(id :String, name: String, email: String)
  implicit val personDO = DynamoObject.of3(Person)

  "Generates DO for basic case class" in {
    val tst = Person("12312321", "Piotr", "piotrga@gmail.com")
    assert(personDO.fromDynamo(personDO.toDynamo(tst)) == tst)
  }

  "Works with nulls" in {
    val tst2 = Person("12312321", "Piotr", null)
    assert(personDO.fromDynamo(personDO.toDynamo(tst2)) == tst2)
  }

  "Save/Read of dynamic DynamoObject" in {
    val tst = Person("12312321", "Piotr", "piotrga@gmail.com")
    if (! TableExists[Person]()) CreateTable[Person](5,5).blockingExecute(dynamo, 1 minute)

    val saved : Option[Person] = Save(tst) andThen Read[Person](tst.id)
    saved.get must be(tst)
  }

}

