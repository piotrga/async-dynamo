package com.zeebox.dynamo

import org.scalatest.matchers.MustMatchers
import org.scalatest.{BeforeAndAfterAll, FreeSpec}
import akka.actor.Actor
import com.amazonaws.services.dynamodb.model.AttributeValue
import java.util.UUID

object DynamoTestDataObjects{
  case class DynamoTestObject(id:String, someValue:String)

  implicit object DynamoTestDO extends DynamoObject[DynamoTestObject]{
    def toDynamo(t: DynamoTestObject) = Map("id"->t.id, "someValue"->t.someValue)
    def fromDynamo(a: Map[String, AttributeValue]) = DynamoTestObject(a("id").getS, a("someValue").getS)
    protected def table = "dynamotest"
  }

  case class Broken(id:String)
  implicit object BrokenDO extends DynamoObject[Broken]{
    def toDynamo(t: Broken) = Map()
    def fromDynamo(a: Map[String, AttributeValue]) = Broken("wiejfi")
    protected def table = "nonexistenttable"
  }
}

class DynamoTest extends FreeSpec with MustMatchers with BeforeAndAfterAll{
  implicit val dynamo = Dynamo(DynamoConfig(System.getProperty("amazon.accessKey"), System.getProperty("amazon.secret"), "devng_", "https://dynamodb.eu-west-1.amazonaws.com"), 3)
  import DynamoTestDataObjects._


  "Save/Get" in {
    val obj = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)
    (dynamo ? Save(obj)).get

    val saved = Read[DynamoTestObject](obj.id).blockingExecute.get
    assert (saved === obj)
  }

  "Get returns None if record not found" in {
    assert( Read[DynamoTestObject](UUID.randomUUID().toString).blockingExecute === None )
  }

  "What happens if reading from non-existent table" in {
    intercept[ThirdPartyException]{
      Read[Broken](UUID.randomUUID().toString).blockingExecute
    }.getMessage must include("resource not found")
  }




  override protected def beforeAll() {
    super.beforeAll()
    dynamo.start()
  }

  override protected def afterAll() {
    super.afterAll()
    DeleteAll[DynamoTestObject].blockingExecute
    dynamo.stop()
  }
}

