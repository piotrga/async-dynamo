package com.zeebox.dynamo

import org.scalatest.{Suite, BeforeAndAfterAll}
import akka.util.duration._
import akka.actor.Kill
import com.amazonaws.services.dynamodb.model.AttributeValue

object DynamoTestDataObjects{
  case class DynamoTestObject(id:String, someValue:String)

  implicit object DynamoTestDO extends DynamoObject[DynamoTestObject]{
    def toDynamo(t: DynamoTestObject) = Map("id"->t.id, "someValue"->t.someValue)
    def fromDynamo(a: Map[String, AttributeValue]) = DynamoTestObject(a("id").getS, a("someValue").getS)
    protected val table = "%s_dynamotest" format Option(System.getenv("USER")).getOrElse("unknown") }

  case class Broken(id:String)
  implicit object BrokenDO extends DynamoObject[Broken]{
    def toDynamo(t: Broken) = Map()
    def fromDynamo(a: Map[String, AttributeValue]) = Broken("wiejfi")
    protected def table = "nonexistenttable"
  }
}

trait DynamoSupport extends BeforeAndAfterAll{ self : Suite =>
  implicit val dynamo = Dynamo(DynamoConfig(System.getProperty("amazon.accessKey"), System.getProperty("amazon.secret"), "devng_", System.getProperty("dynamo.url", "https://dynamodb.eu-west-1.amazonaws.com")), 3)
  implicit val timeout = 10 seconds

  override protected def afterAll() {
    println("Stopping dynamo")
    dynamo ! Kill
    super.afterAll()
  }
}

trait DynamoTestObjectSupport extends BeforeAndAfterAll with DynamoSupport{ self : Suite =>
  import DynamoTestDataObjects._

  override protected def beforeAll() {
    super.beforeAll()
    println("Creating Table for DynamoTestObject")
    if (!TableExists[DynamoTestObject]().blockingExecute){
      CreateTable[DynamoTestObject]().blockingExecute(dynamo,1 minute)
    }
    println("Table created")
  }

}

