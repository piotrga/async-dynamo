package com.zeebox.dynamo

import org.scalatest.matchers.MustMatchers
import org.scalatest.{BeforeAndAfterAll, FreeSpec}
import com.amazonaws.services.dynamodb.model.AttributeValue
import java.util.UUID
import akka.util.duration._
import akka.pattern.ask


class OperationsTest extends FreeSpec with MustMatchers with DynamoTestObjectSupport{
  import DynamoTestDataObjects._

  "Save/Get" in {
    assertCanSaveGetObject()
  }

  "Get returns None if record not found" in {
    assert( Read[DynamoTestObject](UUID.randomUUID().toString).blockingExecute === None )
  }

  "Reading from non-existent table causes ThirdPartyException" in {
    intercept[ThirdPartyException]{
      Read[Broken](UUID.randomUUID().toString).blockingExecute
    }.getMessage must include("resource not found")
  }

  "Client survives 100 parallel errors" in {
    (1 to 100).par.foreach{ i =>
      intercept[ThirdPartyException]{
        Read[Broken](UUID.randomUUID().toString).blockingExecute
      }
    }
    assertCanSaveGetObject()
  }

  "Delete" in {
    val obj = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)
    Save(obj).blockingExecute
    DeleteById[DynamoTestObject](obj.id).blockingExecute

    Read[DynamoTestObject](obj.id).blockingExecute must be ('empty)
  }

  private def assertCanSaveGetObject() {
    val obj = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)
    Save(obj).blockingExecute

    val saved = Read[DynamoTestObject](obj.id).blockingExecute.get
    assert(saved === obj)
  }

}

