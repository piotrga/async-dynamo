package asyncdynamo

import org.scalatest.matchers.MustMatchers
import org.scalatest.FreeSpec
import java.util.UUID

import asyncdynamo.blocking._


class OperationsTest extends FreeSpec with MustMatchers with DynamoTestObjectSupport{
  import DynamoTestDataObjects._

  "Save/Get" in {
    assertCanSaveGetObject()
  }

  "Get returns None if record not found" in {
    assert( Read[DynamoTestObject](UUID.randomUUID().toString) === None )
  }

  "Reading from non-existent table causes ThirdPartyException" in {
    intercept[ThirdPartyException]{
      Read[Broken](UUID.randomUUID().toString)
    }.getMessage must include("resource not found")
  }

  "Client survives 100 parallel errors" in {
    (1 to 100).par.foreach{ i =>
      intercept[ThirdPartyException]{
        Read[Broken](UUID.randomUUID().toString)
      }
    }
    assertCanSaveGetObject()
  }

  "Delete" in {
    val obj = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)
    Save(obj)
    DeleteById[DynamoTestObject](obj.id)

    Read[DynamoTestObject](obj.id) must be ('empty)
  }

  private def assertCanSaveGetObject() {
    val obj = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)
    Save(obj)

    val saved = Read[DynamoTestObject](obj.id).get
    assert(saved === obj)
  }

}

