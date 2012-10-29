package asyncdynamo

import nonblocking.{Read, Save}
import org.scalatest.matchers.MustMatchers
import org.scalatest.FreeSpec
import java.util.UUID
import asyncdynamo.DynamoTestObjectSupport
import asyncdynamo.DynamoTestDataObjects.DynamoTestObject
import asyncdynamo.nonblocking.Read
import asyncdynamo.nonblocking.Save


class DynamoReaderMonadTest extends FreeSpec with MustMatchers with DynamoTestObjectSupport{

  "Save/Get" in {
    val obj = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)

    val saved : Option[DynamoTestObject] = for {
      _ <- Save(obj)
      saved <- Read[DynamoTestObject](obj.id)
    } yield saved

    assert(saved.get === obj)
  }

}

