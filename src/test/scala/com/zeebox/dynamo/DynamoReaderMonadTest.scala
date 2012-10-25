package com.zeebox.dynamo

import nonblocking.{Read, Save}
import org.scalatest.matchers.MustMatchers
import org.scalatest.{Suite, BeforeAndAfterAll, FreeSpec}
import java.util.UUID
import akka.util.duration._
import com.zeebox.dynamo.DynamoTestDataObjects.DynamoTestObject


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

