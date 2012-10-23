package com.zeebox.dynamo

import org.scalatest.matchers.MustMatchers
import org.scalatest.{Suite, BeforeAndAfterAll, FreeSpec}
import java.util.UUID
import akka.util.duration._
import com.zeebox.dynamo.DynamoTestDataObjects.DynamoTestObject


class DynamoReaderMonadTest extends FreeSpec with MustMatchers with DynamoTestObjectSupport{
  import DbOperation._


  "Save/Get" in {
    val obj = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)

    val saveRead = for {
      _ <- Save(obj)
      saved <- Read[DynamoTestObject](obj.id)
    } yield saved

    val saved = saveRead.blockingExecute

    assert(saved.get === obj)
  }

}

