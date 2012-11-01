package asyncdynamo

import nonblocking.Query
import org.scalatest.matchers.MustMatchers
import org.scalatest.FreeSpec
import java.util.UUID

import asyncdynamo.blocking._
import akka.dispatch.{Await, Future}
import akka.actor.ActorSystem


class OperationsTest extends FreeSpec with MustMatchers with DynamoTestObjectSupport{
  import DynamoTestDataObjects._

  implicit val sys = ActorSystem("test")

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

  "Client survives 100 parallel errors" in{
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

  "Query with range" in {
    val id = UUID.randomUUID().toString
    val obj1 = DynamoTestWithRangeObject(id, "1", "value 1")
    val obj2 = DynamoTestWithRangeObject(id, "2", "value 2")
    Save(obj1)
    Save(obj2)

    Query[DynamoTestWithRangeObject](id, "GT", List("0")).blockingStream must (contain(obj1) and contain(obj2))
    Query[DynamoTestWithRangeObject](id, "GT", List("1")).blockingStream must (contain(obj2) and not(contain(obj1)) )
  }

  import akka.util.duration._
  private def givenTestObjectsInDb(n: Int) : Seq[DynamoTestWithRangeObject] = {
    val id = UUID.randomUUID().toString
    Await.result(
      Future.sequence(
        (1 to n) map (i => nonblocking.Save(DynamoTestWithRangeObject(id, i.toString , "value "+i)).executeOn(dynamo)(n * 5 seconds))
      ), n * 5 seconds )
  }

  "Query works for more than 100 elements" in {
    val N = 150
    val objs = givenTestObjectsInDb(N)
    Query[DynamoTestWithRangeObject](objs(0).id, "GT", List("0")).blockingStream.size must be(N)
  }

  "Query honours limit parameter" in {
    val objs = givenTestObjectsInDb(3)
    Query[DynamoTestWithRangeObject](objs(0).id, "GT", List("0"), limit = 2).blockingStream must (contain(objs(0)) and contain(objs(1)) and contain(objs(2)))
  }

  "Query without range condition returns all elements matching the hash key" in {
    val objs = givenTestObjectsInDb(3)
    Query[DynamoTestWithRangeObject](objs(0).id).blockingStream must (contain(objs(0)) and contain(objs(1)) and contain(objs(2)))
  }

  "DeleteByRange" in {
    val objs = givenTestObjectsInDb(3)
    val id = objs(0).id

    nonblocking.DeleteByRange[DynamoTestWithRangeObject](id, range = "1") blockingExecute

    Query[DynamoTestWithRangeObject](id, "EQ", List("1")).blockingStream must be ('empty)
    Query[DynamoTestWithRangeObject](id, "EQ", List("2")).blockingStream must not be ('empty)
  }

  "DeleteByRange for range which is numeric" in pending

  "DeleteByRange with expected" in {
    val objs = givenTestObjectsInDb(3)
    val id = objs(0).id

    nonblocking.DeleteByRange[DynamoTestWithRangeObject](id, range = "1", expected = Map("otherValue" -> "value 1")) blockingExecute

    Query[DynamoTestWithRangeObject](id, "EQ", List("1")).blockingStream must be ('empty)
  }

  private def assertCanSaveGetObject() {
    val obj = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)
    Save(obj)

    val saved = Read[DynamoTestObject](obj.id).get
    assert(saved === obj)
  }

}

