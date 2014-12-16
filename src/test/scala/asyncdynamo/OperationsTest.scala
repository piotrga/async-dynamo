/*
 * Copyright 2012 2ndlanguage Limited.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package asyncdynamo

import functional.Done
import asyncdynamo.nonblocking._
import org.scalatest.matchers.MustMatchers
import org.scalatest.FreeSpec
import java.util.UUID

import akka.actor.{Actor, Props, ActorSystem}
import concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import com.amazonaws.services.dynamodbv2.model.{ConditionalCheckFailedException, ComparisonOperator, ScalarAttributeType}
import asyncdynamo.functional.Done
import asyncdynamo.blocking.DeleteById
import asyncdynamo.blocking.Read
import asyncdynamo.blocking.Save
import asyncdynamo.nonblocking.QueryIndex
import asyncdynamo.nonblocking.ColumnCondition
import scala.annotation.tailrec


class OperationsTest extends FreeSpec with MustMatchers with DynamoTestObjectSupport{
  import DynamoTestDataObjects._

  implicit val sys = ActorSystem("test")
  val listener = sys.actorOf(Props(new Actor{
     def receive = {
      case msg => println("EVENT_STREAM: " + msg)
    }
  }))

  import scala.concurrent.duration._
  private def givenTestObjectsInDb(n: Int) : Seq[DynamoTestWithRangeObject] = {
    val id = UUID.randomUUID().toString
    Await.result(
      Future.sequence(
        (1 to n) map (i => nonblocking.Save(DynamoTestWithRangeObject(id, i.toString , "value "+i)).executeOn(dynamo)(n * 5 seconds))
      ), n * 5 seconds )
  }

  def getRootCause(ex: Throwable): Throwable = {
    if (ex.getCause eq null)
      ex
    else
      getRootCause(ex.getCause)
  }

  dynamo ! ('addListener, listener)
  createTables()

  "Save/Get" in {
    assertCanSaveGetObject()

    val id = UUID.randomUUID().toString
    val obj = DynamoTestWithRangeObject(id, "1", "value 1")
    Save(obj)

    val saved = Read[DynamoTestWithRangeObject](obj.id, Some("1")).get
    assert(saved === obj)
  }

  "Save existing value" in {
    val id = UUID.randomUUID().toString
    val obj = DynamoTestObject(id, "some test value" + math.random)
    Save(obj, false) //should save ok

    val saved = Read[DynamoTestObject](obj.id).get
    assert(saved === obj)

    val objAltered = DynamoTestObject(id, "some other test value" + math.random)

    try {
      Save(objAltered, false) //should fail
      fail(new IllegalStateException("Should never get here, ConditionalCheckFailedException expected already!"))
    }
    catch {
      case ex => {
        getRootCause(ex) match {
          case c: ConditionalCheckFailedException => assert(true) //expected
          case u => fail(u)
        }
      }
    }

    val saved2 = Read[DynamoTestObject](obj.id).get
    assert(saved2 === obj)

    Save(objAltered, true) //should save ok (overwrite existing)

    val saved3 = Read[DynamoTestObject](obj.id).get
    assert(saved3 === objAltered)


    val obj2 = DynamoTestWithRangeObject(id, "1", "value 1")
    Save(obj2, false) //should save ok

    val savedWithRange = Read[DynamoTestWithRangeObject](obj2.id, Some("1")).get
    assert(savedWithRange === obj2)

    val objAltered2 = DynamoTestWithRangeObject(id, "1", "value 778")

    val savedWithRange2 = Read[DynamoTestWithRangeObject](obj2.id, Some("1")).get
    assert(savedWithRange2 === obj2)

    try {
      Save(objAltered2, false) //should fail
      fail(new IllegalStateException("Should never get here, ConditionalCheckFailedException expected already!"))
    }
    catch {
      case ex => {
        getRootCause(ex) match {
          case c: ConditionalCheckFailedException => assert(true) //expected
          case u => fail(u)
        }
      }
    }

    Save(objAltered2, true) //should save ok (overwrite existing)

    val savedWithRange3 = Read[DynamoTestWithRangeObject](obj2.id, Some("1")).get
    assert(savedWithRange3 === objAltered2)
  }


  "Can update values in an existing record" in {
    val obj = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)
    Save(obj)

    val saved = Read[DynamoTestObject](obj.id).get
    assert(saved === obj)

    val objNew = DynamoTestObject(obj.id, "some new test value" + math.random)
    Update(objNew.id, objNew).blockingExecute

    val updated = Read[DynamoTestObject](objNew.id).get
    assert(updated === objNew)
  }

  "Can update values with a range key in an existing record" in {
    val id = UUID.randomUUID().toString
    val obj = DynamoTestWithRangeObject(id, "1", "value 1")
    Save(obj)

    val saved = Read[DynamoTestWithRangeObject](obj.id, Some("1")).get
    assert(saved === obj)

    val objNew = DynamoTestWithRangeObject(id, "1", "some new test value" + math.random)
    Update(objNew.id, objNew, Some(objNew.rangeValue)).blockingExecute

    val updated = Read[DynamoTestWithRangeObject](obj.id, Some("1")).get
    assert(updated === objNew)
  }

  "Get returns None if record not found" in {
    assert( Read[DynamoTestObject](UUID.randomUUID().toString) === None )
  }

  "Get returns when using a hash and range key" in {
    val id = UUID.randomUUID().toString
    val obj1 = DynamoTestWithRangeObject(id, "1", "value 1")
    val obj2 = DynamoTestWithRangeObject(id, "2", "value 2")
    Save(obj1)
    Save(obj2)

    val eh = asyncdynamo.nonblocking.Read[DynamoTestWithRangeObject](id, Some("1")).blockingExecute

    assert(eh === Some(obj1))
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

  "Query with range" in {
    val id = UUID.randomUUID().toString
    val obj1 = DynamoTestWithRangeObject(id, "1", "value 1")
    val obj2 = DynamoTestWithRangeObject(id, "2", "value 2")
    Save(obj1)
    Save(obj2)

    Query[DynamoTestWithRangeObject](id, "GT", List("0")).blockingStream must (contain(obj1) and contain(obj2))
    Query[DynamoTestWithRangeObject](id, "GT", List("1")).blockingStream must (contain(obj2) and not(contain(obj1)) )
  }

  "Query works for more than 100 elements" in {
    val N = 150
    val objs = givenTestObjectsInDb(N)
    Query[DynamoTestWithRangeObject](objs(0).id, "GT", List("0")).blockingStream.size must be(N)
  }

  "Query iteratee" in {
    import asyncdynamo.functional.Iteratee._
    import asyncdynamo._
    val N = 5
    val objs = givenTestObjectsInDb(N)
    val it = Await.result(Query[DynamoTestWithRangeObject](objs(0).id, "GT", List("0")).run(takeAll()), 5 seconds)

    it match {
      case Done(list)=> list.size must be(N)
      case x@_ => fail("Expected done but got " +x)
    }

  }

  "Query honours limit parameter" in {
    val objs = givenTestObjectsInDb(3)
    Query[DynamoTestWithRangeObject](objs(0).id, "GT", List("0"), limit = 2).blockingStream must (contain(objs(0)) and contain(objs(1)) and contain(objs(2)))
  }

  {
    val id = UUID.randomUUID().toString
    blocking.Save(DynamoTestWithNumericRangeObject(id, 100 , "value 100"))
    blocking.Save(DynamoTestWithNumericRangeObject(id, 50 , "value 50"))

    def query(from:Int, to:Int) = Query[DynamoTestWithNumericRangeObject](id, "BETWEEN", Seq(from, to)).blockingStream.toSeq

    "Query works with numeric range" in {
      query( 0, 150).size must be(2)
      query( 0,  55).size must be(1)
      query(60, 150).size must be(1)
    }

    "Query from is inclusive" in { query( 50, 51).size must be(1)}
    "Query to is inclusive" in { query( 0, 50).size must be(1)}
  }


  "Query without range condition returns all elements matching the hash key" in {
    val objs = givenTestObjectsInDb(3)
    Query[DynamoTestWithRangeObject](objs(0).id).blockingStream must (contain(objs(0)) and contain(objs(1)) and contain(objs(2)))
  }

  "Delete" in {
    val obj = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)
    Save(obj)
    DeleteById[DynamoTestObject](obj.id)

    Read[DynamoTestObject](obj.id) must be ('empty)
  }

  "Delete return deleted item" in {
    val obj = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)
    Save(obj)

    val out = DeleteById[DynamoTestObject](obj.id, retrieveBeforeDelete = true)
    out must be(Some(obj))

    Read[DynamoTestObject](obj.id) must be ('empty)

    val out2 = DeleteById[DynamoTestObject](obj.id, retrieveBeforeDelete = true)
    out2 must be(None)
  }

  "Delete with expected" in {
    val someVal = "some test value" + math.random
    val obj = DynamoTestObject(UUID.randomUUID().toString, someVal)
    Save(obj)

    try {
      DeleteById[DynamoTestObject](obj.id, expected = Map("someValue" -> "fdgfd"))
    } catch {
      case tpe: asyncdynamo.ThirdPartyException => tpe.getCause.getCause match {
        case ccfe: ConditionalCheckFailedException => "Expected"
        case e: Exception => fail(e)
      }
      case e: Exception => fail(e)
    }

    Read[DynamoTestObject](obj.id) must be (Some(obj))

    DeleteById[DynamoTestObject](obj.id, expected = Map("someValue" -> someVal))
    Read[DynamoTestObject](obj.id) must be ('empty)
  }

  "DeleteByRange" in {
    val objs = givenTestObjectsInDb(3)
    val id = objs(0).id

    val ff = nonblocking.DeleteByRange[DynamoTestWithRangeObject](id, range = "1") blockingExecute

    Query[DynamoTestWithRangeObject](id, "EQ", List("1")).blockingStream must be ('empty)
    Query[DynamoTestWithRangeObject](id, "EQ", List("2")).blockingStream must not be ('empty)
  }

  "DeleteByRange return deleted item" in {
    val objs = givenTestObjectsInDb(3)
    val id = objs(0).id

    val out = nonblocking.DeleteByRange[DynamoTestWithRangeObject](id, range = "1", retrieveBeforeDelete = true) blockingExecute

    out must be(Some(objs(0)))

    Query[DynamoTestWithRangeObject](id, "EQ", List("1")).blockingStream must be ('empty)
    Query[DynamoTestWithRangeObject](id, "EQ", List("2")).blockingStream must not be ('empty)

    val out2 = nonblocking.DeleteByRange[DynamoTestWithRangeObject](id, range = "1", retrieveBeforeDelete = true) blockingExecute

    out2 must be(None)
  }

  "DeleteByRange for range which is numeric" in pending

  "DeleteByRange with expected" in {
    val objs = givenTestObjectsInDb(3)
    val id = objs(0).id

    try {
      nonblocking.DeleteByRange[DynamoTestWithRangeObject](id, range = "1", expected = Map("otherValue" -> "value 100010")) blockingExecute
    } catch {
      case tpe: asyncdynamo.ThirdPartyException => tpe.getCause.getCause match {
        case ccfe: ConditionalCheckFailedException => "Expected"
        case e: Exception => fail(e)
      }
      case e: Exception => fail(e)
    }

    Query[DynamoTestWithRangeObject](id, "EQ", List("1")).blockingStream must be (Stream(objs(0)))

    nonblocking.DeleteByRange[DynamoTestWithRangeObject](id, range = "1", expected = Map("otherValue" -> "value 1")) blockingExecute

    Query[DynamoTestWithRangeObject](id, "EQ", List("1")).blockingStream must be ('empty)
  }

  {
    val N = 150
    val objs = givenTestObjectsInDb(N)
    val id = objs(0).id
    val colConditions = Seq(
      ColumnCondition("id", ScalarAttributeType.S, ComparisonOperator.EQ,id),
      ColumnCondition("rangeValue", ScalarAttributeType.S, ComparisonOperator.GT,"0"))

    "Scan works for more than 100 elements" in {
      Scan[DynamoTestWithRangeObject](colConditions).blockingStream.size must be(N)
    }

    "Batch delete works with range table" in {
      Query[DynamoTestWithRangeObject](objs(0).id).blockingStream must not be ('empty)

      val idAndRangePairs = for (obj <- objs) yield (obj.id,Some(obj.rangeValue))
      BatchDeleteById[DynamoTestWithRangeObject](idAndRangePairs) blockingExecute

      Query[DynamoTestWithRangeObject](objs(0).id).blockingStream must be ('empty)
    }
  }

  "Batch delete works with non-range table" in {

    val obj1 = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)
    Save(obj1)
    val obj2 = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)
    Save(obj2)
    val obj3 = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)
    Save(obj3)

    Query[DynamoTestObject](obj1.id).blockingStream must not be ('empty)

    val idAndRangePairs = Seq((obj1.id,None), (obj2.id,None), (obj3.id,None))
    BatchDeleteById[DynamoTestObject](idAndRangePairs) blockingExecute

    Query[DynamoTestObject](obj1.id).blockingStream must be ('empty)
    Query[DynamoTestObject](obj2.id).blockingStream must be ('empty)
    Query[DynamoTestObject](obj3.id).blockingStream must be ('empty)
  }

  "Query on Index works" in {
    val id = UUID.randomUUID().toString
    val obj1 = DynamoTestWithGlobalSecondaryIndexObject(id, 1, "value 1")
    val obj2 = DynamoTestWithGlobalSecondaryIndexObject(id, 2, "value 2")
    Save(obj1)
    Save(obj2)

    val colConditions = Seq(
      ColumnCondition("rangeValue", ScalarAttributeType.N, ComparisonOperator.EQ,"2"),
      ColumnCondition("someValue", ScalarAttributeType.S, ComparisonOperator.EQ,"value 2")
    )
    QueryIndex[DynamoTestWithGlobalSecondaryIndexObject]("TestSecondaryIndex",colConditions).blockingStream must (contain(obj2))

    val colConditions2 = Seq(
      ColumnCondition("rangeValue", ScalarAttributeType.N, ComparisonOperator.EQ,"1"),
      ColumnCondition("someValue", ScalarAttributeType.S, ComparisonOperator.EQ,"value 2")
    )
    QueryIndex[DynamoTestWithGlobalSecondaryIndexObject]("TestSecondaryIndex",colConditions2).blockingStream must be ('empty)
  }

  private def assertCanSaveGetObject() {
    val obj = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)
    Save(obj)

    val saved = Read[DynamoTestObject](obj.id).get
    assert(saved === obj)
  }
}