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
import nonblocking.{CreateTable, Query}
import org.scalatest.matchers.MustMatchers
import org.scalatest.FreeSpec
import java.util.UUID

import asyncdynamo.blocking._
import akka.actor.{Actor, Props, ActorSystem}
import concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator


class OperationsTest extends FreeSpec with MustMatchers with DynamoTestObjectSupport{
  import DynamoTestDataObjects._

  implicit val sys = ActorSystem("test")
  val listener = sys.actorOf(Props(new Actor{
     def receive = {
      case msg => println("EVENT_STREAM: " + msg)
    }
  }))

  dynamo ! ('addListener, listener)
  createTables()

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

    Query[DynamoTestWithRangeObject](id, ComparisonOperator.GT, List("0")).blockingStream must (contain(obj1) and contain(obj2))
    Query[DynamoTestWithRangeObject](id, ComparisonOperator.GT, List("1")).blockingStream must (contain(obj2) and not(contain(obj1)) )
  }

  import scala.concurrent.duration._
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
    Query[DynamoTestWithRangeObject](objs(0).id, ComparisonOperator.GT, List("0")).blockingStream.size must be(N)
  }

  "Query iteratee" in {
    import asyncdynamo.functional.Iteratee._
    import asyncdynamo._
    val N = 5
    val objs = givenTestObjectsInDb(N)
    val it = Await.result(Query[DynamoTestWithRangeObject](objs(0).id, ComparisonOperator.GT, List("0")).run(takeAll()), 5 seconds)

    it match {
      case Done(list)=> list.size must be(N)
      case x@_ => fail("Expected done but got " +x)
    }

  }

  "Query honours limit parameter" in {
    val objs = givenTestObjectsInDb(3)
    Query[DynamoTestWithRangeObject](objs(0).id, ComparisonOperator.GT, List("0"), limit = 2).blockingStream must (contain(objs(0)) and contain(objs(1)) and contain(objs(2)))
  }

  {
    val id = UUID.randomUUID().toString
    blocking.Save(DynamoTestWithNumericRangeObject(id, 100 , "value 100"))
    blocking.Save(DynamoTestWithNumericRangeObject(id, 50 , "value 50"))

    def query(from:Int, to:Int) = Query[DynamoTestWithNumericRangeObject](id, ComparisonOperator.BETWEEN, Seq(from, to)).blockingStream.toSeq

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

  "DeleteByRange" in {
    val objs = givenTestObjectsInDb(3)
    val id = objs(0).id

    nonblocking.DeleteByRange[DynamoTestWithRangeObject](id, range = "1") blockingExecute

    Query[DynamoTestWithRangeObject](id, ComparisonOperator.EQ, List("1")).blockingStream must be ('empty)
    Query[DynamoTestWithRangeObject](id, ComparisonOperator.EQ, List("2")).blockingStream must not be ('empty)
  }

  "DeleteByRange for range which is numeric" in pending

  "DeleteByRange with expected" in {
    val objs = givenTestObjectsInDb(3)
    val id = objs(0).id

    nonblocking.DeleteByRange[DynamoTestWithRangeObject](id, range = "1", expected = Map("otherValue" -> "value 1")) blockingExecute

    Query[DynamoTestWithRangeObject](id, ComparisonOperator.EQ, List("1")).blockingStream must be ('empty)
  }

  private def assertCanSaveGetObject() {
    val obj = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)
    Save(obj)

    val saved = Read[DynamoTestObject](obj.id).get
    assert(saved === obj)
  }

}

