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

import nonblocking.Query
import org.scalatest.matchers.MustMatchers
import org.scalatest.FreeSpec
import asyncdynamo.DynamoTestDataObjects.DynamoTestWithRangeObject
import java.util.UUID
import akka.dispatch.{Future, Await}
import akka.util.Timeout
import akka.actor.{Actor, Props, ActorSystem}

class ThrottlingTest extends FreeSpec with MustMatchers{
  import akka.util.duration._
  implicit val dynamo = Dynamo(
    DynamoConfig(
      System.getProperty("amazon.accessKey"),
      System.getProperty("amazon.secret"),
      tablePrefix = "devng_",
      endpointUrl = System.getProperty("dynamo.url", "https://dynamodb.eu-west-1.amazonaws.com" ),
      maxRetries = 4
    ), connectionCount = 10)
  implicit val timeout = Timeout(10 seconds)
  implicit val sys = ActorSystem("test")

  dynamo ! ('addListener, sys.actorOf(Props(new Actor{
    protected def receive = {
      case msg => println("EVENT_STREAM: " + msg)
    }
  })))

  "10k saves + 1 Query" ignore {
    val N = 10000
    val objs = givenTestObjectsInDb(N)
    Query[DynamoTestWithRangeObject](objs(0).id, "GT", List("0")).blockingStream.size must be(N)
  }


  private def givenTestObjectsInDb(n: Int) : Seq[DynamoTestWithRangeObject] = {
    val id = UUID.randomUUID().toString
    Await.result(
      Future.sequence(
        (1 to n) map (i => nonblocking.Save(DynamoTestWithRangeObject(id, i.toString , "value "+i)).executeOn(dynamo)(n * 5 seconds))
      ), n * 5 seconds )
  }


}
