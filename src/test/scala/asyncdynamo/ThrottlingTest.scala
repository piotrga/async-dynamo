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
import asyncdynamo.DynamoTestDataObjects.DynamoTestWithRangeObject
import java.util.UUID
import akka.util.Timeout
import akka.actor.{ActorRef, Actor, Props, ActorSystem}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{TimeUnit, CountDownLatch}
import util.{Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps


object ThrottlingTest extends  App{
  def main(args : Seq[String]){
    import scala.concurrent.duration._
    implicit val dynamo : ActorRef = Dynamo(
      DynamoConfig(
        System.getProperty("amazon.accessKey"),
        System.getProperty("amazon.secret"),
        tablePrefix = "devng_",
        endpointUrl = System.getProperty("dynamo.url", "https://dynamodb.eu-west-1.amazonaws.com" ),
        throttlingRecoveryStrategy = AmazonThrottlingRecoveryStrategy(4)
        //      throttlingRecoveryStrategy = ExpotentialBackoffThrottlingRecoveryStrategy(maxRetries = 3, backoffBase = 650 millis)
      ), connectionCount = 30)
    implicit val timeout = Timeout(33 seconds)
    implicit val sys = ActorSystem("test")

    dynamo ! ('addListener, sys.actorOf(Props( new Actor {
      def receive = {
        case msg: ProvisionedThroughputExceeded =>
          context.system.log.warning(msg.toString())
      }})))

    //  dynamo ! ('addListener, sys.actorOf(Props(new Actor{
    //    protected def receive = {
    //      case msg:ProvisionedThroughputExceeded => println("EVENT_STREAM: " + msg)
    //    }
    //  })))

    val successCount = new AtomicInteger(0)
    val failureCount = new AtomicInteger(0)

    val N = 12000
    val id = UUID.randomUUID().toString
    createTestObjectsInDb(id, N)
    assert (Query[DynamoTestWithRangeObject](id, "GT", List("0")).blockingStream.size == N )


    def createTestObjectsInDb(id : String, n: Int)  {

      val finished = new CountDownLatch(n)
      val evener = (30 seconds) / n
      (1 to n) map {
        i =>
          nonblocking.Save(DynamoTestWithRangeObject(id, i.toString , "value "+i)).executeOn(dynamo)(10 seconds)
            .andThen{
              case Success(_) => successCount.incrementAndGet()
              case Failure(_) => failureCount.incrementAndGet()
            }
            .onComplete{_ =>
            finished.countDown()
            if (finished.getCount % 50 == 0)
              println("Success count = [%d], Failure count = [%d]"  format (successCount.get(), failureCount.get))
          }
          Thread.sleep(evener toMillis)
      }


      finished.await(n* 10,  TimeUnit.SECONDS)
      println("FINAL: Success count = [%d], Failure count = [%d]"  format (successCount.get(), failureCount.get))

    }


  }

}