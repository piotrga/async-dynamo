package com.zeebox.dynamo

import org.scalatest.FreeSpec
import org.scalatest.matchers.MustMatchers
import akka.util.duration._
import akka.dispatch.Await
import akka.util.Duration
import java.util.concurrent.TimeoutException
import annotation.tailrec

class AdminOperationsTest extends FreeSpec with MustMatchers with DynamoSupport{
  import DbOperation._
  case class AdminTest(id:String, value: String)
  implicit val at1DO = DynamoObject.of2(AdminTest)

  @tailrec
  final def activeWait(times:Int, millis:Long, cond: => Boolean){
    if(! cond ) {
      if (times<0) throw new TimeoutException()
      Thread.sleep(millis)
      activeWait(times-1, millis, cond)
    }
  }

  "Combination of create/delete table operations" in {
    try DeleteTable[AdminTest].blockingExecute catch {case _ => ()} //ignore if it doesn't exist
    activeWait(60, 1000, !TableExists[AdminTest]().blockingExecute)

    CreateTable[AdminTest](5,5).blockingExecute(dynamo, 1 minute)
    TableExists[AdminTest]().blockingExecute must be (true)
    IsTableActive[AdminTest]().blockingExecute must be (true)
    DeleteTable[AdminTest]().blockingExecute
    activeWait(60, 1000, !TableExists[AdminTest]().blockingExecute)
  }
}
