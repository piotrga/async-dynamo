package com.zeebox.dynamo


import org.scalatest.FreeSpec
import org.scalatest.matchers.MustMatchers
import akka.util.duration._
import akka.dispatch.Await
import akka.util.Duration
import java.util.concurrent.TimeoutException
import annotation.tailrec

class AdminOperationsTest extends FreeSpec with MustMatchers with DynamoSupport{
  case class AdminTest(id:String, value: String)
  implicit val at1DO = DynamoObject.of2(AdminTest)



  @tailrec
  final def eventually(times:Int, millis:Long)(cond: => Boolean){
    if(! cond ) {
      if (times<0) throw new TimeoutException()
      Thread.sleep(millis)
      eventually(times-1, millis)(cond)
    }
  }

  val eventually : ( => Boolean) => Unit = eventually(60, 1000)

  "Combination of create/delete table operations" in {
    import blocking._
    try DeleteTable[AdminTest] catch {case _ => ()} //ignore if it doesn't exist
    eventually(!TableExists[AdminTest]())

    nonblocking.CreateTable[AdminTest](5,5).blockingExecute(dynamo, 1 minute)
    TableExists[AdminTest]() must be (true)
    IsTableActive[AdminTest]() must be (true)
    DeleteTable[AdminTest]()
    eventually( !TableExists[AdminTest]() )
  }
}
