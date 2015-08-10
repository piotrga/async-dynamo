/*
 * Copyright 2012-2015 2ndlanguage Limited.
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
package com.github.piotrga.asyncdynamo

// Scala
import concurrent.duration._
import annotation.tailrec
import language.postfixOps

// Java
import java.util.concurrent.TimeoutException

// ScalaTest
import org.scalatest.FreeSpec
import org.scalatest.matchers.MustMatchers

// This project
import blocking._

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

  val eventually : ( => Boolean) => Unit = eventually(120, 1000)

  "Combination of create/delete table operations" ignore {
    try DeleteTable[AdminTest] catch {case _: Throwable => ()} //ignore if it doesn't exist
    eventually(!TableExists[AdminTest]())

    nonblocking.CreateTable[AdminTest](5,5).blockingExecute(dynamo, 1 minute)
    TableExists[AdminTest]() must be (true)
    IsTableActive[AdminTest]() must be (true)
    DeleteTable[AdminTest]()
    eventually( !TableExists[AdminTest]() )
  }
}
