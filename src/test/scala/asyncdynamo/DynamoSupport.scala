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

import nonblocking.{CreateTable, TableExists}
import org.scalatest.{BeforeAndAfterAll, Suite}
import akka.util.Timeout
import scala.concurrent.duration._
import com.amazonaws.services.dynamodbv2.model.AttributeValue

object DynamoTestDataObjects{
  case class DynamoTestObject(id:String, someValue:String)

  implicit object DynamoTestDO extends DynamoObject[DynamoTestObject]{
    def toDynamo(t: DynamoTestObject) = Map("id"->t.id, "someValue"->t.someValue)
    def fromDynamo(a: Map[String, AttributeValue]) = DynamoTestObject(a("id").getS, a("someValue").getS)
    protected val table = "%s_dynamotest" format Option(System.getenv("USER")).getOrElse("unknown")
  }

  case class DynamoTestWithRangeObject(id:String, rangeValue:String, otherValue: String)
  implicit object DynamoTestWithRangeDO extends DynamoObject[DynamoTestWithRangeObject]{
    def toDynamo(t: DynamoTestWithRangeObject) = Map("id"->t.id, "rangeValue"->t.rangeValue, "otherValue" -> t.otherValue)
    def fromDynamo(a: Map[String, AttributeValue]) = DynamoTestWithRangeObject(a("id").getS, a("rangeValue").getS, a("otherValue").getS)
    protected val table = "%s_dynamotest_withrange" format Option(System.getenv("USER")).getOrElse("unknown")
    override val range = Some(defineAttribute("rangeValue", "S"))
  }

  case class DynamoTestWithNumericRangeObject(id:String, rangeValue:Int, otherValue: String)
  implicit object DynamoTestWithNumericRangeObjectDO extends DynamoObject[DynamoTestWithNumericRangeObject]{
    def toDynamo(t: DynamoTestWithNumericRangeObject) = Map("id"->t.id, "rangeValue"->toN(t.rangeValue), "otherValue" -> t.otherValue)
    def fromDynamo(a: Map[String, AttributeValue]) = DynamoTestWithNumericRangeObject(a("id").getS, a("rangeValue").getN.toInt, a("otherValue").getS)
    protected val table = "%s_dynamotest_with_numeric_range" format Option(System.getenv("USER")).getOrElse("unknown")
    override val range = Some(defineAttribute("rangeValue", "N"))
  }

  case class Broken(id:String)
  implicit object BrokenDO extends DynamoObject[Broken]{
    def toDynamo(t: Broken) = Map()
    def fromDynamo(a: Map[String, AttributeValue]) = Broken("wiejfi")
    protected def table = "nonexistenttable"
  }
}

trait DynamoSupport extends BeforeAndAfterAll{ self : Suite =>
  implicit val dynamo = Dynamo(DynamoConfig(System.getProperty("amazon.accessKey"), System.getProperty("amazon.secret"), tablePrefix = "devng_", endpointUrl = System.getProperty("dynamo.url", "https://dynamodb.eu-west-1.amazonaws.com" )), connectionCount = 4)
  implicit val timeout = Timeout(10 seconds)

  override protected def afterAll() {
    dynamo ! 'stop
    super.afterAll()
  }
}

trait DynamoTestObjectSupport extends DynamoSupport{ self : Suite =>
  import DynamoTestDataObjects._

  protected def createTables() {
    println("Creating test tables... It might take a while...")
    if (!TableExists[DynamoTestObject]().blockingExecute){
      CreateTable[DynamoTestObject]().blockingExecute(dynamo,1 minute)
    }

    if (!TableExists[DynamoTestWithRangeObject]().blockingExecute){
      CreateTable[DynamoTestWithRangeObject](100, 100).blockingExecute(dynamo,1 minute)
    }

    if (!TableExists[DynamoTestWithNumericRangeObject]().blockingExecute){
      CreateTable[DynamoTestWithNumericRangeObject](100, 100).blockingExecute(dynamo,1 minute)
    }
  }
}


