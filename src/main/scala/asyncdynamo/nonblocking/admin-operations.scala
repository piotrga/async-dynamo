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

package asyncdynamo.nonblocking

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import akka.actor.{ActorSystem, ActorRef}
import akka.util.Timeout
import scala.concurrent.duration._
import asyncdynamo._
import concurrent.{Promise, Future, Await}
import util.{Failure, Success}

import scala.collection.JavaConversions._

case class CreateTable[T](readThroughput: Long = 5, writeThrougput: Long = 5)(implicit dyn:DynamoObject[T]) extends DbOperation[Unit]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) {

    val provisionedThroughput = new ProvisionedThroughput()
      .withReadCapacityUnits(readThroughput)
      .withWriteCapacityUnits(writeThrougput)

    val attributes = List(dyn.key) ++ dyn.range.toSeq

    val keySchema = List(new KeySchemaElement()
      .withAttributeName(dyn.key.getAttributeName)
      .withKeyType(KeyType.HASH)
    ) ++ dyn.range.map(attr =>
      new KeySchemaElement()
        .withAttributeName(attr.getAttributeName())
        .withKeyType(KeyType.RANGE)
    )

    val request = new CreateTableRequest()
      .withTableName(dyn.table(tablePrefix))
      .withProvisionedThroughput(provisionedThroughput)
      .withAttributeDefinitions(attributes)
      .withKeySchema(keySchema)
    db.createTable(request)
  }

  override def blockingExecute(implicit dynamo: ActorRef, timeout: Timeout) {
    val deadline = Deadline.now + timeout.duration
    Await.ready(this.executeOn(dynamo)(timeout), timeout.duration)
    Await.ready(IsTableActive()(dyn).blockUntilTrue(deadline.timeLeft), deadline.timeLeft)
  }



}

case class TableExists[T](implicit dyn: DynamoObject[T]) extends DbOperation[Boolean]{
  private[asyncdynamo] def execute(db: AmazonDynamoDB, tablePrefix: String) = {
    val tableName = dyn.table(tablePrefix)
    db.listTables().getTableNames.contains(tableName)
  }
}

case class IsTableActive[T](implicit dyn: DynamoObject[T]) extends DbOperation[Boolean]{
  private[asyncdynamo] def execute(db: AmazonDynamoDB, tablePrefix: String) = {
    val tableName = dyn.table(tablePrefix)
    if (db.listTables().getTableNames.contains(tableName)){
      val status = db.describeTable(new DescribeTableRequest().withTableName(tableName)).getTable.getTableStatus.toUpperCase()
      status == "ACTIVE"
    }else false
  }


  def blockUntilTrue(timeout:FiniteDuration)(implicit dynamo: ActorRef): Future[Unit] = {
    val start = System.currentTimeMillis()
    implicit val sys = ActorSystem("blockUntilTrue") //TODO: this is not very resource-efficient (Peter G. 23/10/2012)
    implicit val exec = sys.dispatcher
    val promise = Promise[Unit]()

    def schedule() {
      sys.scheduler.scheduleOnce(100 milliseconds){
        if (System.currentTimeMillis() - start < timeout.toMillis) {
          this.executeOn(dynamo)(timeout).onComplete{
            case Success(false) => schedule()
            case Success(true)  => promise.tryComplete(Success(()))
            case Failure(e) => promise.failure(e)
          }
        }
      }
    }

    schedule()

    promise.future.andThen{case _ => sys.shutdown()}
  }

}

case class DeleteTable[T] (implicit dyn:DynamoObject[T]) extends DbOperation[Unit]{
  private[asyncdynamo] def execute(db: AmazonDynamoDB, tablePrefix: String) {
    db.deleteTable(new DeleteTableRequest().withTableName(dyn.table(tablePrefix)))
  }
}
