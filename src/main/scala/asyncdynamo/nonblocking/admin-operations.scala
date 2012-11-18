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

import com.amazonaws.services.dynamodb.AmazonDynamoDB
import com.amazonaws.services.dynamodb.model._
import akka.dispatch.{Await, Future, Promise}
import akka.actor.{ActorSystem, ActorRef, Scheduler, Actor}
import akka.util.{Timeout, Duration}
import akka.util.duration._
import asyncdynamo._

case class CreateTable[T](readThroughput: Long =5, writeThrougput: Long = 5)(implicit dyn:DynamoObject[T]) extends DbOperation[Unit]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) {


    val keySchema = dyn.range match {
      case Some((range)) =>
        new KeySchema().withHashKeyElement(dyn.key)
        .withRangeKeyElement(range)
      case None =>
        new KeySchema().withHashKeyElement(dyn.key)
    }

    val provisionedThroughput = new ProvisionedThroughput()
      .withReadCapacityUnits(readThroughput)
      .withWriteCapacityUnits(writeThrougput)

    val request = new CreateTableRequest()
      .withTableName(dyn.table(tablePrefix))
      .withKeySchema(keySchema)
      .withProvisionedThroughput(provisionedThroughput)
    db.createTable(request)
  }

  override def blockingExecute(implicit dynamo: ActorRef, timeout: Timeout) {
    Await.ready(this.executeOn(dynamo)(timeout).flatMap{ _ =>
      IsTableActive()(dyn).blockUntilTrue(timeout.duration)
    }, timeout.duration)
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


  def blockUntilTrue(timeout:Duration)(implicit dynamo: ActorRef): Future[Unit] = {
    val start = System.currentTimeMillis()
    implicit val sys = ActorSystem("blockUntilTrue") //TODO: this is not very resource-efficient (Peter G. 23/10/2012)
    val promise = Promise[Unit]().onComplete(_ => sys.shutdown())

    def schedule() {
      sys.scheduler.scheduleOnce(100 milliseconds){
        if (System.currentTimeMillis() - start < timeout.toMillis) {
          this.executeOn(dynamo)(timeout).onComplete{
            case Right(false) => schedule()
            case Right(true)  => promise.tryComplete(Right(true))
            case Left(e) => promise.failure(e)
          }
        }
      }
    }

    schedule()

    promise
  }

}

case class DeleteTable[T] (implicit dyn:DynamoObject[T]) extends DbOperation[Unit]{
  private[asyncdynamo] def execute(db: AmazonDynamoDB, tablePrefix: String) {
    db.deleteTable(new DeleteTableRequest().withTableName(dyn.table(tablePrefix)))
  }
}