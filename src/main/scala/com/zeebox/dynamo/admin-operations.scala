package com.zeebox.dynamo

import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.services.dynamodb.model._
import akka.dispatch.{Await, Future, Promise}
import akka.actor.{ActorSystem, ActorRef, Scheduler, Actor}
import akka.util.Duration
import akka.util.duration._

case class CreateTable[T](readThroughput: Long =5, writeThrougput: Long = 5)(implicit dyn:DynamoObject[T]) extends DbOperation[Unit]{
  def execute(db: AmazonDynamoDBClient, tablePrefix:String) {

    val hashKey = new KeySchemaElement()
      .withAttributeName(dyn.keyName)
      .withAttributeType(dyn.keyType)

    val ks = if (dyn.keyRange)
      new KeySchema().withRangeKeyElement(hashKey)
    else
      new KeySchema().withHashKeyElement(hashKey)

    val provisionedThroughput = new ProvisionedThroughput()
      .withReadCapacityUnits(readThroughput)
      .withWriteCapacityUnits(writeThrougput)

    val request = new CreateTableRequest()
      .withTableName(dyn.table(tablePrefix))
      .withKeySchema(ks)
      .withProvisionedThroughput(provisionedThroughput)
    //    println("Creating Dynamo table [%s]" format dyn.table(tablePrefix))
    db.createTable(request)
  }

  override def blockingExecute(implicit dynamo: ActorRef, timeout: Duration) {
    Await.ready(this.executeOn(dynamo)(timeout).flatMap{ _ =>
      IsTableActive()(dyn).blockUntilTrue(timeout)
    }, timeout)
  }



}

case class TableExists[T](implicit dyn: DynamoObject[T]) extends DbOperation[Boolean]{
  private[dynamo] def execute(db: AmazonDynamoDBClient, tablePrefix: String) = {
    val tableName = dyn.table(tablePrefix)
    db.listTables().getTableNames.contains(tableName)
  }
}

case class IsTableActive[T](implicit dyn: DynamoObject[T]) extends DbOperation[Boolean]{
  private[dynamo] def execute(db: AmazonDynamoDBClient, tablePrefix: String) = {
    val tableName = dyn.table(tablePrefix)
    if (db.listTables().getTableNames.contains(tableName)){
      val status = db.describeTable(new DescribeTableRequest().withTableName(tableName)).getTable.getTableStatus.toUpperCase()
      status == "ACTIVE"
    }else false
  }


  def blockUntilTrue(timeout:Duration)(implicit dynamo: ActorRef): Future[Unit] = {
    val start = System.currentTimeMillis()
    implicit var sys = ActorSystem("blockUntilTrue") //TODO: this is not very resource-efficient (Peter G. 23/10/2012)
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
  private[dynamo] def execute(db: AmazonDynamoDBClient, tablePrefix: String) {
    db.deleteTable(new DeleteTableRequest().withTableName(dyn.table(tablePrefix)))
  }
}