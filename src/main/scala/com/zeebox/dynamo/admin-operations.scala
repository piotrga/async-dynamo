package com.zeebox.dynamo

import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.services.dynamodb.model._
import akka.dispatch.{Future, CompletableFuture, Promise}
import akka.actor.{ActorRef, Scheduler, Actor}
import java.util.concurrent.TimeUnit
import akka.util.Duration

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

    db.createTable(request)
  }

  override def blockingExecute(implicit dynamo: ActorRef, timeout: Duration) {
    this.executeOn(dynamo)(timeout).flatMap{ _ =>
      IsTableActive()(dyn).blockUntilTrue(timeout)
    }.get
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
    val promise = Promise[Unit](timeout.toMillis)

    def schedule() {
      Scheduler.scheduleOnce(() => {
        if (!promise.isExpired) {
          this.executeOn(dynamo)(timeout).map{ active =>
            if (active) promise.completeWithResult(true)
            else schedule()
          }.recover{case e => promise.completeWithException(e) }
        }
      }, 100, TimeUnit.MILLISECONDS)
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