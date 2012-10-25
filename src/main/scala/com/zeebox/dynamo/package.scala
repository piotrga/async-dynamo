package com.zeebox

import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import akka.actor.ActorRef
import akka.util.{Timeout, Duration}
import dynamo.{DynamoObject, nonblocking}
import akka.dispatch.Future

package object dynamo {

  implicit def toDbOperation[T](f: (AmazonDynamoDBClient, String) => T): DbOperation[T] = new DbOperation[T] {
    private[dynamo] def execute(db: AmazonDynamoDBClient, tablePrefix: String) = f(db, tablePrefix)
  }

  implicit def toResult[T](op : DbOperation[T])(implicit dynamo: ActorRef, timeout: Timeout)  : T = op.blockingExecute
  implicit def toFuture[T](op : DbOperation[T])(implicit dynamo: ActorRef, timeout: Timeout)  : Future[T] = op.executeOn(dynamo)(timeout)

}

