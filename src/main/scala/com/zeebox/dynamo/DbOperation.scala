package com.zeebox.dynamo

import com.zeebox.functional.ReaderMonad
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import akka.actor.ActorRef
import akka.util.Duration
import akka.dispatch.Future

trait DbOperation[T]{ self =>
  def map[B](g: T => B): DbOperation[B] = (db: AmazonDynamoDBClient, tablePrefix:String) => g(execute(db, tablePrefix))
  def flatMap[B](g: T => DbOperation[B]): DbOperation[B] = (db: AmazonDynamoDBClient, tablePrefix:String) => g(execute(db, tablePrefix)).execute(db, tablePrefix)

  private[dynamo] def execute(db: AmazonDynamoDBClient, tablePrefix:String):T

  def blockingExecute(implicit dynamo: ActorRef, timeout:Duration): T = {
    executeOn(dynamo).get
  }

  def executeOn(dynamo: ActorRef)(implicit timeout:Duration): Future[T] = {
    dynamo ask(this, timeout.toMillis) map(_.asInstanceOf[T])
  }
}

object DbOperation{
//  implicit def readerToDbOperation[T](r: ReaderMonad[(AmazonDynamoDBClient, String), T]) : DbOperation[T] = new DbOperation[T] {
//    protected def execute(db: AmazonDynamoDBClient, tablePrefix: String) = r.apply((db, tablePrefix))
//  }

  implicit def toDbOperation[T](f : ( AmazonDynamoDBClient, String) => T) : DbOperation[T] = new DbOperation[T] {
    private[dynamo] def execute(db: AmazonDynamoDBClient, tablePrefix: String) = f(db, tablePrefix)
  }
}
