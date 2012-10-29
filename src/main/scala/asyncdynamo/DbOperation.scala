package asyncdynamo

import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import akka.actor.ActorRef
import akka.util.{Timeout, Duration}
import akka.dispatch.{Await, Future}
import akka.pattern.ask

trait DbOperation[T]{ self =>
  def map[B](g: T => B): DbOperation[B] = (db: AmazonDynamoDBClient, tablePrefix:String) => g(execute(db, tablePrefix))
  def flatMap[B](g: T => DbOperation[B]): DbOperation[B] = (db: AmazonDynamoDBClient, tablePrefix:String) => g(execute(db, tablePrefix)).execute(db, tablePrefix)
  def >>[B](g: => DbOperation[B]): DbOperation[B] = flatMap(_ => g)
  def andThen[B](g: => DbOperation[B]) = >>(g)

  private[asyncdynamo] def execute(db: AmazonDynamoDBClient, tablePrefix:String):T

  def blockingExecute(implicit dynamo: ActorRef, timeout:Timeout): T = {
    Await.result(executeOn(dynamo)(timeout), timeout.duration)
  }

  def executeOn(dynamo: ActorRef)(implicit timeout:Timeout): Future[T] = {
    dynamo.ask(this).map(_.asInstanceOf[T])
  }
}

object DbOperation{
//  implicit def readerToDbOperation[T](r: ReaderMonad[(AmazonDynamoDBClient, String), T]) : DbOperation[T] = new DbOperation[T] {
//    protected def execute(db: AmazonDynamoDBClient, tablePrefix: String) = r.apply((db, tablePrefix))
//  }

}
