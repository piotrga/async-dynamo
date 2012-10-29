package asyncdynamo

import akka.actor.ActorRef
import akka.util.Timeout

package object blocking {
  def Read[T](id: String)(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.Read(id).blockingExecute

  def ListAll[T](limit: Int)(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.ListAll(limit).blockingExecute

  def Save[T](o: T)(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.Save(o).blockingExecute

  def DeleteById[T](id: String)(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.DeleteById(id).blockingExecute

  def DeleteAll[T]()(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.DeleteAll().blockingExecute

  def TableExists[T]()(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.TableExists[T]().blockingExecute

  def DeleteTable[T]()(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.DeleteTable[T]().blockingExecute

  def IsTableActive[T]()(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.IsTableActive[T]().blockingExecute

  def CreateTable[T](readThroughput: Long =5, writeThrougput: Long = 5)(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.CreateTable[T](readThroughput, writeThrougput).blockingExecute

}
