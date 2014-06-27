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

import akka.actor.ActorRef
import akka.util.Timeout
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import asyncdynamo.nonblocking.ColumnCondition

package object blocking {
  def Read[T](id: String, range: Option[String] = None)(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.Read(id,range).blockingExecute

  def Scan[T](conditions: Seq[ColumnCondition], exclusiveStartKey: Option[Map[String,AttributeValue]])(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.Scan(conditions, exclusiveStartKey).blockingExecute

  def Save[T](o: T)(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.Save(o).blockingExecute

  def Update[T](id: String, o: T, range: Option[String] = None)(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.Update(id, o, range).blockingExecute

  def DeleteById[T](id: String, expected: Map[String,String] = Map.empty, retrieveBeforeDelete: Boolean = false)(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.DeleteById(id, expected, retrieveBeforeDelete).blockingExecute

  def DeleteByRange[T](id: String, range: Any, expected: Map[String,String] = Map.empty, retrieveBeforeDelete: Boolean = false)(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.DeleteByRange(id, range, expected, retrieveBeforeDelete).blockingExecute

  def DeleteAll[T]()(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.DeleteAll().blockingExecute

  def TableExists[T]()(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.TableExists[T]().blockingExecute

  def DeleteTable[T]()(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.DeleteTable[T]().blockingExecute

  def IsTableActive[T]()(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.IsTableActive[T]().blockingExecute

  def CreateTable[T](readThroughput: Long =5, writeThrougput: Long = 5)(implicit dynamo: ActorRef, timeout: Timeout, dyn: DynamoObject[T]) = nonblocking.CreateTable[T](readThroughput, writeThrougput).blockingExecute
}
