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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import akka.actor.ActorRef
import akka.util.Timeout
import concurrent.Future
import scala.language.implicitConversions

package object asyncdynamo {

  implicit def toDbOperation[T](f: (AmazonDynamoDB, String) => T): DbOperation[T] = new DbOperation[T] {
    private[asyncdynamo] def execute(db: AmazonDynamoDB, tablePrefix: String) = f(db, tablePrefix)
  }

  implicit def toResult[T](op : DbOperation[T])(implicit dynamo: ActorRef, timeout: Timeout)  : T = op.blockingExecute
  implicit def toFuture[T](op : DbOperation[T])(implicit dynamo: ActorRef, timeout: Timeout)  : Future[T] = op.executeOn(dynamo)(timeout)

}

