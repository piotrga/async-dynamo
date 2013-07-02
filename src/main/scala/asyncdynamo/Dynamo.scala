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

import akka.actor._
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import akka.routing.SmallestMailboxRouter
import com.typesafe.config.ConfigFactory
import com.amazonaws.services.dynamodbv2.model._
import akka.actor.Status.Failure
import asyncdynamo.Operation.Type
import concurrent.duration._

class Dynamo(config: DynamoConfig) extends Actor {

  private val clientConfig = {
    val c = new ClientConfiguration()
    c.setMaxConnections(1)
    c.setMaxErrorRetry(config.throttlingRecoveryStrategy.amazonMaxErrorRetry)
    c.setConnectionTimeout(config.timeout.toMillis.toInt)
    c.setSocketTimeout(config.timeout.toMillis.toInt)
    c
  }

  private val delegate = if (!config.accessKey.isEmpty)
    new AmazonDynamoDBClient(new BasicAWSCredentials(config.accessKey, config.secret), clientConfig)
  else
    new AmazonDynamoDBClient(clientConfig)

  private val db = new TracingAmazonDynamoDB(delegate, context.system.eventStream)

  db.setEndpoint(config.endpointUrl)

  override def receive = {
    case pending @ PendingOperation(op, deadline) if (deadline.hasTimeLeft()) =>
      try{
        val (result, duration) = time(config.throttlingRecoveryStrategy.onExecute( op.safeExecute(db, config.tablePrefix), pending, context.system ))
        sender ! result
        context.system.eventStream publish OperationExecuted(duration, op)
      } catch {
        case ex: ProvisionedThroughputExceededException =>
          context.system.eventStream publish OperationFailed(op, ex)
          context.system.eventStream publish ProvisionedThroughputExceeded(op, "Giving up")
          throw ex
        case ex: Throwable =>
          context.system.eventStream publish OperationFailed(op, ex)
          throw ex
      }
    case overdue @ PendingOperation(operation, _) => context.system.eventStream publish OperationOverdue(operation)
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    super.preRestart(reason, message)
    sender ! Failure(new ThirdPartyException("AmazonDB Error: [%s] while executing [%s]" format (reason.getMessage, message), reason))
  }

  private def time[T](f : => T) :( T, FiniteDuration) = {
    val start = System.currentTimeMillis()
    val res = f
    val duration = System.currentTimeMillis() - start
    (res, duration.millis)
  }

}

object Dynamo {
  def apply(config: DynamoConfig, connectionCount: Int) : ActorRef = {
    val system = ActorSystem("Dynamo", ConfigFactory.load().getConfig("Dynamo") )

    system.actorOf(Props(new Actor {
      val router = context.actorOf(Props(new Dynamo(config))
        .withRouter(SmallestMailboxRouter(connectionCount))
        .withDispatcher("dynamo-connection-dispatcher"), "DynamoConnection")

      def receive = {
        case overdue @ PendingOperation(op, deadline) if (deadline.isOverdue()) =>
          system.eventStream publish OperationOverdue(op)
        case msg: PendingOperation[_] =>
          router forward msg
        case 'stop =>
          system.shutdown()
        case ('addListener, listener : ActorRef) =>
          system.eventStream.subscribe(listener, classOf[DynamoEvent])
        case _ => () // ignore other messages
      }
    }), "DynamoClient")
  }
}

case class DynamoConfig(
                         accessKey : String,
                         secret: String,
                         tablePrefix: String,
                         endpointUrl: String,
                         timeout: FiniteDuration = 10 seconds,
                         throttlingRecoveryStrategy : ThrottlingRecoveryStrategy = AmazonThrottlingRecoveryStrategy.forTimeout(10 seconds)
                         )



class ThirdPartyException(msg: String, cause:Throwable=null) extends RuntimeException(msg, cause)

trait DynamoEvent

case class DynamoRequestExecuted(operation:Operation, readUnits: Double = 0 , writeUnits: Double =0, time : Long = System.currentTimeMillis(), duration : Long) extends DynamoEvent
case class OperationExecuted(duration:FiniteDuration, operation: DbOperation[_]) extends DynamoEvent
case class OperationFailed(operation: DbOperation[_], reason: Throwable) extends DynamoEvent
case class ProvisionedThroughputExceeded(operation: DbOperation[_], msg:String) extends DynamoEvent
case class OperationOverdue(operation: DbOperation[_]) extends DynamoEvent

object Operation{
  sealed trait Type
  case object Read extends Type
  case object Write extends Type
  case object Admin extends Type
}

case class Operation(tableName: String, operationType : Type, name : String)


