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
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import akka.routing.SmallestMailboxRouter
import akka.util.duration._
import akka.util.Duration
import com.typesafe.config.ConfigFactory
import akka.actor.Status.Failure
import com.amazonaws.services.dynamodb.model.ProvisionedThroughputExceededException

class Dynamo(config: DynamoConfig) extends Actor {

  val clientConfig = {
    val c = new ClientConfiguration()
    c.setMaxConnections(1)
    c.setMaxErrorRetry(config.throttlingRecoveryStrategy.amazonMaxErrorRetry)
    c.setConnectionTimeout(config.timeout.toMillis.toInt)
    c.setSocketTimeout(config.timeout.toMillis.toInt)
    c
  }

  val db = new AmazonDynamoDBClient(new BasicAWSCredentials(config.accessKey, config.secret), clientConfig)
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

  private def time[T](f : => T) :( T, Duration) = {
    val start = System.currentTimeMillis()
    val res = f
    val duration = System.currentTimeMillis() - start
    (res, duration.millis)
  }

}

object Dynamo{
  def apply(config: DynamoConfig, connectionCount: Int) = {
    val system = ActorSystem("Dynamo", ConfigFactory.load().getConfig("Dynamo") )

    system.actorOf(Props(new Actor {
      val router = context.actorOf(Props(new Dynamo(config))
        .withRouter(SmallestMailboxRouter(connectionCount))
        .withDispatcher("dynamo-connection-dispatcher"), "DynamoConnection")

      protected def receive = {
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
                         timeout: Duration = 10 seconds,
                         throttlingRecoveryStrategy : ThrottlingRecoveryStrategy = AmazonThrottlingRecoveryStrategy(10)
                         )



class ThirdPartyException(msg: String, cause:Throwable=null) extends RuntimeException(msg, cause)

trait DynamoEvent
case class OperationExecuted(duration:Duration, operation: DbOperation[_]) extends DynamoEvent
case class OperationFailed(operation: DbOperation[_], reason: Throwable) extends DynamoEvent
case class ProvisionedThroughputExceeded(operation: DbOperation[_], msg:String) extends DynamoEvent
case class OperationOverdue(operation: DbOperation[_]) extends DynamoEvent

trait ThrottlingRecoveryStrategy{
  def amazonMaxErrorRetry : Int
  def onExecute[T](f: => T, operation: PendingOperation[T], system : ActorSystem) : T
}

case class AmazonThrottlingRecoveryStrategy(maxRetries: Int) extends ThrottlingRecoveryStrategy{
  val amazonMaxErrorRetry = maxRetries
  def onExecute[T](f: => T, operation: PendingOperation[T], system : ActorSystem) : T = f
}

case class ExpotentialBackoffThrottlingRecoveryStrategy(maxRetries: Int, backoffBase: Duration) extends  ThrottlingRecoveryStrategy{
  val amazonMaxErrorRetry = 0
  lazy val BASE = backoffBase.toMillis

  def onExecute[T](evaluate: => T, operation: PendingOperation[T], system : ActorSystem) : T = {

      def Try(attempt: Int) : T = {
        try evaluate
        catch{
          case ex: ProvisionedThroughputExceededException =>
            val pauseMillis = BASE * math.pow(2, attempt).toInt
            system.eventStream publish ProvisionedThroughputExceeded(operation.operation, "Retrying in [%d] millis. Attempt [%d]" format (pauseMillis, attempt))
            Thread.sleep(pauseMillis)
            if (attempt < maxRetries) Try(attempt+1) else evaluate
        }
      }

      if (maxRetries > 0) Try(attempt = 1) else evaluate


  }
}

