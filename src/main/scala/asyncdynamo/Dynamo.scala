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
    c.setMaxErrorRetry(0)
    c.setConnectionTimeout(config.timeout.toMillis.toInt)
    c.setSocketTimeout(config.timeout.toMillis.toInt)
    c
  }

  val db = new AmazonDynamoDBClient(new BasicAWSCredentials(config.accessKey, config.secret), clientConfig)
  db.setEndpoint(config.endpointUrl)

  override def receive = {
    case op:DbOperation[_] =>
      try{
        val (result, duration) = time(retryIfCapacityExceeded(config.maxRetries, backoffBase = config.backoffBase, op))
        sender ! result
        context.system.eventStream.publish(OperationExecuted(duration, op))
      } catch {
        case ex: ProvisionedThroughputExceededException =>
          context.system.eventStream.publish(OperationFailed(op, ex))
          context.system.eventStream.publish(ProvisionedThroughputExceeded(op, "Giving up"))
          throw ex
        case ex: Throwable =>
          context.system.eventStream.publish(OperationFailed(op, ex))
          throw ex
      }
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

  private def retryIfCapacityExceeded[A](maxRetries: Int, backoffBase : Duration, op :DbOperation[A]) : A = {
    val BASE = backoffBase.toMillis
    def retry(attempt: Int) : A = {
      try op.safeExecute(db, config.tablePrefix)
      catch{
        case ex: ProvisionedThroughputExceededException =>
          val pauseMillis = BASE * math.pow(2, attempt).toInt
          context.system.eventStream.publish(ProvisionedThroughputExceeded(op, "Retrying in [%d] millis. Attempt [%d]" format (pauseMillis, attempt)))
          Thread.sleep(pauseMillis)
          if (attempt < maxRetries){
            retry(attempt+1)
          }else{
            op.execute(db, config.tablePrefix)
          }
      }
    }
    retry(attempt = 1)
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
        case 'stop =>
          system.shutdown()
        case ('addListener, listener : ActorRef) =>
          system.eventStream.subscribe(listener, classOf[DynamoEvent])
        case msg: DbOperation[_] =>
          router forward msg
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
                         maxRetries : Int = 3,
                         backoffBase : Duration = 2 seconds
                         )



class ThirdPartyException(msg: String, cause:Throwable=null) extends RuntimeException(msg, cause)

trait DynamoEvent
case class OperationExecuted(duration:Duration, operation: DbOperation[_]) extends DynamoEvent
case class OperationFailed(operation: DbOperation[_], reason: Throwable) extends DynamoEvent
case class ProvisionedThroughputExceeded(operation: DbOperation[_], msg:String) extends DynamoEvent