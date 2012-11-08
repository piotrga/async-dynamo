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

import akka.actor.{Props, ActorSystem, Actor}
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import akka.actor.Status.Failure
import akka.routing.SmallestMailboxRouter
import akka.util.duration._
import akka.util.Duration

class Dynamo(config: DynamoConfig) extends Actor {

  val clientConfig = {
    val c = new ClientConfiguration()
    c.setMaxConnections(1)
    c.setMaxErrorRetry(config.maxRetries)
    c.setConnectionTimeout(config.timeout.toMillis.toInt)
    c.setSocketTimeout(config.timeout.toMillis.toInt)
    c
  }

  val db = new AmazonDynamoDBClient(new BasicAWSCredentials(config.accessKey, config.secret), clientConfig)
  db.setEndpoint(config.endpointUrl)

  override def receive = {
    case op:DbOperation[_] =>
      sender ! op.execute(db, config.tablePrefix)
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    super.preRestart(reason, message)
    sender ! Failure(new ThirdPartyException("AmazonDB Error: [%s] while executing [%s]" format (reason.getMessage, message), reason))
  }
}

object Dynamo{
  def apply(config: DynamoConfig, connectionCount: Int) = {
    val system = ActorSystem("Dynamo")
    system.actorOf(Props(new Actor {
      val router = context.actorOf(Props(new Dynamo(config)).withRouter(SmallestMailboxRouter(connectionCount).withDispatcher("dynamo-connection-dispatcher")).withDispatcher("dynamo-connection-dispatcher"), "DynamoConnection")

      protected def receive = {
        case 'stop =>
          system.shutdown()
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
                         maxRetries : Int = 3
                         )



class ThirdPartyException(msg: String, cause:Throwable=null) extends RuntimeException(msg, cause)

