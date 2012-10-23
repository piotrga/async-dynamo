package com.zeebox.dynamo

import akka.actor.{Props, ActorSystem, Kill, Actor}
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.auth.BasicAWSCredentials
import akka.actor.Status.Failure
import akka.routing.RoundRobinRouter

class Dynamo(config: DynamoConfig) extends Actor {
  val db = new AmazonDynamoDBClient(new BasicAWSCredentials(config.accessKey, config.secret))
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
      val router = context.actorOf(Props(new Dynamo(config)), "DynamoConnection")
//      val router = context.actorOf(Props(new Dynamo(config)).withRouter(RoundRobinRouter(connectionCount)), "DynamoConnection")

      protected def receive = {
        case Kill =>
          system.shutdown()
        case msg =>
          router forward msg
      }
    }))
  }
}

case class DynamoConfig(
                         accessKey : String,
                         secret: String,
                         tablePrefix: String,
                         endpointUrl: String
                         )



class ThirdPartyException(msg: String, cause:Throwable) extends RuntimeException(msg, cause)

