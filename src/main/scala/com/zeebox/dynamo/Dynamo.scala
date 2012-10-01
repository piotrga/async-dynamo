package com.zeebox.dynamo

import akka.actor.{ActorRef, Actor}
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.auth.BasicAWSCredentials
import akka.dispatch.Future
import com.zeebox.akka.Routers
import akka.util.Duration
import akka.util.duration._

class Dynamo(config: DynamoConfig) extends Actor {
  val db = new AmazonDynamoDBClient(new BasicAWSCredentials(config.accessKey, config.secret))
  db.setEndpoint(config.endpointUrl)

  def receive = {
    case op:DbOperation[_] => self.tryReply(try op.execute(db, config.tablePrefix) catch {case e:Throwable => throw new ThirdPartyException("AmazonDB Error: [%s] while executing [%s]" format (e.getMessage, op), e)})
  }
}

case class DynamoConfig(
                         accessKey : String,
                         secret: String,
                         tablePrefix: String,
                         endpointUrl: String
                         )

trait DbOperation[T]{
  private[dynamo] def execute(db: AmazonDynamoDBClient, tablePrefix:String):T

  def blockingExecute(implicit dynamo: ActorRef, timeout:Duration = 5 seconds): T = {
    executeOn(dynamo).get
  }

  def executeOn(dynamo: ActorRef)(implicit timeout:Duration = 5 seconds): Future[T] = {
    dynamo ask(this, timeout.toMillis) map(_.asInstanceOf[T])
  }
}

class ThirdPartyException(msg: String, cause:Throwable) extends RuntimeException(msg, cause)

object Dynamo{
  def apply(config: DynamoConfig, connectionCount: Int) = {
    val dynamo = Routers.cyclicIteratorLoadBalancer(connectionCount, new Dynamo(config), (_,_)=>())
    dynamo.start()
    dynamo
  }
}