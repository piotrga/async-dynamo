package com.zeebox.dynamo

import akka.actor.Actor
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.auth.BasicAWSCredentials
import com.zeebox.akka.Routers

class Dynamo(config: DynamoConfig) extends Actor {
  val db = new AmazonDynamoDBClient(new BasicAWSCredentials(config.accessKey, config.secret))
  db.setEndpoint(config.endpointUrl)

  def receive = {
    case op:DbOperation[_] => self.tryReply(
      try
        op.execute(db, config.tablePrefix)
      catch {
        case e:Throwable => throw new ThirdPartyException("AmazonDB Error: [%s] while executing [%s]" format (e.getMessage, op), e)
      }
    )
  }
}

object Dynamo{
  def apply(config: DynamoConfig, connectionCount: Int) = {
    val dynamo = Routers.cyclicIteratorLoadBalancer(connectionCount, new Dynamo(config), (_,_)=>())
    dynamo.start()
    dynamo
  }
}
case class DynamoConfig(
                         accessKey : String,
                         secret: String,
                         tablePrefix: String,
                         endpointUrl: String
                         )



class ThirdPartyException(msg: String, cause:Throwable) extends RuntimeException(msg, cause)

