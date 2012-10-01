package com.zeebox.dynamo

import _root_.akka.actor.Actor
import _root_.akka.actor.ActorRef
import _root_.akka.actor.MaximumNumberOfRestartsWithinTimeRangeReached
import _root_.akka.actor.Supervisor
import _root_.akka.config.Supervision
import _root_.akka.config.Supervision.SupervisorConfig
import _root_.akka.dispatch.Future
import akka.actor.{Supervisor, MaximumNumberOfRestartsWithinTimeRangeReached, ActorRef, Actor}
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.auth.BasicAWSCredentials
import scala.collection.JavaConverters._
import com.amazonaws.services.dynamodb.model._
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodb.model.AttributeValue
import com.amazonaws.services.dynamodb.model.PutItemRequest
import com.amazonaws.services.dynamodb.model.ScanRequest
import com.amazonaws.services.dynamodb.model.DeleteItemRequest
import com.amazonaws.services.dynamodb.model.Key
import akka.dispatch.Future
import collection.immutable
import akka.config.Supervision.SupervisorConfig
import akka.config.Supervision
import akka.routing.{CyclicIterator, LoadBalancer, InfiniteIterator}
import akka.actor.Actor._
import akka.actor.MaximumNumberOfRestartsWithinTimeRangeReached
import com.zeebox.dynamo.DynamoConfig
import akka.config.Supervision.SupervisorConfig
import com.zeebox.akka.Routers

class ThirdPartyException(msg: String, cause:Throwable) extends RuntimeException(msg, cause)

object Dynamo{
  def apply(config: DynamoConfig, connectionCount: Int) = {
    val dynamo = Routers.cyclicIteratorLoadBalancer(connectionCount, new Dynamo(config), (_,_)=>())
    dynamo.start()
    dynamo

  }
}

class Dynamo(config: DynamoConfig) extends Actor {
  val db = new AmazonDynamoDBClient(
    new BasicAWSCredentials(config.accessKey, config.secret))
  db.setEndpoint(config.endpointUrl)

  def receive = {
    case op:DbOperation[_] => self.tryReply(try op.execute(db, config.tablePrefix) catch {case e:Throwable => throw new ThirdPartyException("AmazonDB Error: %s" format e.getMessage, e)})
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    super.preRestart(reason, message)
  }
}

trait DynamoObject[T]{
  protected implicit def toAttribute(value : String) = new AttributeValue().withS(value)

  def toDynamo(t:T) : Map[String, AttributeValue]
  def fromDynamo(attributes: Map[String, AttributeValue]) : T
  protected def table : String
  def table(prefix: String): String = prefix + table
  def keyName: String = "id"
}

case class DynamoConfig(
                         accessKey : String,
                         secret: String,
                         tablePrefix: String,
                         endpointUrl: String
                         )

trait DbOperation[T]{
  private[dynamo] def execute(db: AmazonDynamoDBClient, tablePrefix:String):T

  def blockingExecute(implicit dynamo:ActorRef): T = {
    (dynamo ? this).get.asInstanceOf[T]
  }

  def executeOn(implicit dynamo:ActorRef): Future[T] = {
    (dynamo ? this).map(_.asInstanceOf[T])
  }

}
case class Save[T : DynamoObject](o : T) extends DbOperation[T]{
  def execute(db: AmazonDynamoDBClient, tablePrefix:String) : T = {
    val dyn = implicitly[DynamoObject[T]]
    db.putItem(new PutItemRequest(dyn.table(tablePrefix), dyn.toDynamo(o).asJava))
    o
  }
}

case class ListAll[T](limit : Int)(implicit dyn:DynamoObject[T]) extends DbOperation[Seq[T]]{
  def execute(db: AmazonDynamoDBClient, tablePrefix:String) : Seq[T] = {
    db.scan(new ScanRequest(dyn.table(tablePrefix)).withLimit(limit)).getItems.asScala.map {
      item => dyn.fromDynamo(item.asScala.toMap)
    }
  }
}

case class Read[T](id:String)(implicit dyn:DynamoObject[T]) extends DbOperation[Option[T]]{
  def execute(db: AmazonDynamoDBClient, tablePrefix:String) : Option[T] = {
    val attributes = db.getItem(new GetItemRequest(dyn.table(tablePrefix), new Key().withHashKeyElement(new AttributeValue(id)))).getItem
    Option (attributes) map ( attr => dyn.fromDynamo(attr.asScala.toMap) )
  }
}

case class DeleteAll[T](implicit dyn:DynamoObject[T]) extends DbOperation[Int]{
  def execute(db: AmazonDynamoDBClient, tablePrefix:String) : Int = {
    val res = db.scan(new ScanRequest(dyn.table(tablePrefix)))
    res.getItems.asScala.par.map{ item =>
      val id = item.get(dyn.keyName)
      println("Deleting item [%s] from [%s]" format(id, dyn.table(tablePrefix)))
      db.deleteItem( new DeleteItemRequest().withTableName(dyn.table(tablePrefix)).withKey(new Key().withHashKeyElement(id)) )
    }
    res.getCount
  }
}
