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

package asyncdynamo.nonblocking

import scala.collection.JavaConverters._
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.PutItemRequest
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest
import com.amazonaws.services.dynamodbv2
import asyncdynamo.{ThirdPartyException, functional, DynamoObject, DbOperation}
import akka.actor.{ActorSystem, ActorRef}
import akka.util.Timeout
import asyncdynamo.functional._
import asyncdynamo.functional.Iteratee._
import concurrent.{ExecutionContext, Future, Promise}
import reflect.ClassTag
import collection.JavaConversions._

case class Save[T ](o : T)(implicit dyn:DynamoObject[T]) extends DbOperation[T]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : T = {
    db.putItem(new PutItemRequest(dyn.table(tablePrefix), dyn.toDynamo(o).asJava))
    o
  }

  override def toString = "Save[%s](%s)" format (dyn.table(""), o)

}

case class Read[T](id:String, consistentRead : Boolean = true)(implicit dyn:DynamoObject[T]) extends DbOperation[Option[T]]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : Option[T] = {
    val read = new GetItemRequest( dyn.table(tablePrefix), Map(dyn.key.getAttributeName -> new AttributeValue(id)) )
      .withConsistentRead(consistentRead)

    val attributes = db.getItem(read).getItem
    Option (attributes) map ( attr => dyn.fromDynamo(attr.asScala.toMap) )
  }

  override def toString = "Read[%s](id=%s, consistentRead=%s" format (dyn.table(""), id, consistentRead)
}

case class ListAll[T](limit : Int)(implicit dyn:DynamoObject[T]) extends DbOperation[Seq[T]]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : Seq[T] = {
    db.scan(new ScanRequest(dyn.table(tablePrefix)).withLimit(limit)).getItems.asScala.map {
      item => dyn.fromDynamo(item.asScala.toMap)
    }
  }
}

case class DeleteAll[T](implicit dyn:DynamoObject[T]) extends DbOperation[Int]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : Int = {
    if (dyn.range.isDefined) throw new ThirdPartyException("DeleteAll works only for tables without range attribute")
    val res = db.scan(new ScanRequest(dyn.table(tablePrefix)))
    res.getItems.asScala.par.map{ item =>
      val key = Map(dyn.key.getAttributeName -> item.get(dyn.key.getAttributeName))
      db.deleteItem( new DeleteItemRequest().withTableName(dyn.table(tablePrefix)).withKey(key) )
    }
    res.getCount
  }
}

case class DeleteById[T](id: String)(implicit dyn:DynamoObject[T]) extends DbOperation[Unit]{
  def execute(db: AmazonDynamoDB, tablePrefix:String){
    val key = Map(dyn.key.getAttributeName -> new AttributeValue(id))
    db.deleteItem( new DeleteItemRequest().withTableName(dyn.table(tablePrefix)).withKey(key))
  }
  override def toString = "DeleteById[%s](%s)" format (dyn.table(""), id)

}

case class DeleteByRange[T](id: String, range: Any, expected: Map[String,String] = Map.empty)(implicit dyn:DynamoObject[T]) extends DbOperation[Unit]{
  def execute(db: AmazonDynamoDB, tablePrefix:String){
    if (!dyn.range.isDefined) throw new ThirdPartyException("DeleteByRange works only for tables with a range attribute")

    val rangeAttribute = if (dyn.range.get.getKeyType == "S")
      new AttributeValue().withS(range.toString)
    else
      new AttributeValue().withN(range.toString)

    val key = Map(dyn.key.getAttributeName -> new AttributeValue(id), dyn.range.get.getAttributeName -> rangeAttribute)

    val request = new DeleteItemRequest()
      .withTableName(dyn.table(tablePrefix))
      .withKey(key)
      .withExpected(expected.map{case (k,v)=> (k, new ExpectedAttributeValue(new AttributeValue(v.toString)))}.asJava)

    db.deleteItem( request)
  }

  override def toString = "DeleteByRange[%s](%s)" format (dyn.table(""), super.toString)

}

case class Query[T](id: String, operator: Option[String], attributes: Seq[Any], limit : Int, exclusiveStartKey: Option[Map[String,AttributeValue]], consistentRead :Boolean)(implicit dyn:DynamoObject[T]) extends DbOperation[(Seq[T], Option[Map[String,AttributeValue]])]{

  def execute(db: AmazonDynamoDB, tablePrefix:String) : (Seq[T], Option[Map[String,AttributeValue]]) = {

    val hashCondition: Map[String,Condition] = Map(dyn.key.getAttributeName -> new Condition()
      .withComparisonOperator("EQ")
    .withAttributeValueList(new AttributeValue(id))
    )

    val rangeCondition: Map[String,Condition] = operator
      .map( operator => Map(dyn.range.get.getAttributeName -> new Condition()
        .withComparisonOperator(operator)
        .withAttributeValueList(attributes.map(dyn.asRangeAttribute).asJava)))
      .getOrElse(Map[String,Condition]())

    val keyConditions: Map[String,Condition] =
      (hashCondition.keySet ++ rangeCondition.keySet) map { i => i -> (hashCondition.get(i).toList ::: rangeCondition.get(i).toList).head } toMap

    val query = new dynamodbv2.model.QueryRequest()
      .withTableName(dyn.table(tablePrefix))
      .withKeyConditions(keyConditions.asJava)
      .withExclusiveStartKey(exclusiveStartKey getOrElse null)
      .withConsistentRead(consistentRead)
      .withLimit(limit)

    val result = db.query(query)
    val items = result.getItems.asScala.map {
      item => dyn.fromDynamo(item.asScala.toMap)
    }

    val ret: Map[String,AttributeValue] =  result.getLastEvaluatedKey.toMap
    (items, Option(ret))
  }

  def blockingStream(implicit dynamo: ActorRef, pageTimeout: Timeout): Stream[T] = //TODO: use iteratees or some other magic to get rid of this blocking behaviour (Peter G. 31/10/2012)
    functional.unfold[Query[T], Seq[T]](this){
      query =>
        val (resultChunk, lastKey) = query.blockingExecute
        lastKey match{
          case None => (None, resultChunk)
          case key@Some(_) => (Some(query.copy(exclusiveStartKey = key)), resultChunk)
        }
    }.flatten


  def run[A](iter:Iteratee[T,A])(implicit dynamo: ActorRef, pageTimeout: Timeout, execCtx :ExecutionContext) : Future[Iteratee[T,A]] = {

    def nextBatch(token : Option[Map[String,AttributeValue]]) = this.copy(exclusiveStartKey = token).executeOn(dynamo)(pageTimeout)

    pageAsynchronously2(nextBatch, iter)(new {def apply[X]()= Promise[X]()}, execCtx)
  }
}

object Query{
  def apply[T](id: String, operator: String = null, attributes: Seq[Any] = Nil, limit : Int = Int.MaxValue, exclusiveStartKey: Map[String,AttributeValue] = null, consistentRead :Boolean = true)(implicit dyn:DynamoObject[T]) :Query[T]=
    Query(id, Option(operator), attributes, limit, Option(exclusiveStartKey), consistentRead)
}