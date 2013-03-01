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
import com.amazonaws.services.dynamodb.model._
import com.amazonaws.services.dynamodb.AmazonDynamoDB
import com.amazonaws.services.dynamodb.model.AttributeValue
import com.amazonaws.services.dynamodb.model.PutItemRequest
import com.amazonaws.services.dynamodb.model.ScanRequest
import com.amazonaws.services.dynamodb.model.DeleteItemRequest
import com.amazonaws.services.dynamodb.model.Key
import asyncdynamo.{ThirdPartyException, functional, DynamoObject, DbOperation}
import akka.actor.{ActorSystem, ActorRef}
import akka.util.Timeout
import asyncdynamo.functional._
import asyncdynamo.functional.Iteratee._
import concurrent.{ExecutionContext, Future, Promise}
import reflect.ClassTag


case class Save[T ](o : T)(implicit dyn:DynamoObject[T]) extends DbOperation[T]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : T = {
    db.putItem(new PutItemRequest(dyn.table(tablePrefix), dyn.toDynamo(o).asJava))
    o
  }

  override def toString = "Save[%s](%s)" format (dyn.table(""), o)

}

case class Read[T](id:String, consistentRead : Boolean = true)(implicit dyn:DynamoObject[T]) extends DbOperation[Option[T]]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : Option[T] = {

    val read = new GetItemRequest( dyn.table(tablePrefix), new Key().withHashKeyElement(new AttributeValue(id)))
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
      val id = item.get(dyn.key.getAttributeName)
      val key = new Key().withHashKeyElement(id)
      db.deleteItem( new DeleteItemRequest().withTableName(dyn.table(tablePrefix)).withKey(key) )
    }
    res.getCount
  }
}

case class DeleteById[T](id: String)(implicit dyn:DynamoObject[T]) extends DbOperation[Unit]{
  def execute(db: AmazonDynamoDB, tablePrefix:String){
    db.deleteItem( new DeleteItemRequest().withTableName(dyn.table(tablePrefix)).withKey(new Key().withHashKeyElement(new AttributeValue(id))))
  }
  override def toString = "DeleteById[%s](%s)" format (dyn.table(""), id)

}

case class DeleteByRange[T](id: String, range: Any, expected: Map[String,String] = Map.empty)(implicit dyn:DynamoObject[T]) extends DbOperation[Unit]{
  def execute(db: AmazonDynamoDB, tablePrefix:String){
    val rangeAttribute = if (dyn.range.get.getAttributeType == "S")
      new AttributeValue().withS(range.toString)
    else
      new AttributeValue().withN(range.toString)

    val key = new Key()
      .withHashKeyElement(new AttributeValue(id))
      .withRangeKeyElement(rangeAttribute)

    val request = new DeleteItemRequest()
      .withTableName(dyn.table(tablePrefix))
      .withKey(key)
      .withExpected(expected.map{case (k,v)=> (k, new ExpectedAttributeValue(new AttributeValue(v.toString)))}.asJava)

    db.deleteItem( request)
  }

  override def toString = "DeleteByRange[%s](%s)" format (dyn.table(""), super.toString)


}


case class Query[T](id: String, operator: Option[String], attributes: Seq[Any], limit : Int, exclusiveStartKey: Option[Key], consistentRead :Boolean)(implicit dyn:DynamoObject[T]) extends DbOperation[(Seq[T], Option[Key])]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : (Seq[T], Option[Key]) = {


    val query = new QueryRequest()
      .withTableName(dyn.table(tablePrefix))
      .withHashKeyValue(new AttributeValue(id))
      .withExclusiveStartKey(exclusiveStartKey getOrElse null)
      .withConsistentRead(consistentRead)
      .withLimit(limit)
      .withRangeKeyCondition{ operator map ( operator => new Condition()
      .withComparisonOperator(operator)
      .withAttributeValueList(attributes.map(dyn.asRangeAttribute).asJava)) getOrElse null
    }


    val result = db.query(query)
    val items = result.getItems.asScala.map {
      item => dyn.fromDynamo(item.asScala.toMap)
    }

    (items, Option(result.getLastEvaluatedKey))
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

    def nextBatch(token : Option[Key]) = this.copy(exclusiveStartKey = token).executeOn(dynamo)(pageTimeout)

    pageAsynchronously2(nextBatch, iter)(new {def apply[X]()= Promise[X]()}, execCtx)
  }


}

object Query{
  def apply[T](id: String, operator: String = null, attributes: Seq[Any] = Nil, limit : Int = Int.MaxValue, exclusiveStartKey: Key = null, consistentRead :Boolean = true)(implicit dyn:DynamoObject[T]) :Query[T]=
    Query(id, Option(operator), attributes, limit, Option(exclusiveStartKey), consistentRead)
}
