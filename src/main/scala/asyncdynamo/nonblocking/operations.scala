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
import asyncdynamo.{ThirdPartyException, functional, DynamoObject, DbOperation}
import akka.actor.{ActorSystem, ActorRef}
import akka.util.Timeout
import asyncdynamo.functional._
import asyncdynamo.functional.Iteratee._
import concurrent.{ExecutionContext, Future, Promise}
import reflect.ClassTag


object operations {
  type Key = Map[String, AttributeValue]
}
import operations._

case class Save[T ](o : T)(implicit dyn:DynamoObject[T], returnConsumedCapacity: String) extends DbOperation[T]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : T = {
    db.putItem(new PutItemRequest(dyn.table(tablePrefix), dyn.toDynamo(o).asJava).withReturnConsumedCapacity(returnConsumedCapacity))
    o
  }

  override def toString = "Save[%s](%s)" format (dyn.table(""), o) // TODO why "" ?

}

case class Read[T](id:String, consistentRead : Boolean = true)(implicit dyn:DynamoObject[T], returnConsumedCapacity: String) extends DbOperation[Option[T]]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : Option[T] = {

    // TODO not handling composite primary keys here?
    val read = new GetItemRequest( dyn.table(tablePrefix), Map(dyn.key.keySchema.getAttributeName -> new AttributeValue(id)).asJava)
      .withConsistentRead(consistentRead)
      .withReturnConsumedCapacity(returnConsumedCapacity)

    val attributes = db.getItem(read).getItem
    Option (attributes) map ( attr => dyn.fromDynamo(attr.asScala.toMap) )
  }

  override def toString = "Read[%s](id=%s, consistentRead=%s" format (dyn.table(""), id, consistentRead)
}

case class ListAll[T](limit : Int)(implicit dyn:DynamoObject[T], returnConsumedCapacity: String) extends DbOperation[Seq[T]]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : Seq[T] = {
    db.scan(new ScanRequest(dyn.table(tablePrefix)).withLimit(limit).withReturnConsumedCapacity(returnConsumedCapacity)).getItems.asScala.map {
      item => dyn.fromDynamo(item.asScala.toMap)
    }
  }
}

case class DeleteAll[T](implicit dyn:DynamoObject[T], returnConsumedCapacity: String) extends DbOperation[Int]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : Int = {
    if (dyn.range.isDefined) throw new ThirdPartyException("DeleteAll works only for tables without range attribute")
    val res = db.scan(new ScanRequest(dyn.table(tablePrefix)))
    res.getItems.asScala.par.map{ item =>
      val id = item.get(dyn.key.keySchema.getAttributeName)
      db.deleteItem( new DeleteItemRequest(dyn.table(tablePrefix), Map(dyn.key.keySchema.getAttributeName -> new AttributeValue(id.getS)).asJava).withReturnConsumedCapacity(returnConsumedCapacity) )
    }
    res.getCount
  }
}

case class DeleteById[T](id: String)(implicit dyn:DynamoObject[T], returnConsumedCapacity: String) extends DbOperation[Unit]{
  def execute(db: AmazonDynamoDB, tablePrefix:String){
    db.deleteItem( new DeleteItemRequest(dyn.table(tablePrefix), Map(dyn.key.keySchema.getAttributeName -> new AttributeValue(id)).asJava).withReturnConsumedCapacity(returnConsumedCapacity) )
  }
  override def toString = "DeleteById[%s](%s)" format (dyn.table(""), id)
}

case class DeleteByRange[T](id: String, range: Any, expected: Map[String,String] = Map.empty)(implicit dyn:DynamoObject[T], returnConsumedCapacity: String) extends DbOperation[Unit]{
  def execute(db: AmazonDynamoDB, tablePrefix:String){
    // TODO fix
    val rangeAttribute = if (dyn.range.isDefined)
      new AttributeValue().withS(range.toString)
    else
      new AttributeValue().withN(range.toString)

    val request = new DeleteItemRequest()
      .withTableName(dyn.table(tablePrefix))
      .withKey(Map(dyn.key.keySchema.getAttributeName -> new AttributeValue(id), dyn.range.get.keySchema.getAttributeName -> rangeAttribute).asJava)
      .withExpected(expected.map{case (k,v)=> (k, new ExpectedAttributeValue(new AttributeValue(v.toString)))}.asJava)
      .withReturnConsumedCapacity(returnConsumedCapacity)

    db.deleteItem( request)
  }

  override def toString = "DeleteByRange[%s](%s)" format (dyn.table(""), super.toString)
}


case class Query[T](id: String, operator: Option[ComparisonOperator], attributes: Seq[Any], limit : Int, exclusiveStartKey: Option[Key], consistentRead :Boolean)(implicit dyn:DynamoObject[T], returnConsumedCapacity: String) extends DbOperation[(Seq[T], Option[Key])]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : (Seq[T], Option[Key]) = {

    val hashKeyCondition = new Condition()
      .withComparisonOperator(ComparisonOperator.EQ.toString()) // hash key must be EQ
      .withAttributeValueList(new AttributeValue().withS(id))
//    keyConditions.put("Id", hashKeyCondition); TODO why was key attribute name hardcoded?
    var keyConditions = Map[String, Condition](dyn.key.keySchema.getAttributeName -> hashKeyCondition)
    if (dyn.range.isDefined) { // TODO better way?
      val rangeKeyCondition = operator.map { operator =>
        new Condition()
          .withComparisonOperator(operator.toString)
          .withAttributeValueList(attributes.map(dyn.asRangeAttribute).asJava) // TODO fix
      } getOrElse(null)
      keyConditions += (dyn.range.get.keySchema.getAttributeName -> rangeKeyCondition)
    }

    val query = new QueryRequest()
      .withTableName(dyn.table(tablePrefix))
      .withKeyConditions(keyConditions.asJava)
      .withExclusiveStartKey(if (exclusiveStartKey.isDefined) exclusiveStartKey.get.asJava else null) // TODO better way?
      .withConsistentRead(consistentRead)
      .withReturnConsumedCapacity(returnConsumedCapacity)
      .withLimit(limit)

    val result = db.query(query)
    val items = result.getItems.asScala.map {
      item => dyn.fromDynamo(item.asScala.toMap)
    }

    (items, Option(if (result.getLastEvaluatedKey != null) result.getLastEvaluatedKey.asScala.toMap else null))
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
  def apply[T](id: String, operator: ComparisonOperator = null, attributes: Seq[Any] = Nil, limit : Int = Int.MaxValue, exclusiveStartKey: Key = null, consistentRead :Boolean = true)(implicit dyn:DynamoObject[T], returnConsumedCapacity: String) :Query[T]=
    Query(id, Option(operator), attributes, limit, Option(exclusiveStartKey), consistentRead)
}
