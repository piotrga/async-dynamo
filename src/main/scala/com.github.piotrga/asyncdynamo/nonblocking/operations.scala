/*
 * Copyright 2012-2015 2ndlanguage Limited.
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
package com.github.piotrga.asyncdynamo
package nonblocking

import scala.collection.JavaConverters._
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2
import akka.actor.{ActorRef}
import akka.util.Timeout

import concurrent.{ExecutionContext, Future, Promise}
import collection.JavaConversions._
import scala.Some
import scala.collection.JavaConversions._

// This project
import functional._
import functional.Iteratee._

case class Save[T ](o : T, overwriteExisting: Boolean = true)(implicit dyn:DynamoObject[T]) extends DbOperation[T]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : T = {
    val putRequest = new PutItemRequest(dyn.table(tablePrefix), dyn.toDynamo(o).asJava)
      .withReturnConsumedCapacity("TOTAL")

    if (!overwriteExisting) {
      val expectedValsMap = dyn.rangeAttrib
        .map( rangeValue => Map(
          dyn.hashSchema.getAttributeName -> new ExpectedAttributeValue(false),
          dyn.rangeSchema.get.getAttributeName -> new ExpectedAttributeValue(false)))
        .getOrElse( Map(dyn.hashSchema.getAttributeName -> new ExpectedAttributeValue(false)))

      putRequest.setExpected(expectedValsMap.asJava)
    }

    db.putItem(putRequest)
    o
  }

  override def toString = "Save[%s](%s)" format (dyn.table(""), o)
}

case class Update[T ](id:String, o : T, range: Option[String] = None)(implicit dyn:DynamoObject[T]) extends DbOperation[T]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : T = {

    val keyAttribs = range
      .map( rangeValue => Map(
          dyn.hashSchema.getAttributeName -> new AttributeValue(id),
          dyn.rangeSchema.get.getAttributeName -> new AttributeValue(rangeValue)))
      .getOrElse( Map(dyn.hashSchema.getAttributeName -> new AttributeValue(id)))

    val attribsMinusHashAndRangeKey = dyn.toDynamo(o).filter( attribPair => {
      attribPair._1 != dyn.hashSchema.getAttributeName &&
      (if (dyn.rangeSchema.isDefined) attribPair._1 != dyn.rangeSchema.get.getAttributeName else true)
    })
    val convertToAttribUpdates: Map[String,AttributeValueUpdate] = attribsMinusHashAndRangeKey.map( attribPair => (attribPair._1, new AttributeValueUpdate(attribPair._2, AttributeAction.PUT) ))

    val results = db.updateItem(new UpdateItemRequest(dyn.table(tablePrefix), keyAttribs.asJava, convertToAttribUpdates.asJava).withReturnConsumedCapacity("TOTAL").withReturnValues(ReturnValue.ALL_NEW))
    dyn.fromDynamo(results.getAttributes.toMap)
  }

  override def toString = "Update[%s](%s)" format (dyn.table(""), o)
}

case class Read[T](id:String, range: Option[String] = None, consistentRead : Boolean = true)(implicit dyn:DynamoObject[T]) extends DbOperation[Option[T]]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : Option[T] = {

    val keyAttribs = range
      .map( rangeValue => Map(
          dyn.hashSchema.getAttributeName -> new AttributeValue(id),
          dyn.rangeSchema.get.getAttributeName -> new AttributeValue(rangeValue)))
      .getOrElse( Map(dyn.hashSchema.getAttributeName -> new AttributeValue(id)))

    val read = new GetItemRequest( dyn.table(tablePrefix), keyAttribs )
      .withConsistentRead(consistentRead)
      .withReturnConsumedCapacity("TOTAL")

    val attributes = db.getItem(read).getItem
    Option (attributes) map ( attr => dyn.fromDynamo(attr.asScala.toMap) )
  }

  override def toString = "Read[%s](id=%s, consistentRead=%s" format (dyn.table(""), id, consistentRead)
}

case class DeleteAll[T](implicit dyn:DynamoObject[T]) extends DbOperation[Int]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : Int = {
    if (dyn.rangeSchema.isDefined) throw new ThirdPartyException("DeleteAll works only for tables without range attribute")
    val res = db.scan(new ScanRequest(dyn.table(tablePrefix)).withReturnConsumedCapacity("TOTAL"))
    res.getItems.asScala.par.map{ item =>
      val key = Map(dyn.hashSchema.getAttributeName -> item.get(dyn.hashSchema.getAttributeName))
      db.deleteItem( new DeleteItemRequest().withTableName(dyn.table(tablePrefix)).withKey(key).withReturnConsumedCapacity("TOTAL") )
    }
    res.getCount
  }
}

case class DeleteById[T](id: String, expected: Map[String,String] = Map.empty, retrieveBeforeDelete: Boolean = false)(implicit dyn:DynamoObject[T]) extends DbOperation[Option[T]]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : Option[T] = {
    val key = Map(dyn.hashSchema.getAttributeName -> new AttributeValue(id))

    val request = new DeleteItemRequest()
      .withTableName(dyn.table(tablePrefix))
      .withKey(key)
      .withReturnConsumedCapacity("TOTAL")
      .withExpected(expected.map{case (k,v)=> (k, new ExpectedAttributeValue(new AttributeValue(v.toString)))}.asJava)

    if (retrieveBeforeDelete)
      request.withReturnValues(ReturnValue.ALL_OLD)

    val out= db.deleteItem(request)

    if (retrieveBeforeDelete)
      Option (out.getAttributes) map ( attr => dyn.fromDynamo(attr.asScala.toMap) )
    else
      None
  }

  override def toString = "DeleteById[%s](%s)" format (dyn.table(""), id)
}

case class DeleteByRange[T](id: String, range: Any, expected: Map[String,String] = Map.empty, retrieveBeforeDelete: Boolean = false)(implicit dyn:DynamoObject[T]) extends DbOperation[Option[T]]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : Option[T] = {
    if (!dyn.rangeSchema.isDefined) throw new ThirdPartyException("DeleteByRange works only for tables with a range attribute")

    val key = Map(dyn.hashSchema.getAttributeName -> dyn.asHashAttribute(id), dyn.rangeSchema.get.getAttributeName -> dyn.asRangeAttribute(range))

    val request = new DeleteItemRequest()
      .withTableName(dyn.table(tablePrefix))
      .withKey(key)
      .withReturnConsumedCapacity("TOTAL")
      .withExpected(expected.map{case (k,v)=> (k, new ExpectedAttributeValue(new AttributeValue(v.toString)))}.asJava)

    if (retrieveBeforeDelete)
      request.withReturnValues(ReturnValue.ALL_OLD)

    val out= db.deleteItem(request)

    if (retrieveBeforeDelete)
      Option (out.getAttributes) map ( attr => dyn.fromDynamo(attr.asScala.toMap) )
    else
      None
  }

  override def toString = "DeleteByRange[%s](%s)" format (dyn.table(""), super.toString)

}

case class BatchDeleteById[T](idAndRangePairs: Seq[Tuple2[String,Option[Any]]])(implicit dyn:DynamoObject[T]) extends DbOperation[Unit]{
  def execute(db: AmazonDynamoDB, tablePrefix:String){

    val threadList = for (idAndRangePair <- idAndRangePairs) yield {

      val key = idAndRangePair._2 match {
        case Some(rangeValue) => Map(dyn.hashSchema.getAttributeName -> dyn.asHashAttribute(idAndRangePair._1), dyn.rangeSchema.get.getAttributeName -> dyn.asRangeAttribute(rangeValue))
        case None => Map(dyn.hashSchema.getAttributeName -> dyn.asHashAttribute(idAndRangePair._1))
      }

      new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(key))
    }

    val batchJob = new BatchWriteItemRequest().withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

    def twentyFiveAtATime(first25: Seq[WriteRequest], rest: Seq[WriteRequest]): Unit = {
      if (rest.size > 0) {
        processRequestItems(Map(dyn.table(tablePrefix) -> first25.asJava))
        twentyFiveAtATime(rest.take(25), rest.drop(25))
      }
      else
        processRequestItems(Map(dyn.table(tablePrefix) -> first25.asJava))
    }

    def processRequestItems(requestItems: java.util.Map[String,java.util.List[WriteRequest]]): Unit = {
      batchJob.withRequestItems(requestItems)
      val result = db.batchWriteItem(batchJob)
      val remainingRequestItems = result.getUnprocessedItems()

      // Check for unprocessed keys which could happen if you exceed provisioned throughput
      if (remainingRequestItems.size() > 0)
        processRequestItems(remainingRequestItems)
    }

    twentyFiveAtATime(threadList.take(25), threadList.drop(25))
  }

  override def toString = "DeleteByRange[%s](%s)" format (dyn.table(""), super.toString)
}

case class ColumnCondition(columnName: String, dataType: ScalarAttributeType, operator: ComparisonOperator, value: String) {
  def toConditionTuple(): Tuple2[String, Condition] = {
    val cond = new Condition()
      .withComparisonOperator(operator)
      .withAttributeValueList( DynamoObject.asAttribute(value,dataType))
    (columnName, cond)
  }
}

case class Scan[T](conditions: Seq[ColumnCondition], exclusiveStartKey: Option[Map[String,AttributeValue]] = None)(implicit dyn:DynamoObject[T]) extends DbOperation[(Seq[T], Option[Map[String,AttributeValue]])] {

  def execute(db: AmazonDynamoDB, tablePrefix:String) : (Seq[T], Option[Map[String,AttributeValue]]) = {
    val condSeq = for (condition <- conditions) yield condition.toConditionTuple()
    val scanFilter = condSeq.toMap

    val scanRequest = new ScanRequest()
      .withTableName(dyn.table(tablePrefix))
      .withScanFilter(scanFilter)
      .withReturnConsumedCapacity("TOTAL")

    exclusiveStartKey.map(key => scanRequest.setExclusiveStartKey(key.asJava))

    val result = db.scan(scanRequest)
    val items = result.getItems.asScala.map {
      item => dyn.fromDynamo(item.asScala.toMap)
    }

    val lastEvaluatedKey = if (result.getLastEvaluatedKey != null)
      Option(result.getLastEvaluatedKey.toMap)
    else
      None

    (items, lastEvaluatedKey)
  }

  def blockingStream(implicit dynamo: ActorRef, pageTimeout: Timeout): Stream[T] = //TODO: use iteratees or some other magic to get rid of this blocking behaviour (Peter G. 31/10/2012)
    functional.unfold[Scan[T], Seq[T]](this){
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

case class QueryIndex[T](indexName: String, conditions: Seq[ColumnCondition], limit : Int = Int.MaxValue, exclusiveStartKey: Option[Map[String,AttributeValue]] = None, consistentRead :Boolean = false)(implicit dyn:DynamoObject[T]) extends DbOperation[(Seq[T], Option[Map[String,AttributeValue]])]{
  def execute(db: AmazonDynamoDB, tablePrefix:String) : (Seq[T], Option[Map[String,AttributeValue]]) = {

    val condSeq = for (condition <- conditions) yield condition.toConditionTuple()
    val keyConditions = condSeq.toMap

    val query = new dynamodbv2.model.QueryRequest()
      .withTableName(dyn.table(tablePrefix))
      .withKeyConditions(keyConditions.asJava)
      .withConsistentRead(consistentRead)
      .withLimit(limit)
      .withReturnConsumedCapacity("TOTAL")
      .withIndexName(indexName)

    exclusiveStartKey.map(key => query.setExclusiveStartKey(key.asJava))

    val result = db.query(query)
    val items = result.getItems.asScala.map {
      item => dyn.fromDynamo(item.asScala.toMap)
    }

    val lastEvaluatedKey = if (result.getLastEvaluatedKey != null)
      Option(result.getLastEvaluatedKey.toMap)
    else
      None

    (items, lastEvaluatedKey)
  }

  def blockingStream(implicit dynamo: ActorRef, pageTimeout: Timeout): Stream[T] = //TODO: use iteratees or some other magic to get rid of this blocking behaviour (Peter G. 31/10/2012)
    functional.unfold[QueryIndex[T], Seq[T]](this){
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

case class Query[T](id: String, operator: Option[String], attributes: Seq[Any], limit : Int, exclusiveStartKey: Option[Map[String,AttributeValue]], consistentRead :Boolean)(implicit dyn:DynamoObject[T]) extends DbOperation[(Seq[T], Option[Map[String,AttributeValue]])]{

  def execute(db: AmazonDynamoDB, tablePrefix:String) : (Seq[T], Option[Map[String,AttributeValue]]) = {

    val keyConditions = operator
    .map( operator => Map(
      dyn.hashSchema.getAttributeName -> new Condition()
        .withComparisonOperator("EQ")
        .withAttributeValueList(dyn.asHashAttribute(id)),
      dyn.rangeSchema.get.getAttributeName -> new Condition()
        .withComparisonOperator( operator )
        .withAttributeValueList( attributes.map(dyn.asRangeAttribute).asJava ))
    )
    .getOrElse( Map(
      dyn.hashSchema.getAttributeName -> new Condition()
        .withComparisonOperator("EQ")
        .withAttributeValueList(dyn.asHashAttribute(id)))
    )

    val query = new dynamodbv2.model.QueryRequest()
      .withTableName(dyn.table(tablePrefix))
      .withKeyConditions(keyConditions.asJava)
      .withConsistentRead(consistentRead)
      .withLimit(limit)
      .withReturnConsumedCapacity("TOTAL")

    //query.setIndexName()

    exclusiveStartKey.map(key => query.setExclusiveStartKey(key.asJava))

    val result = db.query(query)
    val items = result.getItems.asScala.map {
      item => dyn.fromDynamo(item.asScala.toMap)
    }

    val lastEvaluatedKey = if (result.getLastEvaluatedKey != null)
      Option(result.getLastEvaluatedKey.toMap)
    else
      None

    (items, lastEvaluatedKey)
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
  def apply[T](id: String, operator: String = null, attributes: Seq[Any] = Nil, limit : Int = Int.MaxValue, exclusiveStartKey: Option[Map[String,AttributeValue]] = None, consistentRead :Boolean = true)(implicit dyn:DynamoObject[T]) :Query[T]=
    Query(id, Option(operator), attributes, limit, exclusiveStartKey, consistentRead)
}