package asyncdynamo.nonblocking

import scala.collection.JavaConverters._
import com.amazonaws.services.dynamodb.model._
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.services.dynamodb.model.AttributeValue
import com.amazonaws.services.dynamodb.model.PutItemRequest
import com.amazonaws.services.dynamodb.model.ScanRequest
import com.amazonaws.services.dynamodb.model.DeleteItemRequest
import com.amazonaws.services.dynamodb.model.Key
import asyncdynamo.{functional, DynamoObject, DbOperation}
import akka.actor.ActorRef
import akka.util.Timeout


case class Save[T : DynamoObject](o : T) extends DbOperation[T]{
  def execute(db: AmazonDynamoDBClient, tablePrefix:String) : T = {
    val dyn = implicitly[DynamoObject[T]]
    db.putItem(new PutItemRequest(dyn.table(tablePrefix), dyn.toDynamo(o).asJava))
    o
  }
}

case class Read[T](id:String, consistentRead : Boolean = true)(implicit dyn:DynamoObject[T]) extends DbOperation[Option[T]]{
  def execute(db: AmazonDynamoDBClient, tablePrefix:String) : Option[T] = {

    val read = new GetItemRequest( dyn.table(tablePrefix), new Key().withHashKeyElement(new AttributeValue(id)))
      .withConsistentRead(consistentRead)

    val attributes = db.getItem(read).getItem
    Option (attributes) map ( attr => dyn.fromDynamo(attr.asScala.toMap) )
  }
}

case class ListAll[T](limit : Int)(implicit dyn:DynamoObject[T]) extends DbOperation[Seq[T]]{
  def execute(db: AmazonDynamoDBClient, tablePrefix:String) : Seq[T] = {
    db.scan(new ScanRequest(dyn.table(tablePrefix)).withLimit(limit)).getItems.asScala.map {
      item => dyn.fromDynamo(item.asScala.toMap)
    }
  }
}

case class DeleteAll[T](implicit dyn:DynamoObject[T]) extends DbOperation[Int]{
  def execute(db: AmazonDynamoDBClient, tablePrefix:String) : Int = {
    val res = db.scan(new ScanRequest(dyn.table(tablePrefix)))
    res.getItems.asScala.par.map{ item =>
      val id = item.get(dyn.key.getAttributeName)
      db.deleteItem( new DeleteItemRequest().withTableName(dyn.table(tablePrefix)).withKey(new Key().withHashKeyElement(id)) )
    }
    res.getCount
  }
}

case class DeleteById[T](id: String)(implicit dyn:DynamoObject[T]) extends DbOperation[Unit]{
  def execute(db: AmazonDynamoDBClient, tablePrefix:String){
    db.deleteItem( new DeleteItemRequest().withTableName(dyn.table(tablePrefix)).withKey(new Key().withHashKeyElement(new AttributeValue(id))))
  }
}

case class DeleteByRange[T](id: String, range: Any, expected: Map[String,String] = Map.empty)(implicit dyn:DynamoObject[T]) extends DbOperation[Unit]{
  def execute(db: AmazonDynamoDBClient, tablePrefix:String){
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
}

case class Query[T](id: String, operator: String, attributes: Seq[String], limit : Int = Int.MaxValue, exclusiveStartKey: Key = null, consistentRead :Boolean = true)(implicit dyn:DynamoObject[T]) extends DbOperation[(Seq[T], Option[Key])]{
  def execute(db: AmazonDynamoDBClient, tablePrefix:String) : (Seq[T], Option[Key]) = {

    val condition = new Condition()
      .withComparisonOperator(operator)
      .withAttributeValueList(attributes.map(new AttributeValue(_)).asJava)

    val query = new QueryRequest()
      .withTableName(dyn.table(tablePrefix))
      .withHashKeyValue(new AttributeValue(id))
      .withRangeKeyCondition(condition)
      .withExclusiveStartKey(exclusiveStartKey)
      .withConsistentRead(consistentRead)
      .withLimit(limit)

    val result = db.query(query)
    val items = result.getItems.asScala.map {
      item => dyn.fromDynamo(item.asScala.toMap)
    }

    (items, Option(result.getLastEvaluatedKey))
  }

  def blockingStream(implicit dynamo: ActorRef, pageTimeout: Timeout): Stream[T] =
    functional.unfold[Query[T], Seq[T]](this){
      query =>
        val (resultChunk, lastKey) = query.blockingExecute
        lastKey match{
          case None => (None, resultChunk)
          case Some(key) => (Some(query.copy(exclusiveStartKey = key)), resultChunk)
        }
    }.flatten
}

