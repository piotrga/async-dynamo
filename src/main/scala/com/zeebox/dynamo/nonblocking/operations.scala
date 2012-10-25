package com.zeebox.dynamo.nonblocking

import scala.collection.JavaConverters._
import com.amazonaws.services.dynamodb.model._
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.services.dynamodb.model.AttributeValue
import com.amazonaws.services.dynamodb.model.PutItemRequest
import com.amazonaws.services.dynamodb.model.ScanRequest
import com.amazonaws.services.dynamodb.model.DeleteItemRequest
import com.amazonaws.services.dynamodb.model.Key
import com.zeebox.dynamo.{DynamoObject, DbOperation}


case class Save[T : DynamoObject](o : T) extends DbOperation[T]{
  def execute(db: AmazonDynamoDBClient, tablePrefix:String) : T = {
    val dyn = implicitly[DynamoObject[T]]
    db.putItem(new PutItemRequest(dyn.table(tablePrefix), dyn.toDynamo(o).asJava))
    o
  }
}

case class Read[T](id:String)(implicit dyn:DynamoObject[T]) extends DbOperation[Option[T]]{
  def execute(db: AmazonDynamoDBClient, tablePrefix:String) : Option[T] = {
    val attributes = db.getItem(new GetItemRequest(dyn.table(tablePrefix), new Key().withHashKeyElement(new AttributeValue(id)))).getItem
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
      val id = item.get(dyn.keyName)
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



