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

// Scala
import scala.collection.JavaConverters._

// Java
import java.util.{Map => JMap, List => JList}
import java.lang.{Boolean => JBoolean}

// AWS SDK
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.regions.Region

// Akka
import akka.event.EventStream

protected class TracingAmazonDynamoDB(delegate  : AmazonDynamoDB, eventStream : EventStream) extends AmazonDynamoDB {

  def setEndpoint(endpoint: String) {delegate.setEndpoint(endpoint)}
  def setRegion(region: Region) { delegate.setRegion(region) }
  def getCachedResponseMetadata(request: AmazonWebServiceRequest) = delegate.getCachedResponseMetadata(request)

  def createTable(createTableRequest: CreateTableRequest) = delegate.createTable(createTableRequest)
  def createTable(attributeDefinitions: JList[AttributeDefinition], tableName: String, keySchema: JList[KeySchemaElement], provisionedThroughput: ProvisionedThroughput) =
    delegate.createTable(attributeDefinitions, tableName, keySchema, provisionedThroughput)
  def updateTable(updateTableRequest: UpdateTableRequest) = delegate.updateTable(updateTableRequest)
  def updateTable(tableName: String, provisionedThroughput: ProvisionedThroughput): UpdateTableResult = delegate.updateTable(tableName, provisionedThroughput)
  def describeTable(describeTableRequest: DescribeTableRequest) = delegate.describeTable(describeTableRequest)
  def describeTable(tableName: String) = delegate.describeTable(tableName)
  def listTables() = delegate.listTables()
  def listTables(listTablesRequest: ListTablesRequest) = delegate.listTables(listTablesRequest)
  def listTables(limit: Integer): ListTablesResult = delegate.listTables(limit)
  def listTables(exclusiveStartTableName: String, limit: Integer) = delegate.listTables(exclusiveStartTableName, limit)
  def listTables(exclusiveStartTableName: String) = delegate.listTables(exclusiveStartTableName)
  def deleteTable(deleteTableRequest: DeleteTableRequest) = delegate.deleteTable(deleteTableRequest)
  def deleteTable(tableName: String) = delegate.deleteTable(tableName)

  def shutdown() {delegate.shutdown()}

  import Operation._

  def deleteItem(deleteItemRequest: DeleteItemRequest) = {
    val (res, duration) = time (delegate.deleteItem(deleteItemRequest))
    pub(DynamoRequestExecuted(Operation(deleteItemRequest.getTableName, Write,"DeleteItem"), writeUnits = Option(Double.unbox(res.getConsumedCapacity.getCapacityUnits)), duration = duration))
    res
  }

  def deleteItem(tableName: String, key: JMap[String, AttributeValue], returnValues: String): DeleteItemResult =  {
    val (res, duration) = time (delegate.deleteItem(tableName, key, returnValues))
    pub(DynamoRequestExecuted(Operation(tableName, Write,"DeleteItem"), writeUnits = Option(Double.unbox(res.getConsumedCapacity.getCapacityUnits)), duration = duration))
    res
  }

  def deleteItem(tableName: String, key: JMap[String, AttributeValue]): DeleteItemResult = {
    val (res, duration) = time (delegate.deleteItem(tableName, key))
    pub(DynamoRequestExecuted(Operation(tableName, Write, "DeleteItem"), writeUnits = Option(Double.unbox(res.getConsumedCapacity.getCapacityUnits)), duration = duration))
    res
  }

  def getItem(getItemRequest: GetItemRequest) = {
    val (res, duration) = time(delegate.getItem(getItemRequest))
    pub(DynamoRequestExecuted(Operation(getItemRequest.getTableName, Read, "GetItem"), readUnits = Option(Double.unbox(res.getConsumedCapacity.getCapacityUnits)), duration = duration))
    res
  }

  def getItem(tableName: String, key: JMap[String, AttributeValue], consistentRead: JBoolean): GetItemResult = {
    val (res, duration) = time(delegate.getItem(tableName, key, consistentRead))
    pub(DynamoRequestExecuted(Operation(tableName, Read,"GetItem"), readUnits = Option(Double.unbox(res.getConsumedCapacity.getCapacityUnits)), duration = duration))
    res
  }

  def getItem(tableName: String, key: JMap[String, AttributeValue]): GetItemResult = getItem(tableName, key, true)

  def scan(scanRequest: ScanRequest) = {
    val (res, duration) = time(delegate.scan(scanRequest))
    pub(DynamoRequestExecuted(Operation(scanRequest.getTableName, Read, "Scan"), readUnits = Option(Double.unbox(res.getConsumedCapacity.getCapacityUnits)), duration = duration))
    res
  }

  def scan(tableName: String, attributesToGet: JList[String], scanFilter: JMap[String, Condition]): ScanResult = {
    val (res, duration) = time(delegate.scan(tableName, attributesToGet, scanFilter))
    pub(DynamoRequestExecuted(Operation(tableName, Read,"Scan"), readUnits = Option(Double.unbox(res.getConsumedCapacity.getCapacityUnits)), duration = duration))
    res
  }

  import collection.JavaConversions._

  def scan(tableName: String, scanFilter: JMap[String, Condition]): ScanResult = scan(tableName, List[String]() ,scanFilter)

  def scan(tableName: String, attributesToGet: JList[String]): ScanResult = scan(tableName, attributesToGet, Map[String,Condition]())

  def updateItem(updateItemRequest: UpdateItemRequest) = {
    val (res, duration) = time(delegate.updateItem(updateItemRequest))
    pub(DynamoRequestExecuted(Operation(updateItemRequest.getTableName, Write, "UpdateItem"), writeUnits = Option(Double.unbox(res.getConsumedCapacity.getCapacityUnits)), duration = duration))
    res
  }

  def updateItem(tableName: String, key: JMap[String, AttributeValue], attributeUpdates: JMap[String, AttributeValueUpdate], returnValues: String): UpdateItemResult = {
    val (res, duration) = time(delegate.updateItem(tableName, key, attributeUpdates, returnValues))
    pub(DynamoRequestExecuted(Operation(tableName, Write,"UpdateItem"), writeUnits = Option(Double.unbox(res.getConsumedCapacity.getCapacityUnits)), duration = duration))
    res
  }

  def updateItem(tableName: String, key: JMap[String, AttributeValue], attributeUpdates: JMap[String, AttributeValueUpdate]): UpdateItemResult = {
    val (res, duration) = time(delegate.updateItem(tableName, key, attributeUpdates))
    pub(DynamoRequestExecuted(Operation(tableName, Write,"UpdateItem"), writeUnits = Option(Double.unbox(res.getConsumedCapacity.getCapacityUnits)), duration = duration))
    res
  }

  def query(queryRequest: QueryRequest) = {
    val (res, duration) = time(delegate.query(queryRequest))
    pub(DynamoRequestExecuted(Operation(queryRequest.getTableName, Read, "Query"), readUnits = Option(Double.unbox(res.getConsumedCapacity.getCapacityUnits)), duration = duration))
    res
  }

  def putItem(putItemRequest: PutItemRequest) = {
    val (res, duration) = time(delegate.putItem(putItemRequest))
    pub(DynamoRequestExecuted(Operation(putItemRequest.getTableName, Write,"PutItem"), writeUnits = Option(Double.unbox(res.getConsumedCapacity.getCapacityUnits)), duration = duration))
    res
  }

  def putItem(tableName: String, item: JMap[String, AttributeValue], returnValues: String): PutItemResult = {
    val (res, duration) = time(delegate.putItem(tableName, item, returnValues))
    pub(DynamoRequestExecuted(Operation(tableName, Write, "PutItem"), writeUnits = Option(Double.unbox(res.getConsumedCapacity.getCapacityUnits)), duration = duration))
    res
  }

  def putItem(tableName: String, item: JMap[String, AttributeValue]): PutItemResult = {
    val (res, duration) = time(delegate.putItem(tableName, item))
    pub(DynamoRequestExecuted(Operation(tableName, Write,"PutItem"), writeUnits = Option(Double.unbox(res.getConsumedCapacity.getCapacityUnits)), duration = duration))
    res
  }

  import collection.JavaConversions._

  def batchGetItem(batchGetItemRequest: BatchGetItemRequest) = {
    val (res, duration) = time(delegate.batchGetItem(batchGetItemRequest))

    res.getConsumedCapacity foreach {
      case consumedCapacity => 
        pub(DynamoRequestExecuted(Operation(consumedCapacity.getTableName(), Read, "BatchGetItem"), readUnits = Option(Double.unbox(consumedCapacity.getCapacityUnits)), duration = duration))
    }
    res
  }

  def batchGetItem(requestItems: JMap[String, KeysAndAttributes]): BatchGetItemResult = {
    val (res, duration) = time(delegate.batchGetItem(requestItems))

    res.getConsumedCapacity foreach {
      case consumedCapacity =>
        pub(DynamoRequestExecuted(Operation(consumedCapacity.getTableName(), Read, "BatchGetItem"), readUnits = Option(Double.unbox(consumedCapacity.getCapacityUnits)), duration = duration))
    }
    res
  }

  def batchGetItem(requestItems: JMap[String, KeysAndAttributes], returnConsumedCapacity: String): BatchGetItemResult = {
    val (res, duration) = time(delegate.batchGetItem(requestItems, returnConsumedCapacity))

    res.getConsumedCapacity foreach {
      case consumedCapacity =>
        pub(DynamoRequestExecuted(Operation(consumedCapacity.getTableName(), Read, "BatchGetItem"), readUnits = Option(Double.unbox(consumedCapacity.getCapacityUnits)), duration = duration))
    }
    res
  }

  def batchWriteItem(batchWriteItemRequest: BatchWriteItemRequest) = {
    val (res, duration) = time(delegate.batchWriteItem(batchWriteItemRequest))

    res.getConsumedCapacity foreach {
      case consumedCapacity => 
        pub(DynamoRequestExecuted(Operation(consumedCapacity.getTableName(), Write, "BatchWriteItem"), writeUnits = Option(Double.unbox(consumedCapacity.getCapacityUnits)), duration = duration))
    }
    res
  }

  def batchWriteItem(requestItems: JMap[String, JList[WriteRequest]]): BatchWriteItemResult = {
    val (res, duration) = time(delegate.batchWriteItem(requestItems))

    res.getConsumedCapacity foreach {
      case consumedCapacity =>
        pub(DynamoRequestExecuted(Operation(consumedCapacity.getTableName(), Write, "BatchWriteItem"), writeUnits = Option(Double.unbox(consumedCapacity.getCapacityUnits)), duration = duration))
    }
    res
  }

  private def pub(op:DynamoRequestExecuted) = eventStream.publish(op)

  def time[T](f: => T): (T, Long) = {
    val start = System.currentTimeMillis()
    val res = f
    (res, System.currentTimeMillis() - start)
  }
}
