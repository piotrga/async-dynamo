package asyncdynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.regions.Region
import akka.event.EventStream
import com.amazonaws.regions.Region
import java.util
import java.lang.Boolean
import scala.collection.JavaConverters._

private class TracingAmazonDynamoDB(delegate  : AmazonDynamoDB, eventStream : EventStream) extends AmazonDynamoDB {

  def setEndpoint(endpoint: String) {delegate.setEndpoint(endpoint)}
  def setRegion(region: Region) { delegate.setRegion(region) }
  def getCachedResponseMetadata(request: AmazonWebServiceRequest) = delegate.getCachedResponseMetadata(request)

  def createTable(createTableRequest: CreateTableRequest) = delegate.createTable(createTableRequest)
  def createTable(attributeDefinitions: util.List[AttributeDefinition], tableName: String, keySchema: util.List[KeySchemaElement], provisionedThroughput: ProvisionedThroughput) =
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
    pub(DynamoRequestExecuted(Operation(deleteItemRequest.getTableName, Write,"DeleteItem"), writeUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }

  def deleteItem(tableName: String, key: util.Map[String, AttributeValue], returnValues: String): DeleteItemResult =  {
    val (res, duration) = time (delegate.deleteItem(tableName, key, returnValues))
    pub(DynamoRequestExecuted(Operation(tableName, Write,"DeleteItem"), writeUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }

  def deleteItem(tableName: String, key: util.Map[String, AttributeValue]): DeleteItemResult = {
    val (res, duration) = time (delegate.deleteItem(tableName, key))
    pub(DynamoRequestExecuted(Operation(tableName, Write,"DeleteItem"), writeUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }

  def getItem(getItemRequest: GetItemRequest) = {
    val (res, duration) = time(delegate.getItem(getItemRequest))
    pub(DynamoRequestExecuted(Operation(getItemRequest.getTableName, Read,"GetItem"), readUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }

  def getItem(tableName: String, key: util.Map[String, AttributeValue], consistentRead: Boolean): GetItemResult = {
    val (res, duration) = time(delegate.getItem(tableName, key, consistentRead))
    pub(DynamoRequestExecuted(Operation(tableName, Read,"GetItem"), readUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }

  def getItem(tableName: String, key: util.Map[String, AttributeValue]): GetItemResult = getItem(tableName, key, true)

  def scan(scanRequest: ScanRequest) = {
    val (res, duration) = time(delegate.scan(scanRequest))
    pub(DynamoRequestExecuted(Operation(scanRequest.getTableName, Read,"Scan"), readUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }

  def scan(tableName: String, attributesToGet: util.List[String], scanFilter: util.Map[String, Condition]): ScanResult = {
    val (res, duration) = time(delegate.scan(tableName, attributesToGet, scanFilter))
    pub(DynamoRequestExecuted(Operation(tableName, Read,"Scan"), readUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }

  import collection.JavaConversions._

  def scan(tableName: String, scanFilter: util.Map[String, Condition]): ScanResult = scan(tableName, List[String]() ,scanFilter)

  def scan(tableName: String, attributesToGet: util.List[String]): ScanResult = scan(tableName, attributesToGet, Map[String,Condition]())

  def updateItem(updateItemRequest: UpdateItemRequest) = {
    val (res, duration) = time(delegate.updateItem(updateItemRequest))
    pub(DynamoRequestExecuted(Operation(updateItemRequest.getTableName, Write,"UpdateItem"), writeUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }

  def updateItem(tableName: String, key: util.Map[String, AttributeValue], attributeUpdates: util.Map[String, AttributeValueUpdate], returnValues: String): UpdateItemResult = {
    val (res, duration) = time(delegate.updateItem(tableName, key, attributeUpdates, returnValues))
    pub(DynamoRequestExecuted(Operation(tableName, Write,"UpdateItem"), writeUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }

  def updateItem(tableName: String, key: util.Map[String, AttributeValue], attributeUpdates: util.Map[String, AttributeValueUpdate]): UpdateItemResult = {
    val (res, duration) = time(delegate.updateItem(tableName, key, attributeUpdates))
    pub(DynamoRequestExecuted(Operation(tableName, Write,"UpdateItem"), writeUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }

  def query(queryRequest: QueryRequest) = {
    val (res, duration) = time(delegate.query(queryRequest))
    pub(DynamoRequestExecuted(Operation(queryRequest.getTableName, Read,"Query"), readUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }

  def putItem(putItemRequest: PutItemRequest) = {
    val (res, duration) = time(delegate.putItem(putItemRequest))
    pub(DynamoRequestExecuted(Operation(putItemRequest.getTableName, Write,"PutItem"), writeUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }

  def putItem(tableName: String, item: util.Map[String, AttributeValue], returnValues: String): PutItemResult = {
    val (res, duration) = time(delegate.putItem(tableName, item, returnValues))
    pub(DynamoRequestExecuted(Operation(tableName, Write,"PutItem"), writeUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }

  def putItem(tableName: String, item: util.Map[String, AttributeValue]): PutItemResult = {
    val (res, duration) = time(delegate.putItem(tableName, item))
    pub(DynamoRequestExecuted(Operation(tableName, Write,"PutItem"), writeUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }



  def batchGetItem(batchGetItemRequest: BatchGetItemRequest) = {
    val (res, duration) = time(delegate.batchGetItem(batchGetItemRequest))

    val consumption = res.getResponses.zip(res.getConsumedCapacity)
    consumption foreach {case ((tableName, r), capacityConsumed) =>
      pub(DynamoRequestExecuted(Operation(tableName, Read, "BatchGetItem"), readUnits = capacityConsumed.getCapacityUnits, duration = duration))
    }
    res
  }

  def batchGetItem(requestItems: util.Map[String, KeysAndAttributes]): BatchGetItemResult = {
    val (res, duration) = time(delegate.batchGetItem(requestItems))

    val consumption = res.getResponses.zip(res.getConsumedCapacity)
    consumption foreach {case ((tableName, r), capacityConsumed) =>
      pub(DynamoRequestExecuted(Operation(tableName, Read, "BatchGetItem"), readUnits = capacityConsumed.getCapacityUnits, duration = duration))
    }
    res
  }

  def batchGetItem(requestItems: util.Map[String, KeysAndAttributes], returnConsumedCapacity: String): BatchGetItemResult = {
    val (res, duration) = time(delegate.batchGetItem(requestItems, returnConsumedCapacity))

    val consumption = res.getResponses.zip(res.getConsumedCapacity)
    consumption foreach {case ((tableName, r), capacityConsumed) =>
      pub(DynamoRequestExecuted(Operation(tableName, Read, "BatchGetItem"), readUnits = capacityConsumed.getCapacityUnits, duration = duration))
    }
    res
  }

  private def pub(op:DynamoRequestExecuted) = eventStream.publish(op)

  def time[T]( f: => T) : (T, Long)  ={
    val start = System.currentTimeMillis()
    val res = f
    (res, System.currentTimeMillis() - start)
  }

  def batchWriteItem(batchWriteItemRequest: BatchWriteItemRequest) = {
    val (res, duration) = time(delegate.batchWriteItem(batchWriteItemRequest))

    val consumption = res.getItemCollectionMetrics.zip(res.getConsumedCapacity)
    consumption foreach {case ((tableName, r), capacityConsumed) =>
      pub(DynamoRequestExecuted(Operation(tableName, Write, "BatchWriteItem"), writeUnits = capacityConsumed.getCapacityUnits, duration = duration))
    }
    res
  }

  def batchWriteItem(requestItems: util.Map[String, util.List[WriteRequest]]): BatchWriteItemResult = {
    val (res, duration) = time(delegate.batchWriteItem(requestItems))

    val consumption = res.getItemCollectionMetrics.zip(res.getConsumedCapacity)
    consumption foreach {case ((tableName, r), capacityConsumed) =>
      pub(DynamoRequestExecuted(Operation(tableName, Write, "BatchWriteItem"), writeUnits = capacityConsumed.getCapacityUnits, duration = duration))
    }
    res
  }
}
