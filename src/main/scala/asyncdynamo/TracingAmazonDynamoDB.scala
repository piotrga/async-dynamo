package asyncdynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.AmazonWebServiceRequest
import akka.event.EventStream
import com.amazonaws.regions.Region

private class TracingAmazonDynamoDB(delegate : AmazonDynamoDB, eventStream : EventStream) extends AmazonDynamoDB {

  def setEndpoint(endpoint: String) {delegate.setEndpoint(endpoint)}
  def setRegion(region: Region) {delegate.setRegion(region)}
  def getCachedResponseMetadata(request: AmazonWebServiceRequest) = delegate.getCachedResponseMetadata(request)

  def createTable(createTableRequest: CreateTableRequest) = delegate.createTable(createTableRequest)
  def updateTable(updateTableRequest: UpdateTableRequest) = delegate.updateTable(updateTableRequest)
  def describeTable(describeTableRequest: DescribeTableRequest) = delegate.describeTable(describeTableRequest)
  def listTables() = delegate.listTables()
  def listTables(listTablesRequest: ListTablesRequest) = delegate.listTables(listTablesRequest)
  def deleteTable(deleteTableRequest: DeleteTableRequest) = delegate.deleteTable(deleteTableRequest)

  def shutdown() {delegate.shutdown()}

  import Operation._



  def deleteItem(deleteItemRequest: DeleteItemRequest) = {
    val (res, duration) = time(delegate.deleteItem(deleteItemRequest))
    pub(DynamoRequestExecuted(Operation(deleteItemRequest.getTableName, Write,"DeleteItem"), writeUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }

  def getItem(getItemRequest: GetItemRequest) = {
    val (res, duration) = time(delegate.getItem(getItemRequest))
    pub(DynamoRequestExecuted(Operation(getItemRequest.getTableName, Read,"GetItem"), readUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }

  def scan(scanRequest: ScanRequest) = {
    val (res, duration) = time(delegate.scan(scanRequest))
    pub(DynamoRequestExecuted(Operation(scanRequest.getTableName, Read,"Scan"), readUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
    res
  }


  def updateItem(updateItemRequest: UpdateItemRequest) = {
    val (res, duration) = time(delegate.updateItem(updateItemRequest))
    pub(DynamoRequestExecuted(Operation(updateItemRequest.getTableName, Write,"UpdateItem"), writeUnits = res.getConsumedCapacity.getCapacityUnits, duration = duration))
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

  import collection.JavaConversions._

  def batchGetItem(batchGetItemRequest: BatchGetItemRequest) = {
    val (res, duration) = time(delegate.batchGetItem(batchGetItemRequest))
    res.getConsumedCapacity foreach {case (r) =>
      pub(DynamoRequestExecuted(Operation(r.getTableName, Read, "BatchGetItem"), readUnits = r.getCapacityUnits, duration = duration))
    }
    res
  }

  def batchWriteItem(batchWriteItemRequest: BatchWriteItemRequest) = {
    val (res, duration) = time(delegate.batchWriteItem(batchWriteItemRequest))
    res.getConsumedCapacity foreach {case (r) =>
      pub(DynamoRequestExecuted(Operation(r.getTableName, Write, "BatchWriteItem"), writeUnits = r.getCapacityUnits, duration = duration))
    }
    res
  }

  private def pub(op:DynamoRequestExecuted) = eventStream.publish(op)

  def time[T]( f: => T) : (T, Long)  ={
    val start = System.currentTimeMillis()
    val res = f
    (res, System.currentTimeMillis() - start)
  }

}
