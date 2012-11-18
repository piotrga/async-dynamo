package asyncdynamo

import com.amazonaws.services.dynamodb.AmazonDynamoDB
import com.amazonaws.services.dynamodb.model._
import com.amazonaws.AmazonWebServiceRequest
import akka.event.EventStream

private class TracingAmazonDynamoDB(delegate  : AmazonDynamoDB, eventStream : EventStream) extends AmazonDynamoDB {

  def setEndpoint(endpoint: String) {delegate.setEndpoint(endpoint)}
  def updateTable(updateTableRequest: UpdateTableRequest) = delegate.updateTable(updateTableRequest)
  def getCachedResponseMetadata(request: AmazonWebServiceRequest) = delegate.getCachedResponseMetadata(request)
  def listTables() = delegate.listTables()
  def listTables(listTablesRequest: ListTablesRequest) = delegate.listTables(listTablesRequest)
  def deleteTable(deleteTableRequest: DeleteTableRequest) = delegate.deleteTable(deleteTableRequest)
  def describeTable(describeTableRequest: DescribeTableRequest) = delegate.describeTable(describeTableRequest)
  def shutdown() {delegate.shutdown()}

  import Operation._

  def createTable(createTableRequest: CreateTableRequest) = {
    val res = delegate.createTable(createTableRequest)
    pub(DynamoRequestExecuted(Operation(Admin,"CreateTable"), 0, 0))
    res
  }



  def deleteItem(deleteItemRequest: DeleteItemRequest) = {
    val res = delegate.deleteItem(deleteItemRequest)
    pub(DynamoRequestExecuted(Operation(Write,"DeleteItem"), 0, res.getConsumedCapacityUnits))
    res
  }

  def getItem(getItemRequest: GetItemRequest) = {
    val res = delegate.getItem(getItemRequest)
    pub(DynamoRequestExecuted(Operation(Read,"GetItem"), res.getConsumedCapacityUnits, 0))
    res
  }

  def scan(scanRequest: ScanRequest) = {
    val res = delegate.scan(scanRequest)
    pub(DynamoRequestExecuted(Operation(Read,"Scan"), res.getConsumedCapacityUnits, 0))
    res
  }


  def updateItem(updateItemRequest: UpdateItemRequest) = {
    val res = delegate.updateItem(updateItemRequest)
    pub(DynamoRequestExecuted(Operation(Write,"UpdateItem"), res.getConsumedCapacityUnits, 0))
    res
  }

  def query(queryRequest: QueryRequest) = {
    val res = delegate.query(queryRequest)
    pub(DynamoRequestExecuted(Operation(Read,"Query"), res.getConsumedCapacityUnits, 0))
    res
  }

  def putItem(putItemRequest: PutItemRequest) = {
    val res = delegate.putItem(putItemRequest)
    pub(DynamoRequestExecuted(Operation(Write,"PutItem"), 0, res.getConsumedCapacityUnits))
    res
  }


  import collection.JavaConversions._

  def batchGetItem(batchGetItemRequest: BatchGetItemRequest) = {
    val res = delegate.batchGetItem(batchGetItemRequest)
    pub(DynamoRequestExecuted(Operation(Read,"BatchGetItem"), res.getResponses.values map(_.getConsumedCapacityUnits.toDouble) sum, 0))
    res
  }

  private def pub(op:DynamoRequestExecuted) = eventStream.publish(op)

}
