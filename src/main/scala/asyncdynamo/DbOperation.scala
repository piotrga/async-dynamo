package asyncdynamo

import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import akka.actor.ActorRef
import akka.util.{Timeout, Duration}
import akka.dispatch.{Await, Future}
import akka.pattern.ask

object DbOperation{
  val DEBUG = sys.props.get("asyncdynamo.debug")
}

abstract class DbOperation[T]{ self =>
  private val stack = try DbOperation.DEBUG map (_ => Thread.currentThread().getStackTrace.drop(7).take(7)) catch { case ex:Throwable => println(ex); None}

  def map[B](g: T => B): DbOperation[B] = (db: AmazonDynamoDBClient, tablePrefix:String) => g(safeExecute(db, tablePrefix))
  def flatMap[B](g: T => DbOperation[B]): DbOperation[B] = (db: AmazonDynamoDBClient, tablePrefix:String) => g(safeExecute(db, tablePrefix)).safeExecute(db, tablePrefix)
  def >>[B](g: => DbOperation[B]): DbOperation[B] = flatMap(_ => g)
  def andThen[B](g: => DbOperation[B]) = >>(g)

  /**
   * This is to make error messages more meaningfull.
   */
  private[asyncdynamo] def safeExecute(db: AmazonDynamoDBClient, tablePrefix:String):T = try
    execute(db, tablePrefix)
  catch {
    case ex:Throwable =>
      val additionalInfo = stack map (s => "\nOperation was created here: [\n\t%s\n\t...\n]" format s.mkString("\n\t") ) getOrElse("To see the operation origin please add -Dasyncdynamo.debug system property.")
      throw new ThirdPartyException("AmazonDB Error [%s] while executing [%s]. %s" format (ex.getMessage, this, additionalInfo), ex)
  }

  private[asyncdynamo] def execute(db: AmazonDynamoDBClient, tablePrefix:String):T

  def blockingExecute(implicit dynamo: ActorRef, timeout:Timeout): T = {
    Await.result(executeOn(dynamo)(timeout), timeout.duration)
  }

  def executeOn(dynamo: ActorRef)(implicit timeout:Timeout): Future[T] = {
    dynamo.ask(this).map(_.asInstanceOf[T])
  }
}