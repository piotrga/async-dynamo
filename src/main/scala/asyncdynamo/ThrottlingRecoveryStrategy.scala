package asyncdynamo

import akka.actor.ActorSystem
import akka.util.Duration
import com.amazonaws.services.dynamodb.model.ProvisionedThroughputExceededException

trait ThrottlingRecoveryStrategy{
  def amazonMaxErrorRetry : Int
  def onExecute[T](f: => T, operation: PendingOperation[T], system : ActorSystem) : T
}

object AmazonThrottlingRecoveryStrategy{
  def forTimeout(timeout :Duration) = AmazonThrottlingRecoveryStrategy((math.log(timeout.toMillis / 50 +1)/math.log(2)).toInt)
}

case class AmazonThrottlingRecoveryStrategy(maxRetries: Int) extends ThrottlingRecoveryStrategy{
  val amazonMaxErrorRetry = maxRetries
  def onExecute[T](f: => T, operation: PendingOperation[T], system : ActorSystem) : T = f
}

case class ExpotentialBackoffThrottlingRecoveryStrategy(maxRetries: Int, backoffBase: Duration) extends  ThrottlingRecoveryStrategy{
  val amazonMaxErrorRetry = 0
  lazy val BASE = backoffBase.toMillis

  def onExecute[T](evaluate: => T, operation: PendingOperation[T], system : ActorSystem) : T = {

    def Try(attempt: Int) : T = {
      try evaluate
      catch{
        case ex: ProvisionedThroughputExceededException =>
          val pauseMillis = BASE * math.pow(2, attempt).toInt
          system.eventStream publish ProvisionedThroughputExceeded(operation.operation, "Retrying in [%d] millis. Attempt [%d]" format (pauseMillis, attempt))
          Thread.sleep(pauseMillis)
          if (attempt < maxRetries) Try(attempt+1) else evaluate
      }
    }

    if (maxRetries > 0) Try(attempt = 1) else evaluate


  }
}

