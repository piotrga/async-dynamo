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

import akka.actor.ActorSystem
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException
import concurrent.duration.FiniteDuration

trait ThrottlingRecoveryStrategy{
  def amazonMaxErrorRetry : Int
  def onExecute[T](f: => T, operation: PendingOperation[T], system : ActorSystem) : T
}

object AmazonThrottlingRecoveryStrategy{
  def forTimeout(timeout :FiniteDuration) = AmazonThrottlingRecoveryStrategy((math.log(timeout.toMillis / 50 +1)/math.log(2)).toInt)
}

case class AmazonThrottlingRecoveryStrategy(maxRetries: Int) extends ThrottlingRecoveryStrategy{
  val amazonMaxErrorRetry = maxRetries
  def onExecute[T](f: => T, operation: PendingOperation[T], system : ActorSystem) : T = f
}

case class ExpotentialBackoffThrottlingRecoveryStrategy(maxRetries: Int, backoffBase: FiniteDuration) extends  ThrottlingRecoveryStrategy{
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

