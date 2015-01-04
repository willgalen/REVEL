package com.datastax.driver.scala.core.policies

import com.datastax.driver.core.{WriteType, Statement, ConsistencyLevel}
import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision

/** Always retries with the same CL, constant number of times, regardless of circumstances */
class MultipleRetryPolicy(maxRetryCount: Int) extends RetryPolicy {

  private def retryOrThrow(cl: ConsistencyLevel, nbRetry: Int): RetryDecision = {
    if (nbRetry < maxRetryCount)
      RetryDecision.retry(cl)
    else
      RetryDecision.rethrow()
  }

  override def onReadTimeout(stmt: Statement, cl: ConsistencyLevel,
                             requiredResponses: Int, receivedResponses: Int,
                             dataRetrieved: Boolean, nbRetry: Int) = retryOrThrow(cl, nbRetry)

  override def onUnavailable(stmt: Statement, cl: ConsistencyLevel,
                             requiredReplica: Int, aliveReplica: Int, nbRetry: Int) = retryOrThrow(cl, nbRetry)

  override def onWriteTimeout(stmt: Statement, cl: ConsistencyLevel, writeType: WriteType,
                              requiredAcks: Int, receivedAcks: Int, nbRetry: Int) = retryOrThrow(cl, nbRetry)

}
