package com.datastax.driver.scala.core.io

import com.datastax.driver.core.{Session, Statement}

class QueryExecutor(session: Session, maxConcurrentQueries: Int)
  extends AsyncExecutor(session.executeAsync(_ : Statement), maxConcurrentQueries)

