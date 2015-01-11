package com.datastax.driver.scala

import scala.concurrent.duration.FiniteDuration

package object embedded {

  /* Factor by which to scale timeouts during tests, e.g. to account for shared build system load. */
  implicit class TestDuration(val duration: FiniteDuration) extends AnyVal {
    def dilated: FiniteDuration = (duration * 1.0).asInstanceOf[FiniteDuration]
  }
}

