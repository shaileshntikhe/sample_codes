package com.gslab.convergence

import org.slf4j.LoggerFactory

/**
  * Created by GS-1159 on 26-04-2017.
  */
trait Logging {
  val logger = LoggerFactory.getLogger(this.getClass)
}