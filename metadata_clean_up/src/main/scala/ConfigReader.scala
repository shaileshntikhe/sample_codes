package com.gslab.convergence

import com.typesafe.config.ConfigFactory

/**
  * Created by GS-1159 on 30-04-2017.
  */
trait ConfigReader {
  val config = ConfigFactory.load
}