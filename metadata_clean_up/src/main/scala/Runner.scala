package com.gslab.convergence

import java.util.Date

import scala.io.{Source, StdIn}

/**
  * Created by GS-1159 on 26-04-2017.
  */
object Runner extends App with Logging {
  logger.info(s"app started")

  val startTime = new Date().getTime
  val eventIds = Event.fetchAllEventIds

  /*logger.info(s"press enter to continue")
  StdIn.readLine()
  logger.info(s"continuing app")*/

  val eventMeta = EventMeta.fetchAllMeta

  val (invalidRows, deletedInvalidRows) = EventMeta.cleanMetaData(eventIds, eventMeta, startTime)
  logger.info(s"found total $invalidRows invalid rows")
  logger.info(s"deleted total $deletedInvalidRows invalid rows")

  logger.info(s"cleanup completed, closing cassandra connection")
  CassandraUtils.closeCluster
  CassandraUtils.closeConnection

  logger.info(s"app completed")
}