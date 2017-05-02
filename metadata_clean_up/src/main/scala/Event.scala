package com.gslab.convergence

import java.util.UUID

import com.datastax.driver.core.ResultSet
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions.iterableAsScalaIterable

/**
  * Created by GS-1159 on 30-04-2017.
  */
object Event extends ConfigReader with Logging {

  private val session = CassandraUtils.getSession

  private val eventTable = config.getString("event.table")

  private val fetchSize = config.getInt("fetchSize")

  private val fetchAllEventsPS = session.prepare(s"select event_id from $eventTable")

  private var eventCount = 0L

  def getAllEvents = session.execute(fetchAllEventsPS.bind().setFetchSize(fetchSize))


  def fetchAllEventIds = {
    val allEventData = getAllEvents
    var eventIds = Set.empty[UUID]
    for (eventRow <- allEventData) {
      if (allEventData.getAvailableWithoutFetching == 100 && !allEventData.isFullyFetched) {
        allEventData.fetchMoreResults()
        logger.info(s"fetching more event data, iteration: ${(eventCount / fetchSize) + 1}")
      }
      eventIds += eventRow.getUUID("event_id")
      eventCount += 1
    }
    eventIds
  }

}