package com.gslab.convergence

import java.util.UUID
import com.datastax.driver.core.{ResultSet, Row}
import scala.collection.JavaConversions.iterableAsScalaIterable

/**
  * Created by GS-1159 on 30-04-2017.
  */

object EventMeta extends ConfigReader with Logging {

  private val session = CassandraUtils.getSession

  private val table = config.getString("event.meta.table")

  private val fetchSize = config.getInt("fetchSize")

  private val fetchAllMetaPS = session.prepare(s"select vendor_id, customer_id, meta_key, meta_value, event_id, sensor_id, writetime(sensor_id) as creation_time from $table")

  private val deleteMetaPS = session.prepare(s"delete from $table where vendor_id = ? and customer_id = ? and meta_key = ? and meta_value = ? and event_id = ? if exists")

  private var invalidRows = 0L
  private var deletedInvalidRows = 0L

  def fetchAllMeta = {
    session.execute(fetchAllMetaPS.bind())
  }

  def cleanMetaData(sourceEventIds: Set[UUID], eventMeta: ResultSet, fromTime: Long) = {

    def deleteIfInvalid(row: Row): Boolean =
      if (isRowInvalid(row)) {
        logger.info(s"row $row is marked as invalid")
        invalidRows += 1
        delete(row)
      }
      else {
        false
      }

    def delete(row: Row) =
      try {
        session.execute(deleteMetaPS.bind(row.getString("vendor_id"), row.getString("customer_id"), row.getString("meta_key"), row.getString("meta_value"), row.getUUID("event_id"))).wasApplied()
        deletedInvalidRows += 1
        true
      }
      catch {
        case th: Throwable =>
          logger.error(s"error occured while deleteing meta for row: $row")
          false
      }


    def isRowInvalid(row: Row): Boolean = {
      val eventId = row.getUUID("event_id")
      if (sourceEventIds.contains(eventId)) {
        false
      }
      else {
        if (isNewlyGeneratedEvent(row)) false else true
      }
    }

    def isNewlyGeneratedEvent(row: Row): Boolean = {
      val creationTime = row.getLong("creation_time") / 1000L
      creationTime >= fromTime
    }

    for (row <- eventMeta) {
      if (eventMeta.getAvailableWithoutFetching == 100 && !eventMeta.isFullyFetched)
        eventMeta.fetchMoreResults()
      deleteIfInvalid(row)
    }

    (invalidRows, deletedInvalidRows)
  }

}