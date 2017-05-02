package com.gslab.convergence

import com.datastax.driver.core.Row
import scala.collection.JavaConversions.{iterableAsScalaIterable, mapAsScalaMap}
import scala.collection.JavaConverters._
import com.typesafe.config.ConfigFactory

/**
  * Created by GS-1159 on 29-04-2017.
  */
object RefreshToken extends Logging {

  private val session = CassandraUtils.getSession

  private val config = ConfigFactory.load

  private val refreshTokenSource = config.getString("refreshtoken.source.table")

  private val refreshTokenTarget = config.getString("refreshtoken.target.table")

  private val fetchAllRefreshTokensPS = session.prepare(
    s"select sessionkey, expired, refreshtoken, serialized_value, sessionsecret, vendorid from $refreshTokenSource"
  )

  private val insertRefreshTokenPS = session.prepare(
    s"""insert into $refreshTokenTarget (sessionkey, expired, refreshtoken, serialized_value, sessionsecret, vendorid)
       | values(?, ?, ?, ?, ?, ?)""".stripMargin
  )

  private val fetchSize = config.getInt("resultset.fetchsize")

  private var totalRefreshTokens = 0

  def copy() = {
    val fetchedRefreshTokenRS = session.execute(fetchAllRefreshTokensPS.bind().setFetchSize(fetchSize))


    for (row <- fetchedRefreshTokenRS) {
      if (fetchedRefreshTokenRS.getAvailableWithoutFetching == 100 && !fetchedRefreshTokenRS.isFullyFetched) {
        logger.info(s"fetching more tokens, iteration: ${(totalRefreshTokens / fetchSize) + 1}")
        fetchedRefreshTokenRS.fetchMoreResults()
      }
      insertIntoTargetTable(row)
    }

    logger.info(s"copying all refreshtokens completed, copied $totalRefreshTokens tokens")
  }

  def insertIntoTargetTable(row: Row) = {
    try {
      session.execute(insertRefreshTokenPS.bind(
        row.getUUID("sessionkey"),
        row.getBool("expired").asInstanceOf[Object],
        row.getUUID("refreshtoken"),
        row.getBytes("serialized_value"),
        row.getUUID("sessionsecret"),
        row.getLong("vendorid").asInstanceOf[Object]
      ))
      totalRefreshTokens += 1
    }
    catch {
      case th: Throwable =>
        logger.error(s"error occured while inserting row for refreshToken: ${row.getUUID("refreshtoken")}")
    }
  }

}