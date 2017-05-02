package com.gslab.convergence

/**
  * Created by GS-1159 on 29-04-2017.
  */
object Runner extends App with Logging {
  logger.info(s"app started")
  RefreshToken.copy()
  logger.info(s"copying completed, closing cassandra connection")
  CassandraUtils.closeCluster
  CassandraUtils.closeConnection
  logger.info(s"app completed")
}