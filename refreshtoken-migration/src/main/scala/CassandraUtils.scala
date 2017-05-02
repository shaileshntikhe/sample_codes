package com.gslab.convergence

import com.datastax.driver.core.Cluster
import com.typesafe.config.ConfigFactory

/**
  * Created by GS-1159 on 29-04-2017.
  */
object CassandraUtils {

  private val config = ConfigFactory.load

  private val keyspace = config.getString("refreshtoken.keyspace")

  private val cassandraHost = config.getString("cassandra.host")

  private val cluster = Cluster.builder().addContactPoint(cassandraHost).build()

  private val session = cluster.connect(keyspace)

  def getSession = session

  def closeConnection = session.close()

  def closeCluster = cluster.close()
}