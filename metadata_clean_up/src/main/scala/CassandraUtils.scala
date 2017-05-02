package com.gslab.convergence

import com.datastax.driver.core.Cluster
import com.typesafe.config.ConfigFactory

/**
  * Created by GS-1159 on 30-04-2017.
  */

object CassandraUtils extends ConfigReader {

  private val keyspace = config.getString("vcs.keyspace")

  private val cassandraHost = config.getString("cassandra.host")

  private val cluster = Cluster.builder().addContactPoint(cassandraHost).build()

  private val session = cluster.connect(keyspace)

  def getSession = session

  def closeConnection = session.close()

  def closeCluster = cluster.close()
}