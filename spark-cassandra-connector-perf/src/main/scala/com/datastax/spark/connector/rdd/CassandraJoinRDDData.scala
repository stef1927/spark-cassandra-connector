package com.datastax.spark.connector.rdd

import java.lang.{Long => JLong}

import com.datastax.driver.core.{ResultSet, ResultSetFuture, Session}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.SparkTemplate
import com.datastax.spark.connector.rdd.CassandraJoinRDDBenchmark._
import com.datastax.spark.connector.writer.AsyncExecutor

object CassandraJoinRDDData extends App with SparkTemplate {

  val config = defaultConf.set("spark.cassandra.connection.port", "9042")

  val someContent =
    """Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam quis augue
      tristique, ultricies nisl sit amet, condimentum felis. Ut blandit nisi eget imperdiet pretium. Ut a
      erat in tortor ultrices posuere quis eu dui. Lorem ipsum dolor sit amet, consectetur adipiscing
      elit. Integer vestibulum vitae arcu ac vehicula. Praesent non erat quis ipsum tempor tempus
      vitae quis neque. Aenean eget urna egestas, lobortis velit sed, vestibulum justo. Nam nibh
      risus, bibendum non ex ac, bibendum varius purus. """

  def createKeyspaceCql(name: String) =
    s"""
       |CREATE KEYSPACE IF NOT EXISTS $name
       |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
       |AND durable_writes = false
       |""".stripMargin

  def createTableCql(keyspace: String, tableName: String): String =
    s"""
       |CREATE TABLE $keyspace.$tableName (
       |  key INT,
       |  group BIGINT,
       |  value TEXT,
       |  PRIMARY KEY (key, group)
       |)""".stripMargin

  def insertData(session: Session, keyspace: String, table: String, partitions: Int,
    partitionSize: Int): Unit = {
    def executeAsync(data: (Int, Long, String)): ResultSetFuture = {
      session.executeAsync(
        s"INSERT INTO $ks.$table (key, group, value) VALUES (?, ?, ?)",
        data._1: Integer, data._2.toLong: JLong, data._3.toString
      )
    }
    val executor = new AsyncExecutor[(Int, Long, String), ResultSet](executeAsync, 1000, None, None)

    for (
      partition <- 0 until partitions;
      group <- 0 until partitionSize
    ) yield executor.executeAsync((partition, group, s"${partition}_${group}_${someContent}"))
    executor.waitForCurrentlyExecutingTasks()
  }

  val conn = CassandraConnector(config)
  conn.withSessionDo { session =>
    session.execute(s"DROP KEYSPACE IF EXISTS $ks")
    session.execute(createKeyspaceCql(ks))

    session.execute(createTableCql(ks, "one_element_partitions"))
    session.execute(createTableCql(ks, "small_partitions"))
    session.execute(createTableCql(ks, "moderate_partitions"))
    session.execute(createTableCql(ks, "big_partitions"))

    insertData(session, ks, "one_element_partitions", rows, 1)
    insertData(session, ks, "small_partitions", rows / smallPartitionSize, smallPartitionSize)
    insertData(session, ks, "moderate_partitions", rows/ moderatePartitionSize, moderatePartitionSize)
    insertData(session, ks, "big_partitions", rows / bigPartitionSize, bigPartitionSize)
  }
}
