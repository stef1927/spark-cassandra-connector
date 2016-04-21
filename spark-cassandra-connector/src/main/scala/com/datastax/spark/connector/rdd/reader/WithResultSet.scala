package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.ResultSet

/**
  * A simple trait that retuns a resultSet
  */
trait WithResultSet {
  def resultSet() : Option[ResultSet]
}
