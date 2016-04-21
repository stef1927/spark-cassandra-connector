package com.datastax.spark.connector.rdd.reader

import com.datastax.spark.connector.CassandraRowMetadata

/**
  * A simple trait that can return the metadata
  */
trait WithMetadata {
  def metadata() : CassandraRowMetadata
}
