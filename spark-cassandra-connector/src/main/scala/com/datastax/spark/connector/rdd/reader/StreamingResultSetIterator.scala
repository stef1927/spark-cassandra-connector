package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.{ResultSetFuture, Row, ResultSet}


/** Allows iteration overmultiple pages.
  *
  * @param futures an iterator of ResultSetFuture, each future will deliver a result set
  */
class StreamingResultSetIterator(futures: Iterator[ResultSetFuture]) extends Iterator[Row] {

  private lazy val iterators : Iterator[Row] = futures.map(rsf => rsf.get())
    .flatMap(rs => Seq.fill(rs.getAvailableWithoutFetching)(rs.iterator.next).toIterator)

  override def hasNext: Boolean = iterators.hasNext
  override def next: Row = iterators.next
}