package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.{ResultSet, Row}

/** Allows iteration over multiple pages.
  *
  * @param it an iterator of result sets, each one is a page
  */
class MultiPageResultSetIterator(it: Iterator[ResultSet]) extends Iterator[Row] with WithResultSet {

  private lazy val first = if (it.hasNext) Some(it.next()) else None

  private lazy val iterators : Iterator[Row] = (if (first.isEmpty) it else Iterator(first.get) ++ it)
    .flatMap(rs => Seq.fill(rs.getAvailableWithoutFetching)(rs.iterator.next).toIterator)

  override def hasNext: Boolean = iterators.hasNext
  override def next: Row = iterators.next

  override def resultSet: Option[ResultSet] = first
}