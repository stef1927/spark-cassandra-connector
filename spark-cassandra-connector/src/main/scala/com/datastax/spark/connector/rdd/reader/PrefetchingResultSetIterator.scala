package com.datastax.spark.connector.rdd.reader

import java.util.concurrent.TimeUnit

import com.codahale.metrics.Timer
import com.datastax.driver.core.{ResultSet, Row}
import com.datastax.spark.connector.CassandraRowMetadata
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

/** Allows to efficiently iterate over a large, paged ResultSet,
  * asynchronously prefetching the next page.
  * 
  * @param _resultSet result set obtained from the Java driver
  * @param prefetchWindowSize if there are less than this rows available without blocking,
  *                           initiates fetching the next page
  * @param timer a Codahale timer to optionally gather the metrics of fetching time
  */
class PrefetchingResultSetIterator(columnNames: IndexedSeq[String],
                                   _resultSet: ResultSet,
                                   prefetchWindowSize: Int,
                                   timer: Option[Timer] = None) extends Iterator[Row] with WithMetadata {

  private[this] val iterator = _resultSet.iterator()
  private lazy val _metadata = CassandraRowMetadata.fromResultSet(columnNames, _resultSet)

  override def hasNext: Boolean = iterator.hasNext

  private[this] def maybePrefetch(): Unit = {
    if (!_resultSet.isFullyFetched && _resultSet.getAvailableWithoutFetching < prefetchWindowSize) {
      val t0 = System.nanoTime()
      val future: ListenableFuture[ResultSet] = _resultSet.fetchMoreResults()
      if (timer.isDefined) {
        Futures.addCallback(future, new FutureCallback[ResultSet] {
          override def onSuccess(ignored: ResultSet): Unit = {
            timer.get.update(System.nanoTime() - t0, TimeUnit.NANOSECONDS)
          }

          override def onFailure(ignored: Throwable): Unit = {}
        })
      }
    }
  }

  override def next: Row = {
    maybePrefetch()
    iterator.next()
  }

  override def metadata: CassandraRowMetadata = _metadata
}
