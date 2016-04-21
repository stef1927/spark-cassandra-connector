import java.util

import collection.JavaConverters._
import com.datastax.driver.core._
import com.datastax.spark.connector.rdd.reader.MultiPageResultSetIterator
import com.google.common.util.concurrent.ListenableFuture


class ResultSetImpl(numRows: Int) extends ResultSet {
  override def one(): Row = ???

  override def getExecutionInfo: ExecutionInfo = ???

  override def wasApplied(): Boolean = ???

  override def fetchMoreResults(): ListenableFuture[ResultSet] = ???

  override def isExhausted: Boolean = ???

  override def getColumnDefinitions: ColumnDefinitions = ???

  override def all(): util.List[Row] = ???

  override def isFullyFetched: Boolean = ???

  override def fetchRemainingResults: ResultSetIterator = ???

  override def pagingOptions: AsyncPagingOptions = ???

  override def iterator(): java.util.Iterator[Row] = {
    class IteratorImpl() extends Iterator[Row] {
      private val rows = Seq.fill(numRows)(new RowMock(Option.empty)).toIterator
      override def hasNext: Boolean = rows.hasNext
      override def next: Row = rows.next
    }
    new IteratorImpl().asJava
  }

  override def getAllExecutionInfo: util.List[ExecutionInfo] = ???

  override def getAvailableWithoutFetching: Int = numRows
}


val NUM_PAGES = 10
val NUM_ROWS = 100
val results = Seq.fill(NUM_PAGES)(new ResultSetImpl(NUM_ROWS))
val it = new MultiPageResultSetIterator(results.toIterator)
it.resultSet
it.resultSet.get.getAvailableWithoutFetching
val numRows = it.count(r => true)
val success = numRows == NUM_PAGES * NUM_ROWS

val empty = new MultiPageResultSetIterator(Iterator.empty)
empty.resultSet