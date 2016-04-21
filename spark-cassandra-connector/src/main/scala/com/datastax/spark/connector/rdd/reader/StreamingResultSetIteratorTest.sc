import java.util
import java.util.concurrent.{Executor, TimeUnit}

import collection.JavaConverters._

import com.datastax.driver.core._
import com.datastax.spark.connector.rdd.reader.StreamingResultSetIterator
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

  override def iterator(): java.util.Iterator[Row] = {
    class IteratorImpl() extends Iterator[Row] {
      private val rows = Seq.fill(numRows)(new RowMock(Option.empty)).toIterator
      override def hasNext: Boolean = rows.hasNext
      override def next: Row = rows.next
    }

    return new IteratorImpl().asJava
  }

  override def getAllExecutionInfo: util.List[ExecutionInfo] = ???

  override def getAvailableWithoutFetching: Int = numRows
}

class ResultSetFutureImpl(numRows: Int) extends ResultSetFuture {

  override def get: ResultSet = {
    println("Future get is called")
    new ResultSetImpl(numRows)
  }

  override def isCancelled: Boolean = false

  override def cancel(mayInterruptIfRunning: Boolean): Boolean = ???

  override def getUninterruptibly: ResultSet = new ResultSetImpl(numRows)

  override def getUninterruptibly(timeout: Long, unit: TimeUnit): ResultSet = new ResultSetImpl(numRows)

  override def addListener(listener: Runnable, executor: Executor): Unit = ???

  override def get(timeout: Long, unit: TimeUnit): ResultSet = new ResultSetImpl(numRows)

  override def isDone: Boolean = false
}
val futures = Seq.fill(10)(new ResultSetFutureImpl(10)).toIterator
val it = new StreamingResultSetIterator(futures)
var numRows = 0
while (it.hasNext) {
  numRows += 1
  println(s"Got row $numRows")
  it.next
}
println(s"Got $numRows rows\n")