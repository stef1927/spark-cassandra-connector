package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.{Row, RowIterator}
import com.datastax.spark.connector.CassandraRowMetadata
import org.apache.spark.TaskContext

/**
  * A row iterator where pages are pushed automatically
  * by the server to the driver, which takes care of paging
  * behind the scenes.
  *
  * @param columnNames, the columns names
  * @param it, the driver auto fetching row iterator
  */
class AutoFetchingRowIterator(columnNames: IndexedSeq[String], it: RowIterator, context: TaskContext)
  extends Iterator[Row] with WithMetadata {

  context.addTaskCompletionListener(context => it.close())

  private lazy val _metadata = CassandraRowMetadata.fromColumnDefs(columnNames, it.getColumnDefinitions)

  override def hasNext: Boolean = it.hasNext

  override def next: Row = {
    val ret = it.next
    if (!it.hasNext) {
      // release memory early if the iterator is exhausted,
      // it is safe to call close multiple times and after
      // the iterator is closed, it.hasNext will simply return false
      it.close()
    }
    ret
  }

  override def metadata:  CassandraRowMetadata = _metadata
}