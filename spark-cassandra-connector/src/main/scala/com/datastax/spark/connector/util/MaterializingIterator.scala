package com.datastax.spark.connector.util

import java.util.NoSuchElementException

import scala.collection.mutable

/** Materializes/Fetches up to `materializationLimit` from underlying `iterator`.
  * Materialization happens on `next()` invocation.
  *
  * In following example `it.next()` is invoked three times, results are caches in internal queue.
  * {{{
  * new MaterializingIterator(it, 3).next()
  * }}}
  *
  */
class MaterializingIterator[T](iterator: Iterator[T], materializationLimit: Int) extends Iterator[T] {

  assert(materializationLimit > 0, "Materialization limit must be greater than 0")

  private val materialized: mutable.Queue[T] = new mutable.Queue[T]()

  private def materialize(): Unit = {
    while (iterator.hasNext && materialized.size < materializationLimit) {
      materialized.enqueue(iterator.next)
    }
  }

  override def hasNext = !materialized.isEmpty || iterator.hasNext

  override def next() = {
    if(!hasNext) {
      throw new NoSuchElementException("No more elements")
    }
    materialize()
    materialized.dequeue()
  }
}
