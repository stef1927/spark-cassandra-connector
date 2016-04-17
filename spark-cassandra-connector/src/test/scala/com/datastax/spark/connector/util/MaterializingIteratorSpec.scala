package com.datastax.spark.connector.util

import java.util.NoSuchElementException

import scala.collection.mutable

import org.scalatest.{FlatSpec, Matchers}

class MaterializingIteratorSpec extends FlatSpec with Matchers {

  trait Fixture {
    val queue = mutable.Queue("a", "b", "c")
    val queuePoppingIt = (1 to queue.size).iterator.map(_ => queue.dequeue())
  }

  "MaterializingIterator" should "not materialize on iterator creation" in new Fixture {
    new MaterializingIterator(queuePoppingIt, 3)

    queue.size should be(3)
  }

  it should "materialize on next invocation" in new Fixture {
    new MaterializingIterator(queuePoppingIt, 3).next()

    queue shouldBe 'isEmpty
  }

  it should "materialize not more than available" in new Fixture {
    new MaterializingIterator(queuePoppingIt, 16).next()

    queue shouldBe 'isEmpty
  }

  it should "materialize on every next" in new Fixture {
    val it = new MaterializingIterator(queuePoppingIt, 1)

    it.next() should be("a")
    queue.size should be(2)

    it.next() should be("b")
    queue.size should be(1)

    it.next() should be("c")
    queue.size should be(0)

    intercept[NoSuchElementException] {
      it.next()
    }
  }

  it should "should throw when wrapping empty iterator" in {
    val it = new MaterializingIterator(Iterator.empty, 500)

    intercept[NoSuchElementException] {
      it.next()
    }
  }
}
