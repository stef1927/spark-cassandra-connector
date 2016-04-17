package com.datastax.spark.connector.rdd

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import com.datastax.driver.core.Session
import com.datastax.spark.connector._
import com.datastax.spark.connector.embedded.SparkTemplate
import com.datastax.spark.connector.rdd.CassandraJoinRDDBenchmark._

/**
  * Performs a macrobenchmark of fetching and joining C* table data with given "left" iterator. This
  * is used in [[RDDFunctions.joinWithCassandraTable]] operation.
  * External C* cluster should be started before benchmark execution.
  */
@State(Scope.Benchmark)
class CassandraJoinRDDBenchmark {

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(jvmArgs = Array("-Xmx4096m", "-XX:MaxDirectMemorySize=1024m"))
  def measureOneElementPerPartitionsJoin(f: ModeratePartitionsFixture): List[_] = {
    val it = f.rdd.fetchIterator(f.session, f.rdd.boundStatementBuilder(f.session), f.left.iterator)
    it.toList
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(jvmArgs = Array("-Xmx4096m", "-XX:MaxDirectMemorySize=1024m"))
  def measureSmallPartitionsJoin(f: ModeratePartitionsFixture): List[_] = {
    val it = f.rdd.fetchIterator(f.session, f.rdd.boundStatementBuilder(f.session), f.left.iterator)
    it.toList
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(jvmArgs = Array("-Xmx4096m", "-XX:MaxDirectMemorySize=1024m"))
  def measureModeratePartitionsJoin(f: ModeratePartitionsFixture): List[_] = {
    val it = f.rdd.fetchIterator(f.session, f.rdd.boundStatementBuilder(f.session), f.left.iterator)
    it.toList
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(jvmArgs = Array("-Xmx4096m", "-XX:MaxDirectMemorySize=1024m"))
  def measureBigPartitionsJoin(f: BigPartitionsFixture): List[_] = {
    val it = f.rdd.fetchIterator(f.session, f.rdd.boundStatementBuilder(f.session), f.left.iterator)
    it.toList
  }
}

object CassandraJoinRDDBenchmark {
  val ks = "spark_connector_join_performance_test"

  val rows = 2000000
  val smallPartitionSize = 10
  val moderatePartitionSize = 500
  val bigPartitionSize = 5000

  @State(Scope.Benchmark)
  class Fixture(partitionSize: Int, table: String) extends SparkTemplate {

    def NormalState() {}

    val config = defaultConf.set("spark.cassandra.connection.port", "9042")
      .set("spark.cassandra.input.join.concurrent.reads", "5")
    useSparkConf(config)

    var left: Seq[Tuple1[Int]] = _
    var rdd: CassandraJoinRDD[Tuple1[Int], CassandraRow] = _
    var session: Session = _

    @Setup(Level.Trial)
    def init(): Unit = {
      left = (0 until rows / partitionSize).map(Tuple1(_))
      rdd = sc.parallelize(left).joinWithCassandraTable(ks, table)
      session = rdd.connector.openSession()
    }

    @TearDown(Level.Trial)
    def tearDown(): Unit = {
      session.close()
      sc.stop()
    }
  }

  @State(Scope.Benchmark)
  class OneElementPerPartitionsFixture extends Fixture(moderatePartitionSize, "one_element_partitions")

  @State(Scope.Benchmark)
  class SmallPartitionsFixture extends Fixture(moderatePartitionSize, "small_partitions")

  @State(Scope.Benchmark)
  class ModeratePartitionsFixture extends Fixture(moderatePartitionSize, "moderate_partitions")

  @State(Scope.Benchmark)
  class BigPartitionsFixture extends Fixture(bigPartitionSize, "big_partitions")

}