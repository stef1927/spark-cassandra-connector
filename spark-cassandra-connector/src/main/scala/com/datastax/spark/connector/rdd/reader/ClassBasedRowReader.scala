package com.datastax.spark.connector.rdd.reader

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

import org.apache.spark.sql.catalyst.ReflectionLock.SparkReflectionLock

import com.datastax.driver.core.{TypeCodec, Row}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper._
import com.datastax.spark.connector.util.JavaApiHelper


/** Transforms a Cassandra Java driver `Row` into an object of a user provided class,
  * calling the class constructor */
final class ClassBasedRowReader[R : TypeTag : ColumnMapper](
    table: TableDef,
    selectedColumns: IndexedSeq[ColumnRef])
  extends RowReader[R] {

  private val converter =
    new GettableDataToMappedTypeConverter[R](table, selectedColumns)

  private val isReadingTuples =
    SparkReflectionLock.synchronized(typeTag[R].tpe.typeSymbol.fullName startsWith "scala.Tuple")

  override val neededColumns = {
    val ctorRefs = converter.columnMap.constructor
    val setterRefs = converter.columnMap.setters.values
    Some(ctorRefs ++ setterRefs)
  }


  private lazy val _indexOf = converter.columnMap.constructor.map(_.columnName)
                                                             .zipWithIndex
                                                             .toMap
                                                             .withDefault(name => {
    val columnNames =  converter.columnMap.constructor.map(_.columnName)
    throw new ColumnNotFoundException(
      s"Column not found: $name. " +
        s"Available columns are: ${columnNames.mkString("[", ", ", "]")}")})

  private lazy val _data = new Array[AnyRef](converter.columnMap.constructor.length)

  private var _cachedIndexes = Array[Int]()

  override def read(row: Row, columnNames: Array[String], codecs: Array[TypeCodec[AnyRef]]): R = {

    // If we must use GettableData for the setters, then we need to change it so that GettableData._indexOf is
    // not rebuilt for every row
    //    class GettableDriverRow(val columnNames: IndexedSeq[String], val columnValues: IndexedSeq[AnyRef])
    //      extends GettableData {}
    //
    //    val data = new Array[AnyRef](columnNames.length)
    //    for (i <- columnNames.indices)
    //      data(i) = GettableData.get(row, i, codecs(i))

    // Here we use a GettableByIndexData and we therefore no longer invoke
    // the setters in GettableDataToMappedTypeConverter, this might be a problem.
    // We arrange the values in the correct order that the mapper expects them in.
    // We could improve this by caching the indexes rather than accessing the map each time
    // We also reuse the same Array to avoid creating garbage, even better would be
    // to get rid of it altogether or pass it directly to the converter
    class GettableDriverRow(val columnValues: IndexedSeq[AnyRef])
      extends GettableByIndexData {}

    assert(columnNames.length == _data.length)

    if (_cachedIndexes.isEmpty) {
      // This is a bit of a hack and it shaves maybe 1 second every 5M rows iterated
      _cachedIndexes = columnNames.indices.map(i => _indexOf(columnNames(i))).toArray[Int]
    }

    // Here we use a mutable while loop for performance reasons, scala for loops are
    // converted into range.foreach() and the JVM is unable to inline the foreach closure
    // the alternative would be a tail recursive method
    var i = 0
    while (i < columnNames.length) {
      //val idx = _indexOf(columnNames(i))
      val idx = _cachedIndexes(i)
      _data(idx) = GettableData.get(row, i, codecs(i))
      i += 1
    }

    converter.convert(new GettableDriverRow(_data))
  }
}

class ClassBasedRowReaderFactory[R : TypeTag : ColumnMapper] extends RowReaderFactory[R] {

  def columnMapper = implicitly[ColumnMapper[R]]

  override def rowReader(tableDef: TableDef, selection: IndexedSeq[ColumnRef]) =
    new ClassBasedRowReader[R](tableDef, selection)

  override def targetClass: Class[R] = JavaApiHelper.getRuntimeClass(typeTag[R])
}
