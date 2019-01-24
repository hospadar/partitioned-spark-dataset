package org.hospadaruk

import java.lang
import java.util.concurrent.Callable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionPath
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.{Before, Test}
import org.junit.Assert._

import scala.collection.JavaConverters._


class PartitionProviderTest {

  @Test
  def testMapToInternalRow = {
    val mapRow = Map(
      "string" -> "foob",
      "bool" -> "true",
      "int" -> "5",
      "float" -> "3.2",
      "long" -> "10",
      "double" -> "5.2"
    ).asJava

    val schema = DataTypes.createStructType(
      List(
        StructField("string", DataTypes.StringType),
        StructField("bool", DataTypes.BooleanType),
        StructField("int", DataTypes.IntegerType),
        StructField("float", DataTypes.FloatType),
        StructField("long", DataTypes.LongType),
        StructField("double", DataTypes.DoubleType)
      ).asJava
    )

    val internalRow = PartitionProvider.mapToInternalRow(schema, mapRow)

    assertEquals(
      "Check to make sure interal row is correct",
      InternalRow(
        UTF8String.fromString("foob"),
        true,
        5.asInstanceOf[Int],
        3.2.asInstanceOf[Float],
        10.asInstanceOf[Long],
        5.2.asInstanceOf[Double]
      ),
      internalRow
    )

    assertEquals(
      "Should be OK if fields are missing from the map",
      InternalRow(null, null, null, null, null, null),
      PartitionProvider.mapToInternalRow(schema, Map[String, String]().asJava)
    )
  }


  @Test(expected = classOf[NullPointerException])
  def testNonNullable:Unit = {
    def empty:String = null
    PartitionProvider.mapToInternalRow(
      StructType(Seq(StructField("thing", DataTypes.StringType, false))),
      Map("thing"->empty).asJava
    )
  }

  @Test(expected = classOf[NotImplementedError])
  def testBadType:Unit = {
    PartitionProvider.mapToInternalRow(
      StructType(
        Seq(
          StructField("foo", DataTypes.CalendarIntervalType, true)
        )
      ),
      Map("foo" -> "whatever").asJava
    )
  }


  val expectedPartitions = Seq(
    PartitionPath(
      InternalRow(UTF8String.fromString("1970-01-01"), 5),
      "/something"
    ),
    PartitionPath(
      InternalRow(UTF8String.fromString("1970-01-02"), 6),
      "/something/else"
    )
  )

  @Test
  def testFromMap = {
    val partitions = Map(
      Map("dt" -> "1970-01-01", "blah" -> "5").asJava -> "/something",
      Map("dt" -> "1970-01-02", "blah" -> "6").asJava -> "/something/else"
    )

    val p = PartitionProvider.fromMap(DataTypes.createStructType(List(
      StructField("dt", DataTypes.StringType, true),
      StructField("blah", DataTypes.IntegerType, true)
    ).asJava), partitions.asJava)

    val processed = p.getPartitions().toSeq.sortBy(p => p.path.toUri.toString)


    assertEquals(
      "Should produce the correct partitions",
      expectedPartitions,
      processed
    )
  }

  @Test
  def testFromIterable = {
    assertEquals(
      "Should produce the correct partitions",
      expectedPartitions,
      PartitionProvider.fromIterable(expectedPartitions.toIterable.asJava).getPartitions().toSeq.sortBy(p => p.path.toUri.toString)
    )
  }

  @Test
  def testFromCallable = {
    assertEquals(
      "Should produce the correct partitions from a Callable",
      expectedPartitions,
      PartitionProvider.fromCallable(new Callable[java.lang.Iterable[PartitionPath]] {
        override def call(): lang.Iterable[PartitionPath] = {
          expectedPartitions.toIterable.asJava
        }
      }).getPartitions().toSeq.sortBy(p => p.path.toUri.toString)
    )
  }

}
