package org.hospadaruk

import java.io.File
import java.{lang, util}
import java.util.UUID
import java.util.concurrent.Callable

import org.junit.{After, Assert, Before, Test}
import Assert._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionPath
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

class StaticPartitionedDatasetTest {

  val tempdir = s"${new File(System.getProperty("user.dir")).getCanonicalPath}/temp-parquet/"
  val p1 = s"$tempdir/p1_${UUID.randomUUID().toString}"
  val p2 = s"$tempdir/p2_${UUID.randomUUID().toString}"

  private val allRows = Seq(
    Row("thing1", "p1"),
    Row("thing2", "p1"),
    Row("thing3", "p2"),
    Row("thing4", "p2")
  )

  private val p1Rows = Seq(
    Row("thing1", "p1"),
    Row("thing2", "p1")
  )

  private val evenRows = Seq(
    Row("thing2", "p1"),
    Row("thing4", "p2")
  )

  private val dataSchema = StructType(Seq(StructField("a", DataTypes.StringType, true)))
  private val partitionSchema = StructType(Seq(StructField("b", DataTypes.StringType, true)))
  private val partitionRows = Seq(
    PartitionPath(InternalRow(UTF8String.fromString("p1")), p1),
    PartitionPath(InternalRow(UTF8String.fromString("p2")), p2)
  ).toIterable.asJava

  @Before
  def setup():Unit = {
    cleanup()
    val sparkSession = SparkSession.builder()
      .appName("test static partitioned dataset")
      .master("local[*]")
      .getOrCreate()

    sparkSession.sql("SELECT 'thing1' as a UNION ALL SELECT 'thing2' as a").write.parquet(p1)
    sparkSession.sql("SELECT 'thing3' as a UNION ALL SELECT 'thing4' as a").write.parquet(p2)
  }

  @After
  def cleanup():Unit = {
    val tempfile = new File(tempdir)
    if (tempfile.exists()) {
      if (tempfile.isDirectory){
        FileUtils.deleteDirectory(tempfile)
      } else if (tempfile.isFile) {
        tempfile.delete()
      }
    }

    SparkSession.getActiveSession match {
      case Some(session) => session.stop()
      case _ => {}
    }
  }

  @Test
  def testFromMap():Unit = {

    val dataframe = StaticPartitionedDataset.createDataset(
      SparkSession.getActiveSession.get,
      StructType(Seq(StructField("a", DataTypes.StringType, true))),
      StructType(Seq(StructField("b", DataTypes.StringType, true))),
      new ParquetFileFormat(),
      Map(
        Map("b" -> "p1").asJava -> p1,
        Map("b" -> "p2").asJava -> p2
      ).asJava,
      new util.HashMap[String, String]()
    )

    assertEquals(
      "Do we get the right combined schema?",
      StructType(
        Seq(
          StructField("a", DataTypes.StringType, true),
          StructField("b", DataTypes.StringType, true)
        )
      ),
      dataframe.schema
    )

    assertEquals(
      "Should get all the rows back",
      allRows,
      dataframe.collect().toSeq.sortBy(x => x.get(0).asInstanceOf[String])
    )

    assertEquals(
      p1Rows,
      dataframe.filter("b = 'p1'").collect().toSeq.sortBy(x => x.get(0).asInstanceOf[String])
    )

    assertEquals(
      evenRows,
      dataframe.filter("a in ('thing2', 'thing4')").collect().toSeq.sortBy(x => x.get(0).asInstanceOf[String])
    )
  }

  @Test
  def testFromCallable():Unit = {

    val dataframe = StaticPartitionedDataset.createDataset(
      SparkSession.getActiveSession.get,
      dataSchema,
      partitionSchema,
      new ParquetFileFormat(),
      new Callable[java.lang.Iterable[PartitionPath]] {
        override def call(): lang.Iterable[PartitionPath] = partitionRows
      },
      new util.HashMap[String, String]()
    )

    assertEquals(
      "Do we get the right combined schema?",
      StructType(
        Seq(
          StructField("a", DataTypes.StringType, true),
          StructField("b", DataTypes.StringType, true)
        )
      ),
      dataframe.schema
    )

    assertEquals(
      "Should get all the rows back",
      allRows,
      dataframe.collect().toSeq.sortBy(x => x.get(0).asInstanceOf[String])
    )

    assertEquals(
      p1Rows,
      dataframe.filter("b = 'p1'").collect().toSeq.sortBy(x => x.get(0).asInstanceOf[String])
    )

    assertEquals(
      evenRows,
      dataframe.filter("a in ('thing2', 'thing4')").collect().toSeq.sortBy(x => x.get(0).asInstanceOf[String])
    )

  }

  @Test
  def testFromIterable():Unit = {
    val dataframe = StaticPartitionedDataset.createDataset(
      SparkSession.getActiveSession.get,
      dataSchema,
      partitionSchema,
      new ParquetFileFormat(),
      partitionRows,
      new util.HashMap[String, String]()
    )

    assertEquals(
      "Do we get the right combined schema?",
      StructType(
        Seq(
          StructField("a", DataTypes.StringType, true),
          StructField("b", DataTypes.StringType, true)
        )
      ),
      dataframe.schema
    )

    assertEquals(
      "Should get all the rows back",
      allRows,
      dataframe.collect().toSeq.sortBy(x => x.get(0).asInstanceOf[String])
    )

    assertEquals(
      p1Rows,
      dataframe.filter("b = 'p1'").collect().toSeq.sortBy(x => x.get(0).asInstanceOf[String])
    )

    assertEquals(
      evenRows,
      dataframe.filter("a in ('thing2', 'thing4')").collect().toSeq.sortBy(x => x.get(0).asInstanceOf[String])
    )

  }

  @Test
  def testFromPartitionProvider():Unit = {
    val dataframe = StaticPartitionedDataset.createDataset(
      SparkSession.getActiveSession.get,
      dataSchema,
      partitionSchema,
      new ParquetFileFormat(),
      PartitionProvider.fromIterable(partitionRows),
      new util.HashMap[String, String]()
    )

    assertEquals(
      "Do we get the right combined schema?",
      StructType(
        Seq(
          StructField("a", DataTypes.StringType, true),
          StructField("b", DataTypes.StringType, true)
        )
      ),
      dataframe.schema
    )

    assertEquals(
      "Should get all the rows back",
      allRows,
      dataframe.collect().toSeq.sortBy(x => x.get(0).asInstanceOf[String])
    )

    assertEquals(
      p1Rows,
      dataframe.filter("b = 'p1'").collect().toSeq.sortBy(x => x.get(0).asInstanceOf[String])
    )

    assertEquals(
      evenRows,
      dataframe.filter("a in ('thing2', 'thing4')").collect().toSeq.sortBy(x => x.get(0).asInstanceOf[String])
    )

  }

  @Test
  def testOverlapColumnError():Unit = {
    try {
      val dataframe = StaticPartitionedDataset.createDataset(
        SparkSession.getActiveSession.get,
        StructType(Seq(
          StructField("a", DataTypes.StringType, true),
          StructField("b", DataTypes.StringType, true)
        )),
        StructType(Seq(
          StructField("b", DataTypes.StringType, true),
          StructField("c", DataTypes.StringType, true)
        )),
        new ParquetFileFormat(),
        PartitionProvider.fromIterable(partitionRows),
        new util.HashMap[String, String]()
      )
      fail("Should have thrown an error!")
    } catch {
      case ex:Exception => assertEquals("Cannot create dataframe with overlapping data/partition columns. Set(b) are defined as both data and partition columns.", ex.getMessage)
      case _ => fail("got the wrong message!")
    }
  }
}
