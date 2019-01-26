package org.hospadaruk

import org.junit._
import Assert._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionPath
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.hospadaruk.StaticFileIndex

import scala.collection.JavaConverters._
import java.io.{File, FileOutputStream}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, ExprId, GreaterThanOrEqual, Literal}



class StaticFileIndexTest {

  val workingDir = new File(System.getProperty("user.dir")).getCanonicalPath
  val a1 = s"file:${workingDir}/temp/a1"
  val b2 = s"file:${workingDir}/temp/b2"
  val c3 = s"file:${workingDir}/temp/c3"

  @After
  def cleanupTemp():Unit = {
    val tempDir = new File(s"file://{workingDir}/temp/")

    if (tempDir.exists()) {
      if (tempDir.isDirectory) {
        FileUtils.deleteDirectory(tempDir)
      } else {
        tempDir.delete()
      }
    }
  }

  @Before
  def setup():Unit = {

    cleanupTemp()

    new File(s"${workingDir}/temp/a1").mkdirs()
    new File(s"${workingDir}/temp/b2").mkdirs()
    new File(s"${workingDir}/temp/c3").mkdirs()


    val parta1 = new File(s"${workingDir}/temp/a1/part-1")
    parta1.createNewFile()
    val outstream = new FileOutputStream(parta1)
    outstream.write("abcdefghij".getBytes("UTF-8"))
    outstream.close()

    new File(s"${workingDir}/temp/b2/part-1").createNewFile()
    new File(s"${workingDir}/temp/b2/part-2").createNewFile()

    new File(s"${workingDir}/temp/c3/part-1").createNewFile()
    new File(s"${workingDir}/temp/c3/part-2").createNewFile()
    new File(s"${workingDir}/temp/c3/part-3").createNewFile()
  }



  @Test
  def checkPartitioning():Unit = {


    val partitionProvider = PartitionProvider.fromIterable(Seq(
      PartitionPath(InternalRow(UTF8String.fromString("a"), 1), a1),
      PartitionPath(InternalRow(UTF8String.fromString("b"), 2), b2),
      PartitionPath(InternalRow(UTF8String.fromString("c"), 3), c3)
    ).toIterable.asJava)


    val fileIndex = new StaticFileIndex(
      new Configuration(),
      StructType(Seq(
        StructField("string_field", DataTypes.StringType, false),
        StructField("int_field", DataTypes.IntegerType, false)
      )),
      partitionProvider
    )

    val allFiles = Seq(s"$a1/part-1", s"$b2/part-1", s"$b2/part-2", s"$c3/part-1", s"$c3/part-2", s"$c3/part-3")

    assertEquals(
      "Should have the right rootPaths",
      Seq(a1, b2, c3),
      fileIndex.rootPaths.map(x => x.toString).sorted
    )

    assertEquals(
      "Should have the right schema",
      StructType(Seq(
        StructField("string_field", DataTypes.StringType, false),
        StructField("int_field", DataTypes.IntegerType, false)
      )),
      fileIndex.partitionSchema
    )

    assertEquals(
      "Should find the right size",
      10,
      fileIndex.sizeInBytes
    )



    assertEquals(
      "Should find the right files",
      allFiles.map(x => x.replace("file:/", "file:///")),
      fileIndex.inputFiles.toSeq.sorted
    )

    //let's try some different variations on getLeafFiles
    assertEquals(
      "Should get all files if we don't specify any filters",
      allFiles,
      fileIndex.listFiles(
        Seq(),
        Seq()
      ).flatMap(x => x.files.map(y => y.getPath.toString)).sorted
    )

    assertEquals(
      "Single filter on string field",
      Seq(s"$a1/part-1"),
      fileIndex.listFiles(
        Seq(EqualTo(AttributeReference("string_field", DataTypes.StringType)(), Literal("a"))),
        Seq()
      ).flatMap(x => x.files.map(y => y.getPath.toString)).sorted
    )

    assertEquals(
      "Filter on multiple fields",
      Seq(s"$b2/part-1", s"$b2/part-2"),
      fileIndex.listFiles(
        Seq(
          EqualTo(AttributeReference("string_field", DataTypes.StringType)(), Literal("b")),
          EqualTo(AttributeReference("int_field", DataTypes.IntegerType)(), Literal(2))
        ),
        Seq()
      ).flatMap(x => x.files.map(y => y.getPath.toString)).sorted
    )

    assertEquals(
      "Filter returning some, but not all partitions",
      Seq(s"$b2/part-1", s"$b2/part-2", s"$c3/part-1", s"$c3/part-2", s"$c3/part-3"),
      fileIndex.listFiles(
        Seq(GreaterThanOrEqual(AttributeReference("int_field", DataTypes.IntegerType)(), Literal(2))),
        Seq()
      ).flatMap(x=>x.files.map(y=>y.getPath.toString)).sorted
    )
  }
}

