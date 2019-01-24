package org.hospadaruk

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

import scala.collection.mutable.ListBuffer

class StaticFileIndex(
  hadoopConf: Configuration,
  schema: StructType,
  partitionProvider: PartitionProvider,
  fileStatusCache: FileStatusCache = NoopCache)
  extends FileIndex with Logging {

  private var currentPartitions = partitionProvider.getPartitions()

  override def rootPaths: Seq[Path] = {currentPartitions.map(partition => {partition.path}).toSeq}

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    //if there are no partition columns, just return everything
    if (schema.isEmpty) {
      List(PartitionDirectory(InternalRow.empty, allFiles()))
    } else {
      prunePartitions(partitionFilters, PartitionSpec(schema, currentPartitions.toSeq)).map(partition => {
        PartitionDirectory(partition.values, getLeafFiles(partition.path))
      })
    }

  }

  /*
  Get the partitions we care about.  Lifted directly from PartitionAwareFileIndex
   */
  private def prunePartitions(predicates: Seq[Expression], partitionSpec: PartitionSpec): Seq[PartitionPath] = {
    val PartitionSpec(partitionColumns, partitions) = partitionSpec
    val partitionColumnNames = partitionColumns.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }

    if (partitionPruningPredicates.nonEmpty) {
      val predicate = partitionPruningPredicates.reduce(expressions.And)

      val boundPredicate = InterpretedPredicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      })

      val selected = partitions.filter {
        case PartitionPath(values, _) => boundPredicate.eval(values)
      }
      logInfo {
        val total = partitions.length
        val selectedSize = selected.length
        val percentPruned = (1 - selectedSize.toDouble / total.toDouble) * 100
        s"Selected $selectedSize partitions out of $total, " +
          s"pruned ${if (total == 0) "0" else s"$percentPruned%"} partitions."
      }

      selected
    } else {
      partitions
    }
  }

  /** Returns the list of files that will be read when scanning this relation. */
  override def inputFiles: Array[String] =
    allFiles().map(_.getPath.toUri.toString).toArray

  override def sizeInBytes: Long = allFiles().map(_.getLen).sum

  protected def allFiles(): Seq[FileStatus] = {
    val leafFiles = new ListBuffer[FileStatus]()
    for (partition <- currentPartitions){
      leafFiles.prependAll(getLeafFiles(partition.path))
    }
    leafFiles
  }

  private def getLeafFiles(partitionPath:Path):Seq[FileStatus] = {
    val cachedFiles:Option[Array[FileStatus]] = fileStatusCache.getLeafFiles(partitionPath)
    cachedFiles match {
      case Some(files) => files
      case None => {
        val leafFilesThisDirectory = new ListBuffer[FileStatus]()
        val fs = partitionPath.getFileSystem(hadoopConf)
        val fileListing = fs.listFiles(partitionPath, true)
        while (fileListing.hasNext) {
          leafFilesThisDirectory += fileListing.next()
        }
        leafFilesThisDirectory
      }
    }
  }

  override def refresh(): Unit = {
    fileStatusCache.invalidateAll()
    currentPartitions = partitionProvider.getPartitions()
  }

  override def partitionSchema: StructType = schema
}

