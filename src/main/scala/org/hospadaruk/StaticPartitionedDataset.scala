package org.hospadaruk

import java.util.concurrent.Callable

import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FileFormat, FileStatusCache, HadoopFsRelation, PartitionPath}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._


class StaticPartitionedDataset {

}

object StaticPartitionedDataset {

  def createDataset(session:SparkSession,
                            dataSchema:StructType,
                            partitionSchema:StructType,
                            fileFormat: FileFormat,
                            partitionProvider:PartitionProvider,
                            options: java.util.Map[String, String]
                           ):Dataset[Row] = {

    val dataColumns = dataSchema.fields.map(x => x.name).toSet
    val partitionColumns = partitionSchema.fields.map(x => x.name).toSet
    val overlapColumns = dataColumns.intersect(partitionColumns)
    if (overlapColumns.nonEmpty){
      throw new Exception(s"Cannot create dataframe with overlapping data/partition columns. $overlapColumns are defined as both data and partition columns.")
    }

    val index = new StaticFileIndex(
      session.sparkContext.hadoopConfiguration,
      partitionSchema,
      partitionProvider,
      FileStatusCache.getOrCreate(session)
    )
    val relation = HadoopFsRelation(
      index,
      partitionSchema,
      dataSchema,
      None,
      fileFormat,
      options.asScala.toMap
    )(session)

    session.baseRelationToDataFrame(relation.asInstanceOf[BaseRelation])
  }

  def createDataset(session:SparkSession,
                    dataSchema:StructType,
                    partitionSchema:StructType,
                    fileFormat: FileFormat,
                    partitions:java.util.Map[java.util.Map[java.lang.String, java.lang.String], java.lang.String],
                    options: java.util.Map[String, String]
                   ):Dataset[Row] = {
    val partitionProvider = PartitionProvider.fromMap(partitionSchema, partitions)
    createDataset(session, dataSchema, partitionSchema, fileFormat, partitionProvider, options)
  }

  def createDataset(session:SparkSession,
                         dataSchema:StructType,
                         partitionSchema:StructType,
                         fileFormat: FileFormat,
                         callable:Callable[java.lang.Iterable[PartitionPath]],
                         options: java.util.Map[String, String]
                        ):Dataset[Row] = {
    val partitionProvider = PartitionProvider.fromCallable(callable)
    createDataset(session, dataSchema, partitionSchema, fileFormat, partitionProvider, options)
  }

  def createDataset(session:SparkSession,
                         dataSchema:StructType,
                         partitionSchema:StructType,
                         fileFormat: FileFormat,
                         partitions:java.lang.Iterable[PartitionPath],
                         options: java.util.Map[String, String]
                        ):Dataset[Row] = {
    val partitionProvider = PartitionProvider.fromIterable(partitions)
    createDataset(session, dataSchema, partitionSchema, fileFormat, partitionProvider, options)
  }
}