package org.hospadaruk

import java.util.concurrent.Callable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{PartitionDirectory, PartitionPath}
import org.apache.spark.sql.types.{BooleanType, CharType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

abstract class PartitionProvider {

  def getPartitions():Iterable[PartitionPath]

}

object PartitionProvider {
  def mapToInternalRow(partitionSchema:StructType,
                       partitionId:java.util.Map[java.lang.String, java.lang.String]):InternalRow = {



    InternalRow(
      partitionSchema.map(field => {
        val fieldVal = partitionId.getOrDefault(field.name, null)
        if (fieldVal == null && !field.nullable) {
          throw new NullPointerException(s"Found null field value for partition column '${field.name}' in partition '${partitionId}' ('${field.name}' does not allow null values)")
          null
        }
        else if (null == fieldVal){
          null
        } else {
          field.dataType match {
            case StringType => UTF8String.fromString(fieldVal) //Probably gonna need to cast this to something?
            case IntegerType => java.lang.Integer.parseInt(fieldVal)
            case LongType => java.lang.Long.parseLong(fieldVal)
            case FloatType => java.lang.Float.parseFloat(fieldVal)
            case DoubleType => java.lang.Double.parseDouble(fieldVal)
            case BooleanType => java.lang.Boolean.parseBoolean(fieldVal)
            case _ => throw new NotImplementedError(s"Got data type ${field.dataType} for partition column '${field.name}', but we don't support parsing that type.")
          }
        }
      }):_*
    )
  }

  def fromCallable(callable:Callable[java.lang.Iterable[PartitionPath]]):PartitionProvider = {
    new PartitionProvider {
      override def getPartitions(): Iterable[PartitionPath] = {
        callable.call().asScala
      }
    }
  }

  def fromIterable(partitions:java.lang.Iterable[PartitionPath]):PartitionProvider = {
    new PartitionProvider {
      override def getPartitions(): Iterable[PartitionPath] = {
        partitions.asScala.toSeq
      }
    }
  }

  def fromMap(schema:StructType, partitions:java.util.Map[java.util.Map[java.lang.String, java.lang.String], java.lang.String]):PartitionProvider = {
    new PartitionProvider {
      override def getPartitions(): Iterable[PartitionPath] = {
        partitions.asScala.map(entry => {

          PartitionPath(PartitionProvider.mapToInternalRow(schema, entry._1), entry._2)
        })
      }
    }
  }
}
