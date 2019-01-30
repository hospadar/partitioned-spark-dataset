# partitioned-Spark-dataset

For creating **logically** partitioned Apache Spark datasets with ease, without being forced to store the underlying data in a 
particular folder structure, **without** being forced to use a Hive metastore (or even enable Hive support in the Spark context)

### Use Me
This lib is available via [maven central](https://search.maven.org/artifact/org.hospadaruk/partitioned-spark-dataset/1.0/jar)
```xml
<dependency>
  <groupId>org.hospadaruk</groupId>
  <artifactId>partitioned-spark-dataset</artifactId>
  <version>1.0</version>
</dependency>
``` 

### Spark? Partitions?
The word "partition" is overloaded in Spark, it can mean the smallest unit of work which can be processed 
by a single task, or in the context of Spark SQL, it can refer to a logical division of a large dataset which
enables Spark to efficiently exclude most data from a large scan.  For the purpose of this readme, assume that "partition" 
or "logical partition" refers to the latter variety of partitions.

### How does Hive/Spark/Spark SQL support logical partitioning out-of-the box?

#### Hive and Spark SQL + Hive Metastore:
Hive has a concept of "partitioned tables" (often used for date/month/year partitioned logs) which allow a large dataset
to be stored in subfolders corresponding to the logical partition.  If queries against a table often filter on a column, 
it may make sense to partition the table by that column to reduce the number of rows that Hive (or Spark) needs to scan.

This sort of table is often used to store log data since queries against that data are often date-limited.  Logically partitioning
the table dramatically reduces the size of table scans when executing date-limited queries.

Let's say you have a hadoop filesystem with data like (notice that there are two different date partitions):
```
/warehouse/mydb/logs/date=2018-01-01/part1.parquet
/warehouse/mydb/logs/date=2018-01-02/part1.parquet
```
  
If we created a Hive table pointed at that data with something like this:
```SQL
USE mydb;
CREATE TABLE logs(ip STRING, url STRING) PARTITIONED BY (date STRING) LOCATION '/warehouse/mydb/logs/';
```

If you query that Hive table with Hive (or Spark), filters on the "date" column will be used to restrict the input files scanned.

Examples:
```
SELECT * FROM mydb.foo WHERE dt='2018-01-01' 
-- this query would only scan the parquet data in /warehouse/mydb/logs/date=2018-01-01/part1.parquet
```

#### Spark without a Hive metastore:

Spark can also create a logically partitioned dataset if you create a filesystem as above and point
the usual dataframe reader at the root directory (i.e. `SparkSession.read.parquet("/warehouse/mydb/logs/")`).  

### Why this library?
I was motivated to write this library for a couple reasons:
- I work on a data warehouse|lake|system where we build/manage/store a wide variety of large time-oriented(logs) datasets.
- I don't store data using meaningful file names.  
    - Our data is primarily stored in S3, and we prefer not to delete/overwrite 
      existing data when creating/reprocessing datasets.  
    - Our typical practice is to write a new dataset (let's say a single date 
      of a date-partitioned log table) to a new folder, then run a Hive command like `ALTER table logs PARTITION (date='2018-01-01') SET LOCATION '/warehouse/<random-folder-name>`
    - This technique forces consumers to refer to the Hive metastore to understand which parquet files are currently 
      considered to be part of the table (and that's good because the metastore is the authoritative source for that data).
    - Because we don't need to overwrite data, the old data can hang around for a while, giving us the possibility of rolling back a table
      to what it looked like in the past if some admin (me) accidentally drops it, or we do a big reprocessing job and later discover
      that we've really mangled something important.
    - *The upshot of all of this is that' I can't use Spark's built-in support for reading partitioned-by-directory files*
- I dislike running/managing/integrating a Hive Metastore.
    - The Hive metastore has **GREAT** integration with every other query tool in the hadoop ecosystem (Spark, Presto, AWS Athena, etc), so while it's 
      really handy to have my table metadata in a Hive metastore, _I don't actually use Hive_.  
        - The Hive metastore is very tightly integrated with Hive, supports
          a huge number of features I don't need/want, and is generally much more complex/huge/slow than I want.
    - I've been forced to use Hive, because although I dislike running/managing it, there really is no other option for tracking/
      using logically partitioned datasets (other than using /partition=value/ style directory names which don't work for me, see above)
      
What I _really_ wanted was to be able to tell Spark "Partition 1 is in folder /asdbf, partition 2 is in folder /oiuxf" in 
such a way that Spark could avoid scanning partition 2 if I had filtered it out.  BUT Spark has no support for that out 
of the box (other than fully enabling Hive support).  So I wrote this library!

### Classes and what they do:

#### `org.hospadaruk.StaticPartitionedDataset`

This is the main entrypoint for creating logically partitioned datasets with this lib.  It defines a static method
which can create a Spark Dataset given schema describing the data & a mapping of partition values to filesystem paths.

```scala
def createDataset(session:SparkSession, //pass in your Spark session
                    dataSchema:StructType, //a struct type describing the data found in the underlying files
                    partitionSchema:StructType, //a struct type describing the partition fields.  You can have multiple partition columns, and they can be string, float, double, int, long, or boolean.
                    fileFormat: FileFormat, //a FileFormat to use to open the underlying data.  All partitions must be stored in the same format.
                    partitions, //the partition mappings. See below for different ways to pass things in
                    options: java.util.Map[String, String] //A java map of options to be passed along to the file format for reading the data
                   ):Dataset[Row]
``` 

"partitions" can be one of:
- `java.util.Map<java.util.Map<String, String>, String>`
    - Logically, this is a map from partition values->string.  
    - If you have non-string partition columns, we'll attempt to parse your desired type out of the provided string.  Errors will be thrown if the specified type can't be parsed out.
    
    A table partitioned on date might be represented something like:<br/>
    ```python
    {
      {"date": "2018-01-01"}: "/location/of/data",  
      {"date": "2018-01-02"}: "s3://location/of/other/data"
    }
    ```
    note that all partitions do not need to be on the same filesystem/protocol, as long as the strings can be interpreted as Hadoop file URIs
    
- `java.util.concurrent.Callable<java.lang.Iterable<org.apache.Spark.sql.execution.datasources.PartitionPath>>`
    - a `Callable` which returns a java `Iterable` of `PartitionPath` objects defining each partition.
    - the callable will be called whenever the dataset is `refresh()`'d - this can be useful for datasets whose definition is stored externally and might change over time
    - see `org.hospadaruk.PartitionProvider` for examples on how to create `PartitionPath` objects
      
- `java.lang.Iterable<org.apache.Spark.sql.execution.datasources.PartitionPath>`
    - An `Iterable` of `PartitionPath` objects defining each partition.
    - Maybe handy if you have a list of partition infos, but don't expect them to change.
    
- `org.hospadaruk.PartitionProvider`
    - create a dataset directly from a PartitionProvider.  Used internally for all the other methods, you might call directly
      if you had the need to implement your own partition provider. 
 

#### `org.hospadaruk.StaticFileIndex`

This class extends Spark's `org.apache.Spark.sql.execution.datasources.FileIndex`.  
`FileIndex` is used by `org.apache.Spark.sql.execution.datasources.HadoopFsRelation` to map between partitions and the real files (Hadoop-readable paths) which
contain the data for those partitions.  StaticFileIndex is designed to make a static mapping from partitions->files available to `org.apache.Spark.sql.execution.datasources.HadoopFsRelation`      
    
#### `org.hospadaruk.PartitionProvider`

This class provides the static mapping to StaticFileIndex.  It provides a couple default static methods
to create a provider from maps, callables, and iterables.  The intent of this class is that one might extend/override it 
if one was trying to do something really interesting (like implementing an alternative repository of partition metadata to replace the Hive metastore)


### Scala/Java thoughts:

I implemented these methods to accept java containers because I expect to call them from languages other than scala.
I implemented the library in scala because it's easier to interop with Spark's guts that way.
I don't have a _ton_ of scala experience, so it's possible I did stuff in a weird way.  My Bad.

### Integrating with Spark:

See the pom for the current version of Spark that this lib is built against.  It will probably work with other versions, but you might want to try that
out before going str8-2-prod.  Let me know if you encounter difficulties, PRs accepted.

Spark is listed as a `provided` dependancy, so this lib shouldn't blow away the actual version of Spark you want to build against. 

