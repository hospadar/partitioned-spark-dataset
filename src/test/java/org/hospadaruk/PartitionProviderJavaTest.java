package org.hospadaruk;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.execution.datasources.PartitionPath;
import org.apache.spark.sql.types.*;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Callable;

import org.hospadaruk.PartitionProvider;
import org.hospadaruk.PartitionProvider$;


/*
No assertions, just testing that we can call in from java with sanity
 */
public class PartitionProviderJavaTest {

    Map<Map<String, String>, String> partitions = new HashMap<Map<String, String>, String>();
    Map<String, String> key = new HashMap<>();
    List<PartitionPath> partitionIterable  = new ArrayList<>();
    StructType schema = DataTypes.createStructType(new StructField[]{DataTypes.createStructField("date", DataTypes.StringType, true)});

    @Before
    public void setup() {
        key.put("date", "1970-01-01");
        partitions.put(key, "/some/file");
        partitionIterable.add(new PartitionPath(PartitionProvider$.MODULE$.mapToInternalRow(schema, key), new Path("/some/file")));

    }

    @Test
    public void testFromMap(){

        PartitionProvider p = PartitionProvider.fromMap(schema, partitions);
        p.getPartitions();
    }

    @Test
    public void testFromIterable(){
        PartitionProvider p = PartitionProvider.fromIterable(partitionIterable);
        p.getPartitions();
    }

    @Test
    public void testFromCallableIterable(){
        PartitionProvider p = PartitionProvider.fromCallable(new Callable<Iterable<PartitionPath>>() {
            @Override
            public Iterable<PartitionPath> call() throws Exception {
                return partitionIterable;
            }
        });

        p.getPartitions();
    }

}
