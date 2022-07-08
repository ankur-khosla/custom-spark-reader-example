package org.apache.spark.sql.execution.datasources;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class CustomBatch implements Batch {
    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private String filename;
    public CustomBatch(StructType schema,
                    Map<String, String> properties,
                    CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
        this.filename = options.get("path");
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return new InputPartition[]{
                new CustomPartition(filename,0,0),
                new CustomPartition(filename,1,1)
                };
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new CustomPartitionReaderFactory(schema, filename);
    }
}
