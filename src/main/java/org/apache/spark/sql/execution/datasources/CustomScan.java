package org.apache.spark.sql.execution.datasources;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class CustomScan implements Scan {
    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;

    public CustomScan(StructType schema,
                   Map<String, String> properties,
                   CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public String description() {
        return "custom_scan";
    }

    @Override
    public Batch toBatch() {
        return new CustomBatch(schema,properties,options);
    }
}
