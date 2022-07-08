package org.apache.spark.sql.execution.datasources;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class CustomScanBuilder implements ScanBuilder {
    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;

    public CustomScanBuilder(StructType schema,
                          Map<String, String> properties,
                          CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
    }

    @Override
    public Scan build() {
        return new CustomScan(schema,properties,options);
    }
}
