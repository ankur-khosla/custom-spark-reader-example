package org.apache.spark.sql.execution.datasources;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CustomTable implements SupportsRead {
    private final StructType schema;
    private final Map<String, String> properties;
    private Set<TableCapability> capabilities;

    public CustomTable(StructType schema, Map<String, String> properties) {
        this.schema = schema;
        this.properties = properties;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new CustomScanBuilder(schema, properties, options);
    }

    @Override
    public String name() {
        return "dummy_table";
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        if (capabilities == null) {
            this.capabilities = new HashSet<>();
            capabilities.add(TableCapability.BATCH_READ);
            capabilities.add(TableCapability.MICRO_BATCH_READ);
        }
        return capabilities;
    }
}
