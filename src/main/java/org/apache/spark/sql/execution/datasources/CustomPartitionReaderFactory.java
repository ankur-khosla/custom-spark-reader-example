package org.apache.spark.sql.execution.datasources;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;

public class CustomPartitionReaderFactory implements PartitionReaderFactory {
    private final StructType schema;
    private final String filePath;

    public CustomPartitionReaderFactory(StructType schema, String fileName) {
        this.schema = schema;
        this.filePath = fileName;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        try {
            return new CustomPartitionReader((CustomPartition) partition, schema, filePath);
        } catch (URISyntaxException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
