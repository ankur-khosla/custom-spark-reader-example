package org.apache.spark.sql.execution.datasources;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.connector.read.InputPartition;

@AllArgsConstructor
public class CustomPartition implements InputPartition {

    String filename;
    int lineNumberStart;
    int lineNumberEnd;

    @Override
    public String[] preferredLocations() {
        return new String[0];
    }
}
