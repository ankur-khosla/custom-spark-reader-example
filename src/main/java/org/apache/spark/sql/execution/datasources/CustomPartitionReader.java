package org.apache.spark.sql.execution.datasources;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.input.WholeTextFileRecordReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.JavaConverters;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CustomPartitionReader implements PartitionReader<InternalRow> {

    private final CustomPartition customInputPartition;
    private final String fileName;
    private Iterator<String> iterator;
    private List<Function> valueConverters;
    private List<Employee> employees = new ArrayList<>();

    public CustomPartitionReader(
            CustomPartition customInputPartition,
            StructType schema,
            String fileName) throws URISyntaxException, IOException {
        this.customInputPartition = customInputPartition;
        this.fileName = fileName;
        this.valueConverters = ValueConverters.getConverters(schema);
        this.createCustomReader();
    }

    private void createCustomReader() throws URISyntaxException, IOException {
        URL resource = this.getClass().getClassLoader().getResource(this.fileName);
        iterator = FileUtils.readLines(new File(resource.toURI()), "UTF-8").
                subList(customInputPartition.lineNumberStart, customInputPartition.lineNumberEnd + 1).iterator();
    }

    @Override
    public boolean next() {
        return !employees.isEmpty() || iterator.hasNext();
    }

    @Override
    public InternalRow get() {
        if (employees.isEmpty()) {
            String values = iterator.next();
            String[] employeeDetails = values.split("&");
            String gender = employeeDetails[0];
            String[] emps = employeeDetails[1].split("\\$");
            employees = Arrays.asList(emps).stream().map(emp -> emp.split(",")).
                    map(emp -> new Employee(emp[0], emp[1], gender)).collect(Collectors.toList());
        }
        Employee employee = employees.get(0);
        employees.remove(0);
        Object[] convertedValues = {employee.firstName, employee.lastName, employee.gender};
        for (int i = 0; i < convertedValues.length; i++) {
            convertedValues[i] = valueConverters.get(i).apply(convertedValues[i]);
        }
        return InternalRow.apply(JavaConverters.asScalaIteratorConverter(Arrays.asList(convertedValues).iterator()).asScala().toSeq());
    }


    @Override
    public void close() {
    }

    @Getter
    @Setter
    @AllArgsConstructor
    class Employee {
        private String firstName;
        private String lastName;
        private String gender;
    }
}
