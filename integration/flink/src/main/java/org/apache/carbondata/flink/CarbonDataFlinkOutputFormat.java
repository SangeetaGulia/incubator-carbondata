package org.apache.carbondata.flink;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

public class CarbonDataFlinkOutputFormat extends RichOutputFormat<Tuple2<Void, Object[]>> {

    public static long writeCount = 0;
    private String[] columnNames;
    private String[] columnTypes;
    private String[] dimensionColumns;
    private String storePath;
    private String databaseName;
    private String tableName;
    private long recordCount = 0;
    private ArrayList<Tuple2<Void, Object[]>> records = new ArrayList();

    /**
     * This method returns the number of records written to carbondata table
     *
     * @return writeCount
     */
    public static long getWriteCount() {
        return writeCount;
    }

    public static CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder buildCarbonDataOutputFormat() {
        return new CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder();
    }

    private String getSourcePath() throws IOException {
        String path = new File(this.getClass().getResource("/").getPath() + "../../../..").getCanonicalPath();
        return path + "/integration/flink/target/flink-records/record.csv";
    }

    private boolean isValidColumns() {
        if (columnNames.length == columnTypes.length && dimensionColumns.length < columnNames.length)
            return true;
        else
            return false;
    }

    private boolean isValidDimensions() {
        boolean isValid = true;
        for (int iterator = 0; iterator < columnTypes.length; iterator++) {
            if (columnTypes[iterator].toLowerCase().equals("string") && !Arrays.asList(dimensionColumns).contains(columnNames[iterator])) {
                isValid = false;
                break;
            }
        }
        return isValid;
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
    }

    /**
     * This method is responsible for creating carbon table
     *
     * @param record
     * @throws IOException
     */

    @Override
    public void writeRecord(Tuple2<Void, Object[]> record) throws IOException {
        records.add(record);
        writeCount++;

        if (writeCount == recordCount) {
            if (isValidColumns() && isValidDimensions()) {

                AbsoluteTableIdentifier absoluteTableIdentifier = new AbsoluteTableIdentifier(storePath, new CarbonTableIdentifier(databaseName, tableName, UUID.randomUUID().toString()));

                String sourcePath = getSourcePath();
                File sourceFile = new File(sourcePath);
                sourceFile.getParentFile().mkdirs();
                sourceFile.createNewFile();

                BufferedWriter bufferedWriter = null;
                FileWriter fileWriter = null;
                String columnString = "";

                try {
                    fileWriter = new FileWriter(sourcePath);
                    bufferedWriter = new BufferedWriter(fileWriter);

                    for (int iterator = 0; iterator < columnNames.length; iterator++) {
                        columnString += columnNames[iterator] + ",";
                    }
                    columnString = columnString.substring(0, columnString.length() - 1);
                    bufferedWriter.write(columnString + "\n");

                    for (Tuple2<Void, Object[]> element : records) {
                        String row = (element.toString().substring(7, element.toString().length() - 2)).replace(", ", ",");
                        bufferedWriter.write(row + "\n");
                    }
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                } finally {
                    try {
                        if (bufferedWriter != null) {
                            bufferedWriter.close();
                        }
                        if (fileWriter != null) {
                            fileWriter.close();
                        }
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                }
                CarbondataStoreCreator carbondataStoreCreator = new CarbondataStoreCreator();
                carbondataStoreCreator.createCarbonStore(absoluteTableIdentifier, columnString, columnNames, columnTypes, sourcePath, dimensionColumns);
            } else {
                throw new IllegalArgumentException("Please provide correct column data");
            }
        }
    }

    @Override
    public void close() throws IOException {
    }

    public static class CarbonDataOutputFormatBuilder {

        private final CarbonDataFlinkOutputFormat format = new CarbonDataFlinkOutputFormat();

        /**
         * This method set ColumnNames of the carbon table
         *
         * @param columns
         * @return CarbonDataFlinkOutputFormat
         */
        public CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder setColumnNames(String[] columns) {
            this.format.columnNames = columns;
            return this;
        }

        /**
         * This method set ColumnTypes of the carbon table
         *
         * @param columnTypes
         * @return CarbonDataFlinkOutputFormat
         */
        public CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder setColumnTypes(String[] columnTypes) {
            this.format.columnTypes = columnTypes;
            return this;
        }

        /**
         * This method sets the store location for carbon table
         *
         * @param storePath : location where carbon table needs to be created
         * @return CarbonDataFlinkOutputFormat
         */
        public CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder setStorePath(String storePath) {
            this.format.storePath = storePath;
            return this;
        }

        public CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder setDatabaseName(String databaseName) {
            this.format.databaseName = databaseName;
            return this;
        }

        public CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder setTableName(String tableName) {
            this.format.tableName = tableName;
            return this;
        }

        /**
         * This method is used to set the record count
         *
         * @param recordCount : Number of rows received from input format
         * @return CarbonDataFlinkOutputFormat
         */
        public CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder setRecordCount(long recordCount) {
            this.format.recordCount = recordCount;
            return this;
        }

        /**
         * This method set the dimension Columns
         * NOTE: All Strings must be part of dimension column
         *
         * @param dimensionColumns
         * @return CarbonDataFlinkOutputFormat
         */
        public CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder setDimensionColumns(String[] dimensionColumns) {
            this.format.dimensionColumns = dimensionColumns;
            return this;
        }

        /**
         * This method checks if all required inputs are provided or not
         *
         * @return CarbonDataFlinkOutputFormat
         */
        public CarbonDataFlinkOutputFormat finish() {
            if (format.databaseName == null) {
                throw new IllegalArgumentException("No database name supplied.");
            }

            if (format.tableName == null) {
                throw new IllegalArgumentException("No tablename supplied.");
            }

            if (format.storePath == null) {
                throw new IllegalArgumentException("No storePath supplied.");
            }

            if (format.columnNames == null) {
                throw new IllegalArgumentException("No column names supplied.");
            }

            if (format.columnTypes == null) {
                throw new IllegalArgumentException("No column Types supplied.");
            }

            if (format.dimensionColumns == null) {
                throw new IllegalArgumentException("No dictionary columns supplied.");
            }

            if (format.recordCount == 0) {
                throw new IllegalArgumentException("No recordCount supplied.");
            }

            return this.format;
        }
    }

}
