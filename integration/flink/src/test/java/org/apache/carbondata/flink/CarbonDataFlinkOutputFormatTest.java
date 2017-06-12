/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.flink;

import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.flink.utils.FlinkTestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.spark.sql.CarbonContext;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CarbonDataFlinkOutputFormatTest {

    private final static Logger LOGGER = Logger.getLogger(CarbonFlinkInputFormatPerformanceTest.class.getName());
    private static FlinkTestUtils flinkTestUtils  = new FlinkTestUtils();
    private static CarbonContext carbonContext;

    static String getRootPath() throws IOException {
        return new File(CarbonDataFlinkOutputFormatTest.class.getResource("/").getPath() + "../../../..").getCanonicalPath();
    }

    @BeforeClass
    public static void defineStore() throws IOException {
        carbonContext = flinkTestUtils.createCarbonContext();

        String inputDataForTestTable = getRootPath() + "/integration/flink/src/test/resources/testtable.csv";
        String createTableTestTableCommand = "Create table testtable (Id Int, date Date,country String, name String" +
                ", phonetype String, serialname String, salary Int) stored by 'carbondata'";
        String loadTableTestTableCommand = "LOAD DATA LOCAL INPATH '"+ inputDataForTestTable +"' into table testtable";
        flinkTestUtils.createStore(carbonContext, createTableTestTableCommand, loadTableTestTableCommand);

        String inputDataForTestTableUniqdata = getRootPath() + "/integration/flink/src/test/resources/testtableUniqueData.csv";
        String createTableTestTableCommandUniqdata = "Create table testtableuniquedata (Id Int, date Date,country String," +
                "name String" + ", phonetype String, serialname String, salary Int) stored by 'carbondata'";
        String loadTableTestTableCommandUniqdata = "LOAD DATA LOCAL INPATH '"+ inputDataForTestTableUniqdata +"' into " +
                "table testtableuniquedata";
        flinkTestUtils.createStore(carbonContext, createTableTestTableCommandUniqdata, loadTableTestTableCommandUniqdata);

        String inputDataForUniqData = getRootPath() + "/integration/flink/src/test/resources/UniqData.csv";
        String createTableUniqDataCommand = "CREATE TABLE uniqdata_100 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION" +
                " string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 " +
                "decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double," +
                "INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
                "(\"TABLE_BLOCKSIZE\"= \"256 MB\")";

        String loadTableUniqDataCommand = "LOAD DATA INPATH '" + inputDataForUniqData+ "' into table uniqdata_100 " +
                "OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID," +
                "CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2," +
                "Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')";
        flinkTestUtils.createStore(carbonContext, createTableUniqDataCommand, loadTableUniqDataCommand);
        carbonContext.sql("select * from uniqdata_100");

        String inputDataForFloatTable = getRootPath() + "/integration/flink/src/test/resources/floatData.csv";
        String createTableForFloatCommand = "Create table flink_float_table (ID Int,name String,floatField float)" +
                " stored by 'carbondata'";
        String loadTableForFloatTableCommand = "LOAD DATA LOCAL INPATH '"+ inputDataForFloatTable +"' into table" +
                " flink_float_table";
        flinkTestUtils.createStore(carbonContext, createTableForFloatCommand, loadTableForFloatTableCommand);

        String inputDataForCharTable = getRootPath() + "/integration/flink/src/test/resources/charData.csv";
        String createTableForCharCommand = "Create table flink_char_table (ID Int, Grade char(1)) stored by " +
                "'carbondata'";
        String loadTableForCharTableCommand = "LOAD DATA LOCAL INPATH '"+ inputDataForCharTable +"' into table" +
                " flink_char_table";
        flinkTestUtils.createStore(carbonContext, createTableForCharCommand, loadTableForCharTableCommand);
        CacheProvider.getInstance().dropAllCache();

        LOGGER.info("Created Testing Tables in Store");
    }

    @AfterClass
    public static void removeStore() throws IOException {
        carbonContext.sql("Drop table if exists testtable");
        carbonContext.sql("Drop table if exists uniqdata_100");
        carbonContext.sql("Drop table if exists flink_float_table");
        carbonContext.sql("Drop table if exists flink_char_table");
        flinkTestUtils.closeContext(carbonContext);
//        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/store"));
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/store-input"));
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/carbonmetastore"));
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/flink-records"));
    }

    @Before
    public void beforeTest() throws Exception {
        CarbonDataFlinkOutputFormat.writeCount = 0;
    }

    @Ignore
    @Test
    public void testOutputFormatWithProjection() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/default/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);


        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();
        Tuple2<Void,Object[]> inputRecord = dataSource.collect().iterator().next();

        String[] columnTypes = {"Int", "Date", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "salary"};
        String[] dimensionColumns = {"date", "country"};

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("default")
                        .setTableName("test_table_projection")
                        .setRecordCount(recordCount)
                        .setDimensionColumns(dimensionColumns);

        dataSource.output(outputFormat.finish());
        environment.execute();

        String carbonTablePath = "/integration/flink/target/store/default/test_table_projection";
        CarbonDataFlinkInputFormat carbonFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() +
                carbonTablePath, columns, false);
        List<Tuple2<Void, Object[]>> retrievedDataSource = environment.createInput(carbonFlinkInputFormat
                .getInputFormat()).collect();
        Assert.assertTrue(retrievedDataSource.toString().contains(inputRecord.toString()));
    }

    // Todo: Resolve date format issue caused due to current change in master code
    @Ignore
    @Test
    public void testOutputFormatForSelectAll() throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String path = "/integration/flink/target/store-input/default/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() +
                path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
        dataSource.print();
        long recordCount = dataSource.count();
        Tuple2<Void,Object[]> inputRecord = dataSource.collect().iterator().next();

        String[] columnTypes = {"Int", "Date", "String", "String", "String", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String[] dimensionColumns = {"date", "country", "name", "phonetype", "serialname"};

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("default")
                        .setTableName("testtable")
                        .setRecordCount(recordCount)
                        .setDimensionColumns(dimensionColumns);

        dataSource.output(outputFormat.finish());
        env.execute();

        CacheProvider.getInstance().dropAllCache();
        String carbonTablePath = "/integration/flink/target/store/default/testtable";
        CarbonDataFlinkInputFormat carbonFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() +
                carbonTablePath, columns, false);
        List<Tuple2<Void, Object[]>> retrievedDataSource = env.createInput(carbonFlinkInputFormat.getInputFormat())
                .collect();
        Assert.assertTrue(retrievedDataSource.toString().contains(inputRecord.toString()));
    }

    @Test
    public void testOutputFormatForDatatypes() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"CUST_ID", "CUST_NAME", "ACTIVE_EMUI_VERSION", "DOB", "DOJ", "BIGINT_COLUMN1", "BIGINT_COLUMN2", "DECIMAL_COLUMN1", "DECIMAL_COLUMN2", "Double_COLUMN1", "Double_COLUMN2", "INTEGER_COLUMN1"};
        String path = "/integration/flink/target/store-input/default/uniqdata_100";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();
        Tuple2<Void,Object[]> inputRecord = dataSource.collect().iterator().next();

        String[] columnTypes = {"Int", "String", "String", "timestamp", "timestamp", "bigint", "bigint" ,"decimal(30,10)", "decimal(36,10)", "double","double","Int"};
        String[] columnHeaders = {"CUST_ID", "CUST_NAME", "ACTIVE_EMUI_VERSION", "DOB", "DOJ", "BIGINT_COLUMN1", "BIGINT_COLUMN2", "DECIMAL_COLUMN1", "DECIMAL_COLUMN2", "Double_COLUMN1", "Double_COLUMN2", "INTEGER_COLUMN1"};
        String[] dimensionColumns = {"CUST_NAME", "ACTIVE_EMUI_VERSION", "DOB", "DOJ"};

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("default")
                        .setTableName("uniqData_alltypes")
                        .setRecordCount(recordCount)
                        .setDimensionColumns(dimensionColumns);

        dataSource.output(outputFormat.finish());
        env.execute();

        CacheProvider.getInstance().dropAllCache();
        String carbonTablePath = "/integration/flink/target/store/default/uniqData_alltypes";
        CarbonDataFlinkInputFormat carbonFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + carbonTablePath, columns, false);
        List<Tuple2<Void, Object[]>> retrievedDataSource = env.createInput(carbonFlinkInputFormat.getInputFormat()).collect();
        Assert.assertTrue(retrievedDataSource.toString().contains(inputRecord.toString()));
    }

    @Test
    public void testOutputFormatForFloatDatatype() throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID","name","floatField"};
        String path = "/integration/flink/target/store-input/default/flink_float_table";
        CacheProvider.getInstance().dropAllCache();

        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);
        DataSet<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();
        Tuple2<Void,Object[]> inputRecord = dataSource.collect().iterator().next();

        String[] columnTypes = {"Int", "String", "float"};
        String[] columnHeaders = {"ID","name","floatField"};
        String[] dimensionColumns = {"name"};

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("default")
                        .setTableName("flink_float_table")
                        .setRecordCount(recordCount)
                        .setDimensionColumns(dimensionColumns);

        dataSource.output(outputFormat.finish());
        env.execute();
        long writeCount = CarbonDataFlinkOutputFormat.getWriteCount();

        CacheProvider.getInstance().dropAllCache();
        String carbonTablePath = "/integration/flink/target/store/default/flink_float_table";
        CarbonDataFlinkInputFormat carbonFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + carbonTablePath, columns, false);
        List<Tuple2<Void, Object[]>> retrievedDataSource = env.createInput(carbonFlinkInputFormat.getInputFormat()).collect();
        Assert.assertTrue(retrievedDataSource.toString().contains(inputRecord.toString()));
        Assert.assertEquals(writeCount, recordCount);
    }


    @Test
    public void testOutputFormatForCharDatatype() throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID","Grade"};
        String path = "/integration/flink/target/store-input/default/flink_char_table";

        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);
        DataSet<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();
        Tuple2<Void,Object[]> inputRecord = dataSource.collect().iterator().next();

        String[] columnTypes = {"Int","char"};
        String[] columnHeaders = {"ID","Grade"};
        String[] dimensionColumns = {"Grade"};

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("default")
                        .setTableName("flink_char_table")
                        .setRecordCount(recordCount)
                        .setDimensionColumns(dimensionColumns);

        dataSource.output(outputFormat.finish());
        env.execute();

        CacheProvider.getInstance().dropAllCache();
        String carbonTablePath = "/integration/flink/target/store/default/flink_char_table";
        CarbonDataFlinkInputFormat carbonFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath()
                + carbonTablePath, columns, false);
        List<Tuple2<Void, Object[]>> retrievedDataSource = env.createInput(carbonFlinkInputFormat.getInputFormat()).collect();
        Assert.assertTrue(retrievedDataSource.toString().contains(inputRecord.toString()));
        long writeCount = CarbonDataFlinkOutputFormat.getWriteCount();
        Assert.assertEquals(writeCount, recordCount);
    }

    @Test
    public void testOutputFormatForWrongColumns() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"SSN", "Address", "Contact_Number"};
        String path = "/integration/flink/target/store-input/default/testtable";
        CacheProvider.getInstance().dropAllCache();

        CarbonDataFlinkInputFormat carbonDataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath()
                + path, columns, false);
        try {
            DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbonDataFlinkInputFormat.getInputFormat());
            long recordCount = dataSource.count();

            String[] columnTypes = {"Int", "String", "Long"};
            String[] columnHeaders = {"SSN", "Address", "Contact_Number"};
            String[] dimensioncolumns = {"Address"};

            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormatBuilder =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnNames(columnHeaders)
                            .setColumnTypes(columnTypes)
                            .setStorePath(getRootPath() + "/integration/flink/target/store")
                            .setDatabaseName("default")
                            .setTableName("testtable2")
                            .setRecordCount(recordCount)
                            .setDimensionColumns(dimensioncolumns);

            dataSource.output(outputFormatBuilder.finish());
            environment.execute();
            assert false;
        } catch (Exception e) {
            assert true;
        }

    }

    @Test
    public void testOutputFormatForInvalidColumn() throws Exception {
        LOGGER.info("testOutputFormatForInvalidColumn : For mismatched columnTypes and columnHeaders");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String path = "/integration/flink/target/store-input/default/testtable";
        CacheProvider.getInstance().dropAllCache();

        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);
        DataSet<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "String", "String", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "name", "phonetype", "serialname"};
        String[] dimensioncolumns = {"date", "country", "name", "phonetype", "serialname"};

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("testdb")
                        .setTableName("testtable")
                        .setRecordCount(recordCount)
                        .setDimensionColumns(dimensioncolumns);

        dataSource.output(outputFormat.finish());

        try {
            env.execute();
            assert false;
        } catch (JobExecutionException ex) {
            assert true;
        }
    }

    @Test
    public void testOutputFormatForInvalidDimension() throws Exception {
        LOGGER.info("testOutputFormatForInvalidDimension : String columns not included in dimension");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String path = "/integration/flink/target/store-input/default/testtable";
        CacheProvider.getInstance().dropAllCache();

        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);
        DataSet<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "String", "String", "String", "String", "String", "Long"};
        String[] columnNames = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String[] dimensionColumns = {"date", "name", "phonetype", "serialname"};

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnNames)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("testdb")
                        .setTableName("testtable")
                        .setRecordCount(recordCount)
                        .setDimensionColumns(dimensionColumns);

        dataSource.output(outputFormat.finish());

        try {
            env.execute();
            assert false;
        } catch (JobExecutionException ex) {
            assert true;
        }
    }

    @Test
    public void testOutputFormatForInvalidDimensions() throws Exception {
        LOGGER.info("testOutputFormatForInvalidDimension : Dimension columns are more than table columns");

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/default/testtable";
        CacheProvider.getInstance().dropAllCache();

        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "salary"};
        String[] dimensionColumns = {"date", "country", "name", "phonetype", "serialname"};

        try {
            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnNames(columnHeaders)
                            .setColumnTypes(columnTypes)
                            .setStorePath(getRootPath() + "/integration/flink/target/store")
                            .setDatabaseName("testdb2")
                            .setTableName("testtable2")
                            .setRecordCount(recordCount)
                            .setDimensionColumns(dimensionColumns);

            dataSource.output(outputFormat.finish());
            environment.execute();
            assert false;
        } catch (JobExecutionException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage());
            assert true;
        }
    }

    @Test
    public void testOutputFormatWithoutDimensions() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/default/testtable";
        CacheProvider.getInstance().dropAllCache();

        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "salary"};

        try {
            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnNames(columnHeaders)
                            .setColumnTypes(columnTypes)
                            .setStorePath(getRootPath() + "/integration/flink/target/store")
                            .setDatabaseName("testdb2")
                            .setTableName("testtable2")
                            .setRecordCount(recordCount);

            dataSource.output(outputFormat.finish());
            environment.execute();
            assert false;
        } catch (IllegalArgumentException ex) {
            assert true;
        }
    }

    @Test
    public void testOutputFormatWithoutStorepath() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/default/testtable";
        CacheProvider.getInstance().dropAllCache();

        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "salary"};

        try {
            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnNames(columnHeaders)
                            .setColumnTypes(columnTypes)
                            .setDatabaseName("testdb2")
                            .setTableName("testtable2")
                            .setRecordCount(recordCount);

            dataSource.output(outputFormat.finish());
            environment.execute();
            assert false;
        } catch (IllegalArgumentException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage());
            assert true;
        }
    }

    @Test
    public void testOutputFormatWithoutDatabaseName() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/default/testtable";
        CacheProvider.getInstance().dropAllCache();

        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);


        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "salary"};

        try {
            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnNames(columnHeaders)
                            .setColumnTypes(columnTypes)
                            .setStorePath(getRootPath() + "/integration/flink/target/store")
                            .setTableName("testtable2")
                            .setRecordCount(recordCount);

            dataSource.output(outputFormat.finish());
            environment.execute();
            assert false;
        } catch (IllegalArgumentException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage());
            assert true;
        }
    }

    @Test
    public void testOutputFormatWithoutTableName() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/default/testtable";
        CacheProvider.getInstance().dropAllCache();

        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "salary"};

        try {
            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnNames(columnHeaders)
                            .setColumnTypes(columnTypes)
                            .setStorePath(getRootPath() + "/integration/flink/target/store")
                            .setDatabaseName("testdb2")
                            .setRecordCount(recordCount);

            dataSource.output(outputFormat.finish());
            environment.execute();
            assert false;
        } catch (IllegalArgumentException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage());
            assert true;
        }

    }

    @Test
    public void testOutputFormatWithoutRecordCount() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/default/testtable";
        CacheProvider.getInstance().dropAllCache();

        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());

        String[] columnTypes = {"Int", "Date", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "salary"};

        try {
            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnNames(columnHeaders)
                            .setColumnTypes(columnTypes)
                            .setStorePath(getRootPath() + "/integration/flink/target/store")
                            .setDatabaseName("testdb2")
                            .setTableName("testtable2");

            dataSource.output(outputFormat.finish());
            environment.execute();
            assert false;
        } catch (IllegalArgumentException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage());
            assert true;
        }
    }

    @Test
    public void testOutputFormatWithoutColumnNames() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/default/testtable";
        CacheProvider.getInstance().dropAllCache();

        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());

        String[] columnTypes = {"Int", "Date", "String", "Long"};

        try {
            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnTypes(columnTypes)
                            .setStorePath(getRootPath() + "/integration/flink/target/store")
                            .setDatabaseName("testdb2")
                            .setTableName("testtable2")
                            .setRecordCount(1000);

            dataSource.output(outputFormat.finish());
            environment.execute();
            assert false;
        } catch (IllegalArgumentException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage());
            assert true;
        }
    }

    @Test
    public void testOutputFormatWithoutColumnTypes() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/default/testtable";
        CacheProvider.getInstance().dropAllCache();

        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnHeaders = {"ID", "date", "country", "salary"};

        try {
            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnNames(columnHeaders)
                            .setStorePath(getRootPath() + "/integration/flink/target/store")
                            .setDatabaseName("testdb2")
                            .setTableName("testtable2")
                            .setRecordCount(recordCount);

            dataSource.output(outputFormat.finish());
            environment.execute();
            assert false;
        } catch (IllegalArgumentException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage());
            assert true;
        }
    }

    @Ignore
    @Test
    public void testMultipleLoadForDuplicateData() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "country", "salary"};
        String path = "/integration/flink/target/store-input/default/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath()
                + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();
        Tuple2<Void,Object[]> inputRecord = dataSource.collect().iterator().next();

        String[] columnTypes = {"Int", "String", "Long"};
        String[] columnHeaders = {"ID", "country", "salary"};
        String[] dimensionColumns = {"country"};

        CacheProvider.getInstance().dropAllCache();
        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("default")
                        .setTableName("test_table_multi_load")
                        .setRecordCount(recordCount)
                        .setDimensionColumns(dimensionColumns);

        dataSource.output(outputFormat.finish());
        environment.execute();

//        CacheProvider.getInstance().dropAllCache();
        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder carbonOutputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("default")
                        .setTableName("test_table_multi_load")
                        .setRecordCount(recordCount)
                        .setDimensionColumns(dimensionColumns);

        dataSource.output(carbonOutputFormat.finish());
        environment.execute();
        CacheProvider.getInstance().dropAllCache();
        String carbonTablePath = "/integration/flink/target/store/default/test_table_multi_load";
        CarbonDataFlinkInputFormat carbonFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath()
                + carbonTablePath, columns, false);
        List<Tuple2<Void, Object[]>> retrievedDataSource = environment.createInput(carbonFlinkInputFormat
                .getInputFormat()).collect();
        System.out.println(">>>>>>"+retrievedDataSource);
        System.out.println(retrievedDataSource.toString());
        Assert.assertTrue(retrievedDataSource.toString().contains(inputRecord.toString()));
        Assert.assertEquals(retrievedDataSource.size(), 2 * recordCount);
    }

    @Ignore
    @Test
    public void testMultipleLoadForUniqueData() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "country", "salary"};
        String path = "/integration/flink/target/store-input/default/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath()
                + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        dataSource.print();
        long firstSourceRecordCount = dataSource.count();

        String[] columnTypes = {"Int", "String", "Long"};
        String[] columnHeaders = {"ID", "country", "salary"};
        String[] dimensionColumns = {"country"};

        CacheProvider.getInstance().dropAllCache();
        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("default")
                        .setTableName("test_table_multi_load_uniqdata")
                        .setRecordCount(firstSourceRecordCount)
                        .setDimensionColumns(dimensionColumns);

        dataSource.output(outputFormat.finish());
        environment.execute();

        CacheProvider.getInstance().dropAllCache();


    }

    @Ignore
    @Test
    public void dataLoadForUniqueDataContinued() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "country", "salary"};
        String[] columnTypes = {"Int", "String", "Long"};
        String[] columnHeaders = {"ID", "country", "salary"};
        String[] dimensionColumns = {"country"};

        String secondloadFilePath = "/integration/flink/target/store-input/default/testtableuniquedata";
        CarbonDataFlinkInputFormat carbonFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath()
                + secondloadFilePath, columns, false);
        DataSet<Tuple2<Void, Object[]>> secondDataSource = environment.createInput(carbonFlinkInputFormat.getInputFormat());
        long secondSourceRecordCount = secondDataSource.count();
        secondDataSource.print();
        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder carbonOutputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("default")
                        .setTableName("test_table_multi_load_uniqdata")
                        .setRecordCount(secondSourceRecordCount)
                        .setDimensionColumns(dimensionColumns);
        secondDataSource.output(carbonOutputFormat.finish());
        environment.execute();

        CacheProvider.getInstance().dropAllCache();
        String carbonTablePath = "/integration/flink/target/store/default/test_table_multi_load_uniqdata";
        CarbonDataFlinkInputFormat carbondataInputFormat = new CarbonDataFlinkInputFormat(getRootPath()
                + carbonTablePath, columns, false);
        List<Tuple2<Void, Object[]>> retrievedDataSource = environment.createInput(carbondataInputFormat
                .getInputFormat()).collect();
        System.out.println(retrievedDataSource.toString());
        /*Assert.assertTrue(retrievedDataSource.toString().contains(inputRecord.toString()));
        Assert.assertEquals(retrievedDataSource.size(), firstSourceRecordCount + secondSourceRecordCount);*/
    }


}
