package org.apache.carbondata.flink;


import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.flink.utils.FlinkTestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.sql.CarbonContext;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

public class CDFlinkOutputFormatTest {
    private final static Logger LOGGER = Logger.getLogger(CarbonFlinkInputFormatPerformanceTest.class.getName());
    private static FlinkTestUtils flinkTestUtils  = new FlinkTestUtils();
    private static CarbonContext carbonContext;

    static String getRootPath() throws IOException {
        return new File(CarbonDataFlinkOutputFormatTest.class.getResource("/").getPath() + "../../../..").getCanonicalPath();
    }

    @BeforeClass
    public static void defineStore() throws IOException {
        carbonContext = flinkTestUtils.createCarbonContext();

        String inputDataForBadRecords = getRootPath() + "/integration/flink/src/test/resources/badrecords_decimal.csv";
        String tableName = "badrecords";
        String createTableBadRecordsCommand = "Create table IF NOT EXISTS "+tableName+" (ID Int, date Date, country String," +
                " name String, phonetype String, serialname char(10), salary Int, floatField float," +
                " Timestamp timestamp,BigInt1 bigint, BigInt2 bigint,DECIMAL_COLUMN1 decimal(30,10) ) STORED BY 'carbondata' ";
        String loadTableBadRecordsCommand = "LOAD DATA LOCAL INPATH '"+ inputDataForBadRecords +"' into table "+tableName;
        flinkTestUtils.createStore(carbonContext, createTableBadRecordsCommand, loadTableBadRecordsCommand);

//        String inputDataForUniqData = getRootPath() + "/integration/flink/src/test/resources/badrecords_int.csv";
//        String createTableUniqDataCommand = "CREATE TABLE uniqdata_100 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES (\"TABLE_BLOCKSIZE\"= \"256 MB\")";
//
//        String loadTableUniqDataCommand = "LOAD DATA INPATH '" + inputDataForUniqData+ "' into table uniqdata_100 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')";
//        flinkTestUtils.createStore(carbonContext, createTableUniqDataCommand, loadTableUniqDataCommand);
//        carbonContext.sql("select * from uniqdata_100");
//
//        String inputDataForFloatTable = getRootPath() + "/integration/flink/src/test/resources/floatData.csv";
//        String createTableForFloatCommand = "Create table flink_float_table (ID Int,name String,floatField float) stored by 'carbondata'";
//        String loadTableForFloatTableCommand = "LOAD DATA LOCAL INPATH '"+ inputDataForFloatTable +"' into table flink_float_table";
//        flinkTestUtils.createStore(carbonContext, createTableForFloatCommand, loadTableForFloatTableCommand);
//
//        String inputDataForCharTable = getRootPath() + "/integration/flink/src/test/resources/charData.csv";
//        String createTableForCharCommand = "Create table flink_char_table (ID Int, Grade char(1)) stored by 'carbondata'";
//        String loadTableForCharTableCommand = "LOAD DATA LOCAL INPATH '"+ inputDataForCharTable +"' into table flink_char_table";
//        flinkTestUtils.createStore(carbonContext, createTableForCharCommand, loadTableForCharTableCommand);

//        flinkTestUtils.closeContext(carbonContext);
        CacheProvider.getInstance().dropAllCache();

        LOGGER.info("Created Testing Tables in Store");
    }

    @AfterClass
    public static void removeStore() throws IOException {
        carbonContext.sql("Drop table if exists badrecords");
        carbonContext.sql("Drop table if exists uniqdata_100");
        carbonContext.sql("Drop table if exists flink_float_table");
        carbonContext.sql("Drop table if exists flink_char_table");
        flinkTestUtils.closeContext(carbonContext);
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/store"));
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/store-input"));
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/carbonmetastore"));
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/flink-records"));
    }

    @Before
    public void beforeTest() throws Exception {
        CarbonDataFlinkOutputFormat.writeCount = 0;
    }

    @Test
    public void testOutputFormatWithProjection() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "country", "name", "phonetype", "serialname", "salary", "floatField", "Timestamp", "BigInt1", "BigInt2", "DECIMAL_COLUMN1"};
        String path = "/integration/flink/target/store-input/default/badrecords";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();
        Tuple2<Void,Object[]> inputRecord = dataSource.collect().iterator().next();

        String[] columnTypes = {"Int","String","String", "String", "char(10)", "Int", "float", "timestamp", "bigint", "bigint", "decimal(30,10)"};
        String[] columnHeaders = {"ID","country","name","phonetype","serialname","salary","floatField","Timestamp","BigInt1","BigInt2","DECIMAL_COLUMN1"};
        String[] dimensionColumns = {"country","name","phonetype","serialname","Timestamp"};

        String tableName = "badrecords_projection";
        String databaseName = "default";
        String tablePath = getRootPath() + "/integration/flink/target/store";

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(tablePath)
                        .setDatabaseName(databaseName)
                        .setTableName(tableName)
                        .setRecordCount(recordCount)
                        .setDimensionColumns(dimensionColumns);

        dataSource.output(outputFormat.finish());
        environment.execute();

        String pathCheck = tablePath+"/"+databaseName+"/"+tableName;
        System.out.println(">>>>>>>Path<<<<<<<<\n "+ pathCheck);
        CarbonDataFlinkInputFormat carbonFlinkInputFormat = new CarbonDataFlinkInputFormat(tablePath+"/"+databaseName+"/"+tableName, columns, false);
        List<Tuple2<Void, Object[]>> retrievedDataSource = environment.createInput(carbonFlinkInputFormat.getInputFormat()).collect();
        System.out.println(">>>>>>Retrieved data source>>>>>>\n"+retrievedDataSource.toString());
        Assert.assertTrue(retrievedDataSource.toString().contains(inputRecord.toString()));
    }


}
