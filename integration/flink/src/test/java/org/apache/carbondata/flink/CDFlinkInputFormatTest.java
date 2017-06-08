package org.apache.carbondata.flink;

import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.flink.utils.FlinkTestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.sql.CarbonContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class CDFlinkInputFormatTest {

    protected static String[] columns;
    private static String path;
    private static CarbonContext carbonContext;
    private static FlinkTestUtils flinkTestUtils = new FlinkTestUtils();

    @BeforeClass
    public static void defineStore() throws IOException {
        CacheProvider.getInstance().dropAllCache();
        String testData = getRootPath() + "/integration/flink/src/test/resources/badrecords_timestamp.csv";
        String tableName = "badrecords_timestamp";
        carbonContext = flinkTestUtils.createCarbonContext();
        String createTableCommand = "CREATE TABLE IF NOT EXISTS " + tableName
                + "(ID Int, date Date, country String, name String, phonetype String, " + "serialname char(10), salary Int, floatField float, Timestamp timestamp) STORED BY 'carbondata' ";
        String loadTableCommand = "LOAD DATA LOCAL INPATH '" + testData + "' into table " + tableName;
        flinkTestUtils.createStore(carbonContext, createTableCommand, loadTableCommand);
//        flinkTestUtils.closeContext(carbonContext);
        CacheProvider.getInstance().dropAllCache();
    }

    @AfterClass
    public static void removeStore() throws IOException {
        flinkTestUtils.closeContext(carbonContext);
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/store"));
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/store-input"));
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/carbonmetastore"));
    }

    static String getRootPath() throws IOException {
        return new File(CarbonFlinkInputFormatPerformanceTest.class.getResource("/").getPath() + "../../../..").getCanonicalPath();
    }

//    @Test
//    public void getDataFromCarbon() throws Exception {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        columns = new String[]{"id", "name"};
//        String path = "/integration/flink/target/store-input/default/badRecords_Int";
//        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);
//        List<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat()).collect();
//        String inputTuple = "(null,[1, aaa1])";
//        int rowCount = dataSource.size();
//        Assert.assertTrue(dataSource.toString().contains(inputTuple));
//        assert (rowCount == 10);
//    }

    @Test
    public void getDataFromCarbonSelectAll() throws Exception {
        CacheProvider.getInstance().dropAllCache();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        columns = new String[]{"ID", "country", "name", "phonetype", "serialname", "salary", "floatField", "Timestamp"};
        path = "/integration/flink/target/store-input/default/badrecords_timestamp";

        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path.toLowerCase(), columns, false);
        List<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat()).collect();
        String inputTuple = "(null,[6, china, aaa6, phone294, ASD59961, 15005, 3.5, 1970-01-01 10:00:03])";
        String result = dataSource.toString();
        System.out.println("Result : " + result);
        int rowCount = dataSource.size();
        Assert.assertTrue(dataSource.toString().contains(inputTuple));
        assert (rowCount == 10);
    }

}
