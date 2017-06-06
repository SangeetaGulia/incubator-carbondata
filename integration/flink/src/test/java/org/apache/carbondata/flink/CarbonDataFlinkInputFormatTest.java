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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.sql.CarbonContext;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class CarbonDataFlinkInputFormatTest {

    protected static String[] columns;
    private static FlinkTestUtils flinkTestUtils = new FlinkTestUtils();

    @BeforeClass
    public static void defineStore() throws IOException {
        String testData = getRootPath() + "/integration/flink/src/test/resources/data.csv";
        CarbonContext carbonContext = flinkTestUtils.createCarbonContext();
        String createTableCommand = "CREATE TABLE IF NOT EXISTS t3 "
                + "(ID Int, date Date, country String, name String, phonetype String, " + "serialname char(10), salary Int, floatField float) STORED BY 'carbondata'";
        String loadTableCommand = "LOAD DATA LOCAL INPATH '" + testData + "' into table t3";
        flinkTestUtils.createStore(carbonContext, createTableCommand, loadTableCommand);
        flinkTestUtils.closeContext(carbonContext);
        CacheProvider.getInstance().dropAllCache();
    }

    static String getRootPath() throws IOException {
        return new File(CarbonFlinkInputFormatPerformanceTest.class.getResource("/").getPath() + "../../../..").getCanonicalPath();
    }

    @AfterClass
    public static void removeStore() throws IOException {
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/store-input"));
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/carbonmetastore"));
    }

    @Test
    public void getDataFromCarbon() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        columns = new String[]{"id", "name"};
        String path = "/integration/flink/target/store-input/default/t3";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);
        List<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat()).collect();
        String inputTuple = "(null,[1, aaa1])";
        int rowCount = dataSource.size();
        Assert.assertTrue(dataSource.toString().contains(inputTuple));
        assert (rowCount == 10);
    }

    // Todo : refactor code to correct date format
    @Ignore
    @Test
    public void getDataFromCarbonSelectAll() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        columns = new String[]{"ID", "date", "country", "name", "phonetype", "serialname", "salary", "floatField"};
        String path = "/integration/flink/target/store-input/default/t3";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);
        List<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat()).collect();
        String inputTuple = "(null,[1,2015/7/23,china,aaa1,phone197,ASD69643,15000,2.34])";
        int rowCount = dataSource.size();
        Assert.assertTrue(dataSource.toString().contains(inputTuple));
        assert (rowCount == 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getDataFromInvalidPath() throws Exception {
        columns = new String[]{"id", "name"};
        String path = "./flink/target/store-input/default/t3";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);
        carbondataFlinkInputFormat.getInputFormat();
    }

    @Test(expected = IllegalArgumentException.class)
    public void getDataFromTableHavingInvalidColumns() throws Exception {
        columns = new String[]{};
        String path = "integration/flink/target/store-input/default/t3";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);
        carbondataFlinkInputFormat.getInputFormat();
    }
}
