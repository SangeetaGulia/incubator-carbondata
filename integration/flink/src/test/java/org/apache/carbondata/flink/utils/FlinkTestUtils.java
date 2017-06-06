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

package org.apache.carbondata.flink.utils;

import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.CarbonContext;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

public class FlinkTestUtils {

    static String getRootPath() throws IOException {
        return new File(FlinkTestUtils.class.getResource("/").getPath() + "../../../..").getCanonicalPath();
    }

    String getStoreLocation () throws IOException {
        return getRootPath() + "/integration/flink/target/store-input";
    }

    public CarbonContext createCarbonContext() throws IOException {
        SparkContext sc = new SparkContext(new SparkConf()
        .setAppName("FLINK_TEST_EXAMPLE")
        .setMaster("local[2]"));
    sc.setLogLevel("ERROR");

    CarbonContext cc = new CarbonContext(sc, getStoreLocation(), getRootPath() + "/integration/flink/target/carbonmetastore");

    CarbonProperties.getInstance().addProperty("carbon.storelocation", getStoreLocation());
    return cc;
    }

    public void createStore(CarbonContext carbonContext, String createTableCommand, String loadTableCommand) throws IOException {
        carbonContext.sql (createTableCommand);
        carbonContext.sql(loadTableCommand);
    }

    public void closeContext(CarbonContext carbonContext) {
        carbonContext.sparkContext().cancelAllJobs();
        carbonContext.sparkContext().stop();
    }
}
