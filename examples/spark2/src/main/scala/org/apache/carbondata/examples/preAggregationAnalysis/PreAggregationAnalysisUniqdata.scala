package org.apache.carbondata.examples.preAggregationAnalysis

import java.io.File

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties


object PreAggregationAnalysisUniqdata extends App {

  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath
  val storeLocation = s"hdfs://localhost:54310/prestoCarbonStore"
  val warehouse = s"$rootPath/integration/presto/target/warehouse"
  val metastoredb = s"$rootPath/integration/presto/target/metastore_db"
  val csvPath = "/home/hduser/InputFiles/uniqdata/1lac_UniqData.csv"

  import org.apache.spark.sql.CarbonSession._
  import org.apache.spark.sql.SparkSession

  val carbon = SparkSession
    .builder()
    .master("local")
    .appName("CompareTestExample")
    .config("carbon.sql.warehouse.dir", warehouse).enableHiveSupport()
    .getOrCreateCarbonSession(
      s"$storeLocation", metastoredb)

  CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.LOCK_TYPE, CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS)
  CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION, "true")
  CarbonProperties.getInstance().addProperty(CarbonCommonConstants.SUPPORT_DIRECT_QUERY_ON_DATAMAP, "true")


  def time(resultString: String)(code: => Unit): Unit = {
    val start = System.currentTimeMillis()
    code
    println(s"\n\n $resultString, Query $code executed in (sec):::: "+ (System.currentTimeMillis() - start).toDouble / 1000)
  }
/*

  carbon.sql("drop table if exists uniqdata")

  carbon.sql("CREATE TABLE uniqdata (CUST_ID int,CUST_NAME char(30),ACTIVE_EMUI_VERSION " +
             "char(20), DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 " +
             "bigint,DECIMAL_COLUMN1 decimal(30,10),DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1" +
             " double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache" +
             ".carbondata.format' TBLPROPERTIES (\"NO_INVERTED_INDEX\"=\"cust_name\")")

  carbon.sql(s"LOAD DATA INPATH '$csvPath' into table " +
             "uniqdata OPTIONS ('DELIMITER'=',' ,'QUOTECHAR'='\"'," +
             "'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION," +
             "DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2," +
             "Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','MAXCOLUMNS'='13')")
*/


  carbon.sql("drop datamap if exists timeseries_agg on table uniqdata")

  time("Before Pre-Aggregation") {
    val df = carbon.sql("select DOB, max(DOUBLE_COLUMN1) from uniqdata group by DOB")
    println("Logical Plan before Pre-aggregation" + df.queryExecution.analyzed.toString())
  }

  time("Before Pre-Aggregation") {
    val df = carbon.sql("select DOB, max(DOUBLE_COLUMN1) from uniqdata group by DOB")
    println("Logical Plan before Pre-aggregation" + df.queryExecution.analyzed.toString())
  }

  time("Before Pre-Aggregation") {
    val df = carbon.sql("select DOB, max(DOUBLE_COLUMN1) from uniqdata group by DOB")
    println("Logical Plan before Pre-aggregation" + df.queryExecution.analyzed.toString())
  }

  val df = carbon.sql("create datamap timeseries_agg on table uniqdata using 'timeseries' dmproperties('event_time'='DOB','year_granularity'='1') as select DOB, max(DOUBLE_COLUMN1) from uniqdata group by DOB")

  carbon.sql("show datamap on table uniqdata").show()

  time("After Pre-Aggregation") {
    val df = carbon.sql("select timeseries(DOB,'year'),max(DOUBLE_COLUMN1) from uniqdata group by timeseries(DOB,'year')")
    println("Logical Plan after Pre-aggregation" + df.queryExecution.analyzed.toString())
  }

  time("After Pre-Aggregation") {
    val df = carbon.sql("select timeseries(DOB,'year'),max(DOUBLE_COLUMN1) from uniqdata group by timeseries(DOB,'year')")
    println("Logical Plan after Pre-aggregation" + df.queryExecution.analyzed.toString())
  }

  time("After Pre-Aggregation") {
    val df = carbon.sql("select timeseries(DOB,'year'),max(DOUBLE_COLUMN1) from uniqdata group by timeseries(DOB,'year')")
    println("Logical Plan after Pre-aggregation" + df.queryExecution.analyzed.toString())
  }

  /*time("After Pre-Aggregation") {
    val df = carbon.sql("select max(DOUBLE_COLUMN1), timeseries(DOB,'month') from uniqdata group by timeseries(DOB,'month')")
    println("Logical Plan after Pre-aggregation" + df.queryExecution.analyzed.toString())
  }*/

}
