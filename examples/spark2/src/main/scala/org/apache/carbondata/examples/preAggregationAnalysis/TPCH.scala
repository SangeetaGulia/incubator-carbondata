package org.apache.carbondata.examples.preAggregationAnalysis

import java.io.File
import java.util

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties


case class Result(query:String, time: Double)

object TPCH extends App {

  val listOfResult = new util.ArrayList[Result]()

  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath
  val storeLocation = s"hdfs://localhost:54310/prestoCarbonStore"
  val warehouse = s"$rootPath/integration/presto/target/warehouse"
  val metastoredb = s"$rootPath/integration/presto/target/metastore_db"
  val csvPath = "/home/sangeeta/Downloads/tpch-store-2"

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


  def time(resultString: String)(code: => Unit): Unit = {
    val start = System.currentTimeMillis()
    code
    val time = (System.currentTimeMillis() - start).toDouble / 1000
    listOfResult.add(Result(code.toString, time))
    println(s"\n\n $resultString, Query ${code.toString} executed in (sec):::: "+ (time))
  }

    carbon.sql("DROP TABLE IF EXISTS NATION")

    carbon
      .sql(
        "create table if not exists NATION ( N_NAME string, N_NATIONKEY string, N_REGIONKEY " +
        "string, N_COMMENT string ) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('DICTIONARY_EXCLUDE'='N_COMMENT', 'table_blocksize'='128')")

    carbon.sql(s"LOAD DATA INPATH '$csvPath/nation.tbl' INTO TABLE nation " +
               "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='N_NATIONKEY,N_NAME," +
               "N_REGIONKEY,N_COMMENT')")

    carbon.sql("DROP TABLE IF EXISTS REGION")

    carbon
      .sql(
        "create table if not exists REGION( R_NAME string, R_REGIONKEY string, R_COMMENT string )" +
        " STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('DICTIONARY_EXCLUDE'='R_COMMENT', 'table_blocksize'='128')")

    carbon.sql(s"LOAD DATA INPATH '$csvPath/region.tbl' INTO TABLE region " +
               "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='R_REGIONKEY,R_NAME," +
               "R_COMMENT')")

    carbon.sql("DROP TABLE IF EXISTS PART")

    carbon
      .sql(
        "create table if not exists PART( P_BRAND string, P_SIZE int, P_CONTAINER string, P_TYPE " +
        "string, P_PARTKEY string, P_NAME string, P_MFGR string, P_RETAILPRICE double, P_COMMENT " +
        "string ) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('DICTIONARY_INCLUDE'='P_SIZE','DICTIONARY_EXCLUDE'='P_PARTKEY, P_NAME, P_COMMENT', " +
        "'table_blocksize'='128')")

    carbon
      .sql(s"LOAD DATA INPATH '$csvPath/part.tbl' INTO TABLE part OPTIONS" +
           "('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='P_PARTKEY,P_NAME,P_MFGR,P_BRAND," +
           "P_TYPE,P_SIZE,P_CONTAINER,P_RETAILPRICE,P_COMMENT')")

    carbon.sql("DROP TABLE IF EXISTS SUPPLIER ")

    carbon
      .sql(
        "create table if not exists SUPPLIER( S_COMMENT string, S_SUPPKEY string, S_NAME string, " +
        "S_ADDRESS string, S_NATIONKEY string, S_PHONE string, S_ACCTBAL double ) STORED BY 'org" +
        ".apache.carbondata.format' TBLPROPERTIES ('DICTIONARY_EXCLUDE'='S_COMMENT, S_SUPPKEY, " +
        "S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE' , 'table_blocksize'='128')")

    carbon
      .sql(s"LOAD DATA INPATH '$csvPath/supplier.tbl' INTO TABLE supplier " +
           "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'=' S_SUPPKEY,             " +
           "S_NAME,S_ADDRESS,S_NATIONKEY,S_PHONE,S_ACCTBAL,S_COMMENT')")

    carbon.sql("DROP TABLE IF EXISTS PARTSUPP ")

    carbon
      .sql(
        "create table if not exists PARTSUPP ( PS_PARTKEY string, PS_SUPPKEY string, PS_AVAILQTY " +
        "int, PS_SUPPLYCOST double, PS_COMMENT string ) STORED BY 'org.apache.carbondata.format' " +
        "TBLPROPERTIES ('DICTIONARY_EXCLUDE'='PS_PARTKEY, PS_SUPPKEY, PS_COMMENT', " +
        "'table_blocksize'='128')")

    carbon
      .sql(s"LOAD DATA INPATH '$csvPath/partsupp.tbl' INTO TABLE partsupp " +
           "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='PS_PARTKEY,PS_SUPPKEY ," +
           "PS_AVAILQTY,PS_SUPPLYCOST,PS_COMMENT')")

    carbon.sql("DROP TABLE IF EXISTS CUSTOMER")

    carbon
      .sql(
        "create table if not exists CUSTOMER( C_MKTSEGMENT string, C_NATIONKEY string, C_CUSTKEY " +
        "string, C_NAME string, C_ADDRESS string, C_PHONE string, C_ACCTBAL double, C_COMMENT " +
        "string ) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('DICTIONARY_EXCLUDE'='C_CUSTKEY,C_NAME,C_ADDRESS,C_PHONE,C_COMMENT', " +
        "'table_blocksize'='128')")

    carbon
      .sql(s"LOAD DATA INPATH '$csvPath/customer.tbl' INTO TABLE customer " +
           "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"' , 'FILEHEADER'='C_CUSTKEY,C_NAME," +
           "C_ADDRESS,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMENT')")

    carbon.sql("DROP TABLE IF EXISTS ORDERS ")

    carbon
      .sql(
        "create table if not exists ORDERS( O_ORDERDATE date, O_ORDERPRIORITY string, " +
        "O_ORDERSTATUS string, O_ORDERKEY string, O_CUSTKEY string, O_TOTALPRICE double, O_CLERK " +
        "string, O_SHIPPRIORITY int, O_COMMENT string ) STORED BY 'org.apache.carbondata.format' " +
        "TBLPROPERTIES ('DICTIONARY_EXCLUDE'='O_ORDERKEY, O_CUSTKEY, O_CLERK, O_COMMENT', " +
        "'table_blocksize'='128','no_inverted_index'='O_ORDERKEY, O_CUSTKEY, O_CLERK, O_COMMENT')")

    carbon.sql(s"LOAD DATA INPATH '$csvPath/orders.tbl' INTO TABLE orders " +
               "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='O_ORDERKEY,O_CUSTKEY," +
               "O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY," +
               "O_COMMENT')")

    carbon.sql("DROP TABLE IF EXISTS LINEITEM")
    carbon.sql(
      "create table if not exists lineitem( L_SHIPDATE date, L_SHIPMODE string, L_SHIPINSTRUCT " +
      "string, L_RETURNFLAG string, L_RECEIPTDATE date, L_ORDERKEY string, L_PARTKEY string, " +
      "L_SUPPKEY string, L_LINENUMBER int, L_QUANTITY double, L_EXTENDEDPRICE double, " +
      "L_DISCOUNT double, L_TAX double, L_LINESTATUS string, L_COMMITDATE date, L_COMMENT " +
      "string ) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
      "('DICTIONARY_EXCLUDE'='L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_COMMENT', " +
      "'table_blocksize'='128', 'no_inverted_index'='L_ORDERKEY, L_PARTKEY, L_SUPPKEY, " +
      "L_COMMENT')")

    carbon.sql(s"LOAD DATA INPATH '$csvPath/lineitem.tbl' INTO TABLE lineitem " +
               "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'=' L_ORDERKEY,L_PARTKEY," +
               "L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG," +
               "L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE," +
               "L_COMMENT')")

  carbon.sql("show datamap on table lineitem").show()

    time("Before Aggregation"){carbon.sql("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= cast('1998-09-16' as Timestamp) group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus").show()}

  time("Before Aggregation"){carbon.sql("select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate <  cast('1995-03-22' as timestamp) and l_shipdate > cast('1995-03-22' as timestamp) group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10").show()}

  time("Before Aggregation"){carbon.sql("select o_orderpriority, count(*) as order_count from orders as o where o_orderdate >= cast('1996-05-01' as timestamp) and o_orderdate < cast('1996-08-01' as timestamp) and exists (select * from lineitem where l_orderkey = o.o_orderkey and l_commitdate < l_receiptdate) group by o_orderpriority order by o_orderpriority").show()}

  time("Before Aggregation"){carbon.sql("select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AFRICA' and o_orderdate >= cast('1993-01-01' as timestamp) and o_orderdate < cast('1994-01-01' as timestamp) group by n_name order by revenue desc").show()}

  time("Before Aggregation"){carbon.sql("select o_orderpriority, count(*) as order_count from orders as o where o_orderdate >= cast('1996-05-01' as timestamp) and o_orderdate < cast('1996-08-01' as timestamp) and exists (select * from lineitem where l_orderkey = o.o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority").show()}


  carbon
    .sql(
      "create datamap lineitem_agg8 on table lineitem using 'preaggregate' as select " +
      "l_returnflag, l_linestatus, sum(l_quantity), sum(l_extendedprice), sum(l_extendedprice*(1-l_discount)), sum(l_extendedprice*(1-l_discount)*(1+l_tax)), avg(l_quantity), avg(l_extendedprice), avg(l_discount) from lineitem   group by l_returnflag, l_linestatus")


  carbon
    .sql(
      "create datamap lineitem_agg1 on table lineitem using 'preaggregate' as select " +
      "l_returnflag, l_linestatus, sum(l_quantity) from lineitem  where l_shipdate <= " +
      "'1998-09-02' group by l_returnflag, l_linestatus")
  carbon
    .sql(
      "create datamap lineitem_agg2 on table lineitem using 'preaggregate' as select " +
      "l_returnflag, l_linestatus, sum(l_extendedprice) from lineitem where l_shipdate <= " +
      "'1998-09-02' group by l_returnflag, l_linestatus")
  carbon
    .sql(
      "create datamap lineitem_agg3 on table lineitem using 'preaggregate' as select " +
      "l_returnflag, l_linestatus, sum(l_extendedprice*(1-l_discount)) from lineitem where " +
      "l_shipdate <= '1998-09-02' group by l_returnflag, l_linestatus")
  carbon
    .sql(
      "create datamap lineitem_agg4 on table lineitem using 'preaggregate' as select " +
      "l_returnflag, l_linestatus, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) from lineitem  " +
      "where l_shipdate <= '1998-09-02' group by l_returnflag, l_linestatus")


  time("After Aggregation"){carbon.sql("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= cast('1998-09-16' as Timestamp) group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus").show()}

  time("After Aggregation"){carbon.sql("select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate <  cast('1995-03-22' as timestamp) and l_shipdate > cast('1995-03-22' as timestamp) group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10").show()}

  time("After Aggregation"){carbon.sql("select o_orderpriority, count(*) as order_count from orders as o where o_orderdate >= cast('1996-05-01' as timestamp) and o_orderdate < cast('1996-08-01' as timestamp) and exists (select * from lineitem where l_orderkey = o.o_orderkey and l_commitdate < l_receiptdate) group by o_orderpriority order by o_orderpriority").show()}

  time("After Aggregation"){carbon.sql("select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AFRICA' and o_orderdate >= cast('1993-01-01' as timestamp) and o_orderdate < cast('1994-01-01' as timestamp) group by n_name order by revenue desc").show()}

  time("After Aggregation"){carbon.sql("select o_orderpriority, count(*) as order_count from orders as o where o_orderdate >= cast('1996-05-01' as timestamp) and o_orderdate < cast('1996-08-01' as timestamp) and exists (select * from lineitem where l_orderkey = o.o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority").show()}

  println(listOfResult)

}
