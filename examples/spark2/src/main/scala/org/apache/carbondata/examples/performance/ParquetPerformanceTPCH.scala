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

package org.apache.carbondata.examples.performance

import java.io.File
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.commons.io.FileUtils
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}

// This is used to capture the performance of Parquet on Spark.

object ParquetPerformanceTPCH {

  import org.apache.spark.sql.CarbonSession._


  // Run all queries for the specified table
  private def time(code: => Unit): Double = {
    val start = System.currentTimeMillis()
    code
    // return time in second
    (System.currentTimeMillis() - start).toDouble / 1000
  }

  def evaluateTimeForQuery(queryList: List[String], spark: SparkSession): List[(String, Double)] = {
    queryList.map { query =>
      println(" -----------------------------------------------------------------------------------------")
      println("                                      Executing Query :                                     \n" + query)
      println(" -----------------------------------------------------------------------------------------")

      val querytime = time {
        spark.sql(query).show()
      }

      (query.replaceAll("  ", ""), querytime)
    }
  }


  def main(args: Array[String]): Unit = {

    val LOGGER = LogServiceFactory.getLogService(ParquetPerformanceTPCH.getClass.getName)

    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target/metastore_db"
    val customerCsvPath = "/home/sangeeta/Desktop/test/customer.csv"
    val lineItemCsvPath = "/home/sangeeta/Desktop/test/lineitem.csv"
    val nationCsvPath = "/home/sangeeta/Desktop/test/nation.csv"
    val ordersCsvPath = "/home/sangeeta/Desktop/test/orders.csv"
    val partCsvPath = "/home/sangeeta/Desktop/test/part.csv"
    val partSupCsvPath = "/home/sangeeta/Desktop/test/partsupp.csv"
    val regionCsvPath = "/home/sangeeta/Desktop/test/region.csv"
    val supplierCsvPath = "/home/sangeeta/Desktop/test/supplier.csv"

    // clean data folder
    if (true) {
      val clean = (path: String) => FileUtils.deleteDirectory(new File(path))
      clean(storeLocation)
      clean(warehouse)
      clean(metastoredb)
    }

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    CarbonProperties.getInstance()
      .addProperty("carbon.storelocation", storeLocation)

    spark.sparkContext.setLogLevel("WARN")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")

    // Create table REGION

/*    LOGGER.info("\n\n --------------------------- Creating table REGION ---------------------------------")
    spark.sql(
      """
        |drop table if exists REGIONTEXTFILE
      """.stripMargin)

    spark.sql(
      """
        |drop table if exists REGION
      """.stripMargin
    )

    spark.sql(
      s"""CREATE TABLE REGION ( R_REGIONKEY INT ,
         | R_NAME STRING ,
         | R_COMMENT STRING) stored as parquet
    """.stripMargin
    ).show()

    spark.sql(
      s"""CREATE TABLE REGIONTEXTFILE ( R_REGIONKEY INT ,
         | R_NAME STRING ,
         | R_COMMENT STRING) row format delimited fields terminated by '|' stored as textfile
    """.stripMargin
    ).show()

    spark.sql(
      s"""
         | LOAD DATA LOCAL inpath '${regionCsvPath}' INTO table REGIONTEXTFILE
       """.stripMargin
    ).show()

    spark.sql(
      s"""
         |insert overwrite table REGION select * from REGIONTEXTFILE
       """.stripMargin).show()

    spark.sql(
      """
        |select * from REGION
      """.stripMargin).show()

    LOGGER.info("\n\n --------------------------- Creating table NATION ---------------------------------")

    spark.sql(
      """
        |drop table if exists NATIONTEXTFILE
      """.stripMargin)

    spark.sql(
      """
        |drop table if exists NATION
      """.stripMargin
    )

    spark.sql(
        s"""CREATE TABLE NATION ( N_NATIONKEY INT,
           | N_NAME STRING,
           | N_REGIONKEY INT,
           | N_COMMENT STRING) stored as parquet
    """.stripMargin
      )

    spark.sql(
        s"""CREATE TABLE NATIONTEXTFLE ( N_NATIONKEY INT,
           | N_NAME STRING,
           | N_REGIONKEY INT,
           | N_COMMENT STRING) row format delimited fields terminated by '|' stored as textfile
    """.stripMargin
      )

    spark.sql(
      s"""
         | LOAD DATA LOCAL inpath '${nationCsvPath}' INTO table NATIONTEXTFLE
       """.stripMargin
    ).show()

    spark.sql(
      s"""
         |insert overwrite table NATION select * from NATIONTEXTFLE
       """.stripMargin).show()

    spark.sql(
      """
        |select * from NATION
      """.stripMargin).show()


    LOGGER.info("\n\n --------------------------- Creating table PART ---------------------------------")

    spark.sql(
      """
        |drop table if exists PARTTEXTFILE
      """.stripMargin)

    spark.sql(
      """
        |drop table if exists PART
      """.stripMargin
    )

      spark.sql(
        """
          |CREATE TABLE PART ( P_PARTKEY INT ,
          | P_NAME STRING ,
          | P_MFGR STRING ,
          | P_BRAND STRING ,
          | P_TYPE STRING ,
          | P_SIZE INT ,
          | P_CONTAINER STRING ,
          | P_RETAILPRICE DECIMAL(15,2) ,
          | P_COMMENT STRING ) stored as parquet
        """.stripMargin
      ).show()

    spark.sql(
      """
        |CREATE TABLE PARTTEXTFILE ( P_PARTKEY INT ,
        | P_NAME STRING ,
        | P_MFGR STRING ,
        | P_BRAND STRING ,
        | P_TYPE STRING ,
        | P_SIZE INT ,
        | P_CONTAINER STRING ,
        | P_RETAILPRICE DECIMAL(15,2) ,
        | P_COMMENT STRING ) row format delimited fields terminated by '|' stored as textfile
      """.stripMargin
    ).show()

    spark.sql(
      s"""
         | LOAD DATA LOCAL inpath '${partCsvPath}' INTO table PARTTEXTFILE
       """.stripMargin
    ).show()

    spark.sql(
      s"""
         |insert overwrite table PART select * from PARTTEXTFILE
       """.stripMargin).show()

    spark.sql(
      """
        |select * from PART
      """.stripMargin).show()

    LOGGER.info("\n\n --------------------------- Creating table SUPPLIER ---------------------------------")

    spark.sql(
      """
        |drop table if exists SUPPLIERTEXTFILE
      """.stripMargin)

    spark.sql(
      """
        |drop table if exists SUPPLIER
      """.stripMargin
    )

    spark.sql(
        s"""
        | CREATE TABLE SUPPLIER ( S_SUPPKEY INT ,
        | S_NAME STRING ,
        | S_ADDRESS STRING ,
        | S_NATIONKEY INT ,
        | S_PHONE STRING ,
        | S_ACCTBAL DECIMAL(15,2) ,
        | S_COMMENT STRING ) stored as parquet
      """.stripMargin
      )

    spark.sql(
      s"""
         | CREATE TABLE SUPPLIERTEXTFILE ( S_SUPPKEY INT ,
         | S_NAME STRING ,
         | S_ADDRESS STRING ,
         | S_NATIONKEY INT ,
         | S_PHONE STRING ,
         | S_ACCTBAL DECIMAL(15,2) ,
         | S_COMMENT STRING ) row format delimited fields terminated by '|' stored as textfile
      """.stripMargin
    )

    spark.sql(
      s"""
         | LOAD DATA LOCAL inpath '${supplierCsvPath}' INTO table SUPPLIERTEXTFILE
       """.stripMargin
    ).show()

    spark.sql(
      s"""
         |insert overwrite table SUPPLIER select * from SUPPLIERTEXTFILE
       """.stripMargin).show()

    spark.sql(
      """
        |select * from SUPPLIER
      """.stripMargin).show()

    LOGGER.info("\n\n\n\n --------------------------- Creating table PARTSUPP ---------------------------------")

    spark.sql(
      """
        |drop table if exists PARTSUPPTEXTFILE
      """.stripMargin)

    spark.sql(
      """
        |drop table if exists PARTSUPP
      """.stripMargin
    )

    spark.sql(
        s"""
        | CREATE TABLE PARTSUPP ( PS_PARTKEY INT ,
        | PS_SUPPKEY INT ,
        | PS_AVAILQTY INT ,
        | PS_SUPPLYCOST DECIMAL(15,2) ,
        | PS_COMMENT STRING ) stored as parquet
      """.stripMargin
      ).show()

    spark.sql(
      s"""
         | CREATE TABLE PARTSUPPTEXTFILE ( PS_PARTKEY INT ,
         | PS_SUPPKEY INT ,
         | PS_AVAILQTY INT ,
         | PS_SUPPLYCOST DECIMAL(15,2) ,
         | PS_COMMENT STRING ) row format delimited fields terminated by '|' stored as textfile
      """.stripMargin
    ).show()

    spark.sql(
      s"""
         | LOAD DATA LOCAL inpath '${partSupCsvPath}' INTO table PARTSUPPTEXTFILE
       """.stripMargin
    ).show()

    spark.sql(
      s"""
         |insert overwrite table PARTSUPP select * from PARTSUPPTEXTFILE
       """.stripMargin).show()

    spark.sql(
      """
        |select * from PARTSUPP
      """.stripMargin).show()

    LOGGER.info("\n\n --------------------------- Creating table CUSTOMER ---------------------------------")

    spark.sql(
      """
        |drop table if exists CUSTOMERTEXTFILE
      """.stripMargin)

    spark.sql(
      """
        |drop table if exists CUSTOMER
      """.stripMargin
    )

    spark.sql(
         s"""
         | CREATE TABLE CUSTOMER ( C_CUSTKEY INT ,
         | C_NAME STRING ,
         | C_ADDRESS STRING ,
         | C_NATIONKEY INT ,
         | C_PHONE STRING ,
         | C_ACCTBAL DECIMAL(15,2) ,
         | C_MKTSEGMENT STRING ,
         | C_COMMENT STRING ) stored as parquet
      """.stripMargin
      ).show()

    spark.sql(
      s"""
         | CREATE TABLE CUSTOMERTEXTFILE ( C_CUSTKEY INT ,
         | C_NAME STRING ,
         | C_ADDRESS STRING ,
         | C_NATIONKEY INT ,
         | C_PHONE STRING ,
         | C_ACCTBAL DECIMAL(15,2) ,
         | C_MKTSEGMENT STRING ,
         | C_COMMENT STRING ) row format delimited fields terminated by '|' stored as textfile
      """.stripMargin
    ).show()

    spark.sql(
      s"""
         | LOAD DATA LOCAL inpath '${customerCsvPath}' INTO table CUSTOMERTEXTFILE
       """.stripMargin
    ).show()

    spark.sql(
      s"""
         |insert overwrite table CUSTOMER select * from CUSTOMERTEXTFILE
       """.stripMargin).show()

    spark.sql(
      """
        |select * from CUSTOMER
      """.stripMargin).show()

    LOGGER.info("\n\n --------------------------- Creating table ORDERS ---------------------------------")

    spark.sql(
      """
        |drop table if exists ORDERSTEXTFILE
      """.stripMargin)

    spark.sql(
      """
        |drop table if exists ORDERS
      """.stripMargin
    )

    spark.sql(
        s"""
        | CREATE TABLE ORDERS ( O_ORDERKEY INT ,
        | O_CUSTKEY INT ,
        | O_ORDERSTATUS STRING ,
        | O_TOTALPRICE DECIMAL(15,2) ,
        | O_ORDERDATE TIMESTAMP ,
        | O_ORDERPRIORITY STRING ,
        | O_CLERK STRING ,
        | O_SHIPPRIORITY INT ,
        | O_COMMENT STRING ) stored as parquet
      """.stripMargin
      ).show()

    spark.sql(
      s"""
         | CREATE TABLE ORDERSTEXTFILE ( O_ORDERKEY INT ,
         | O_CUSTKEY INT ,
         | O_ORDERSTATUS STRING ,
         | O_TOTALPRICE DECIMAL(15,2) ,
         | O_ORDERDATE TIMESTAMP ,
         | O_ORDERPRIORITY STRING ,
         | O_CLERK STRING ,
         | O_SHIPPRIORITY INT ,
         | O_COMMENT STRING ) row format delimited fields terminated by '|' stored as textfile
      """.stripMargin
    ).show()

    spark.sql(
      s"""
         | LOAD DATA LOCAL inpath '${ordersCsvPath}' INTO table ORDERSTEXTFILE
       """.stripMargin
    ).show()

    spark.sql(
      s"""
         |insert overwrite table ORDERS select * from ORDERSTEXTFILE
       """.stripMargin).show()

    spark.sql(
      """
        |select * from ORDERS
      """.stripMargin).show()*/

    LOGGER.info("\n\n --------------------------- Creating table LINEITEM ---------------------------------")

    spark.sql(
      """
        |drop table if exists LINEITEMTEXTFILE
      """.stripMargin)

    spark.sql(
      """
        |drop table if exists LINEITEM
      """.stripMargin
    )

    spark.sql(
        s"""
        | CREATE TABLE LINEITEM ( L_ORDERKEY INT ,
        | L_PARTKEY INT ,
        | L_SUPPKEY INT ,
        | L_LINENUMBER INT ,
        | L_QUANTITY DECIMAL(15,2) ,
        | L_EXTENDEDPRICE DECIMAL(15,2) ,
        | L_DISCOUNT DECIMAL(15,2) ,
        | L_TAX DECIMAL(15,2) ,
        | L_RETURNFLAG STRING ,
        | L_LINESTATUS STRING ,
        | L_SHIPDATE TIMESTAMP ,
        | L_COMMITDATE TIMESTAMP ,
        | L_RECEIPTDATE TIMESTAMP ,
        | L_SHIPINSTRUCT STRING ,
        | L_SHIPMODE STRING ,
        | L_COMMENT STRING ) stored as parquet
      """.stripMargin
      ).show()

    spark.sql(
      s"""
         | CREATE TABLE LINEITEMTEXTFILE ( L_ORDERKEY INT ,
         | L_PARTKEY INT ,
         | L_SUPPKEY INT ,
         | L_LINENUMBER INT ,
         | L_QUANTITY DECIMAL(15,2) ,
         | L_EXTENDEDPRICE DECIMAL(15,2) ,
         | L_DISCOUNT DECIMAL(15,2) ,
         | L_TAX DECIMAL(15,2) ,
         | L_RETURNFLAG STRING ,
         | L_LINESTATUS STRING ,
         | L_SHIPDATE TIMESTAMP ,
         | L_COMMITDATE TIMESTAMP ,
         | L_RECEIPTDATE TIMESTAMP ,
         | L_SHIPINSTRUCT STRING ,
         | L_SHIPMODE STRING ,
         | L_COMMENT STRING ) row format delimited fields terminated by '|' stored as textfile
      """.stripMargin
    ).show()

    spark.sql(
      s"""
         | LOAD DATA LOCAL inpath '${lineItemCsvPath}' INTO table LINEITEMTEXTFILE
       """.stripMargin
    ).show()

    spark.sql(
      s"""
         |insert overwrite table LINEITEM select * from LINEITEMTEXTFILE
       """.stripMargin).show()

    spark.sql(
      """
        |select * from LINEITEM
      """.stripMargin).show()


    // ------------------------------------------ TPCH QUERIES -----------------------------------------------------

    val queryList: List[String] = List(
      """select
        |        l_returnflag,
        |        l_linestatus,
        |        sum(l_quantity) as sum_qty,
        |        sum(l_extendedprice) as sum_base_price,
        |        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        |        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        |        avg(l_quantity) as avg_qty,
        |        avg(l_extendedprice) as avg_price,
        |        avg(l_discount) as avg_disc,
        |        count(*) as count_order
        |from
        |        lineitem
        |where
        |        l_shipdate <= '1998-09-16'
        |group by
        |        l_returnflag,
        |        l_linestatus
        |order by
        |        l_returnflag,
        |        l_linestatus""".stripMargin,


      """select
        |        l_orderkey,
        |        sum(l_extendedprice * (1 - l_discount)) as revenue,
        |        o_orderdate,
        |        o_shippriority
        |from
        |        customer,
        |        orders,
        |        lineitem
        |where
        |        c_mktsegment = 'BUILDING'
        |        and c_custkey = o_custkey
        |        and l_orderkey = o_orderkey
        |        and o_orderdate < '1995-03-22'
        |        and l_shipdate > '1995-03-22'
        |group by
        |        l_orderkey,
        |        o_orderdate,
        |        o_shippriority
        |order by
        |        revenue desc,
        |        o_orderdate
        |limit 10""".stripMargin,

      """select
        |        o_orderpriority,
        |        count(*) as order_count
        |from
        |        orders as o
        |where
        |        o_orderdate >= '1996-05-01'
        |        and o_orderdate < '1996-08-01'
        |        and exists (
        |                select
        |                        *
        |                from
        |                        lineitem
        |                where
        |                        l_orderkey = o.o_orderkey
        |                        and l_commitdate < l_receiptdate
        |        )
        |group by
        |        o_orderpriority
        |order by
        |        o_orderpriority""".stripMargin,


      """select
        |        n_name,
        |        sum(l_extendedprice * (1 - l_discount)) as revenue
        |from
        |        customer,
        |        orders,
        |        lineitem,
        |        supplier,
        |        nation,
        |        region
        |where
        |        c_custkey = o_custkey
        |        and l_orderkey = o_orderkey
        |        and l_suppkey = s_suppkey
        |        and c_nationkey = s_nationkey
        |        and s_nationkey = n_nationkey
        |        and n_regionkey = r_regionkey
        |        and r_name = 'AFRICA'
        |        and o_orderdate >= '1993-01-01'
        |        and o_orderdate < '1994-01-01'
        |group by
        |        n_name
        |order by
        |        revenue desc""".stripMargin,


      """select
        |        sum(l_extendedprice * l_discount) as revenue
        |from
        |        lineitem
        |where
        |        l_shipdate >= '1993-01-01'
        |        and l_shipdate < '1994-01-01'
        |        and l_discount between 0.06 - 0.01 and 0.06 + 0.01
        |        and l_quantity < 25""".stripMargin,


      """select
        |        supp_nation,
        |        cust_nation,
        |        l_year,
        |        sum(volume) as revenue
        |from
        |        (
        |                select
        |                        n1.n_name as supp_nation,
        |                        n2.n_name as cust_nation,
        |                        year(l_shipdate) as l_year,
        |                        l_extendedprice * (1 - l_discount) as volume
        |                from
        |                        supplier,
        |                        lineitem,
        |                        orders,
        |                        customer,
        |                        nation n1,
        |                        nation n2
        |                where
        |                        s_suppkey = l_suppkey
        |                        and o_orderkey = l_orderkey
        |                        and c_custkey = o_custkey
        |                        and s_nationkey = n1.n_nationkey
        |                        and c_nationkey = n2.n_nationkey
        |                        and (
        |                                (n1.n_name = 'KENYA' and n2.n_name = 'PERU')
        |                                or (n1.n_name = 'PERU' and n2.n_name = 'KENYA')
        |                        )
        |                        and l_shipdate between '1995-01-01' and '1996-12-31'
        |        ) as shipping
        |group by
        |        supp_nation,
        |        cust_nation,
        |        l_year
        |order by
        |        supp_nation,
        |        cust_nation,
        |        l_year""".stripMargin,


      """select
        |        o_year,
        |        sum(case
        |                when nation = 'PERU' then volume
        |                else 0
        |        end) / sum(volume) as mkt_share
        |from
        |        (
        |                select
        |                        year(o_orderdate) as o_year,
        |                        l_extendedprice * (1 - l_discount) as volume,
        |                        n2.n_name as nation
        |                from
        |                        part,
        |                        supplier,
        |                        lineitem,
        |                        orders,
        |                        customer,
        |                        nation n1,
        |                        nation n2,
        |                        region
        |                where
        |                        p_partkey = l_partkey
        |                        and s_suppkey = l_suppkey
        |                        and l_orderkey = o_orderkey
        |                        and o_custkey = c_custkey
        |                        and c_nationkey = n1.n_nationkey
        |                        and n1.n_regionkey = r_regionkey
        |                        and r_name = 'AMERICA'
        |                        and s_nationkey = n2.n_nationkey
        |                        and o_orderdate between '1995-01-01' and '1996-12-31'
        |                        and p_type = 'ECONOMY BURNISHED NICKEL'
        |        ) as all_nations
        |group by
        |        o_year
        |order by
        |        o_year""".stripMargin,


      """select
        |        nation,
        |        o_year,
        |        sum(amount) as sum_profit
        |from
        |        (
        |                select
        |                        n_name as nation,
        |                        year(o_orderdate) as o_year,
        |                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        |                from
        |                        part,
        |                        supplier,
        |                        lineitem,
        |                        partsupp,
        |                        orders,
        |                        nation
        |                where
        |                        s_suppkey = l_suppkey
        |                        and ps_suppkey = l_suppkey
        |                        and ps_partkey = l_partkey
        |                        and p_partkey = l_partkey
        |                        and o_orderkey = l_orderkey
        |                        and s_nationkey = n_nationkey
        |                        and p_name like '%plum%'
        |        ) as profit
        |group by
        |        nation,
        |        o_year
        |order by
        |        nation,
        |        o_year desc""".stripMargin,


      """select
        |        c_custkey,
        |        c_name,
        |        sum(l_extendedprice * (1 - l_discount)) as revenue,
        |        c_acctbal,
        |        n_name,
        |        c_address,
        |        c_phone,
        |        c_comment
        |from
        |        customer,
        |        orders,
        |        lineitem,
        |        nation
        |where
        |        c_custkey = o_custkey
        |        and l_orderkey = o_orderkey
        |        and o_orderdate >= '1993-07-01'
        |        and o_orderdate < '1993-10-01'
        |        and l_returnflag = 'R'
        |        and c_nationkey = n_nationkey
        |group by
        |        c_custkey,
        |        c_name,
        |        c_acctbal,
        |        c_phone,
        |        n_name,
        |        c_address,
        |        c_comment
        |order by
        |        revenue desc
        |limit 20""".stripMargin,


      """select
        |        l_shipmode,
        |        sum(case
        |                when o_orderpriority = '1-URGENT'
        |                        or o_orderpriority = '2-HIGH'
        |                        then 1
        |                else 0
        |        end) as high_line_count,
        |        sum(case
        |                when o_orderpriority <> '1-URGENT'
        |                        and o_orderpriority <> '2-HIGH'
        |                        then 1
        |                else 0
        |        end) as low_line_count
        |from
        |        orders,
        |        lineitem
        |where
        |        o_orderkey = l_orderkey
        |        and l_shipmode in ('REG AIR', 'MAIL')
        |        and l_commitdate < l_receiptdate
        |        and l_shipdate < l_commitdate
        |        and l_receiptdate >= '1995-01-01'
        |        and l_receiptdate < '1996-01-01'
        |group by
        |        l_shipmode
        |order by
        |        l_shipmode""".stripMargin,


      """select
        |        c_count,
        |        count(*) as custdist
        |from
        |        (
        |                select
        |                        c_custkey,
        |                        count(o_orderkey) as c_count
        |                from
        |                        customer left outer join orders on
        |                                c_custkey = o_custkey
        |                                and o_comment not like '%unusual%accounts%'
        |                group by
        |                        c_custkey
        |        ) c_orders
        |group by
        |        c_count
        |order by
        |        custdist desc,
        |        c_count desc""".stripMargin,


      """select
        |        100.00 * sum(case
        |                when p_type like 'PROMO%'
        |                        then l_extendedprice * (1 - l_discount)
        |                else 0
        |        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
        |from
        |        lineitem,
        |        part
        |where
        |        l_partkey = p_partkey
        |        and l_shipdate >= '1995-08-01'
        |        and l_shipdate < '1995-09-01'""".stripMargin,


      """select
        |        p_brand,
        |        p_type,
        |        p_size,
        |        count(distinct ps_suppkey) as supplier_cnt
        |from
        |        partsupp,
        |        part
        |where
        |        p_partkey = ps_partkey
        |        and p_brand <> 'Brand#34'
        |        and p_type not like 'ECONOMY BRUSHED%'
        |        and p_size in (22, 14, 27, 49, 21, 33, 35, 28)
        |        and partsupp.ps_suppkey not in (
        |                select
        |                        s_suppkey
        |                from
        |                        supplier
        |                where
        |                        s_comment like '%Customer%Complaints%'
        |        )
        |group by
        |        p_brand,
        |        p_type,
        |        p_size
        |order by
        |        supplier_cnt desc,
        |        p_brand,
        |        p_type,
        |        p_size""".stripMargin,


      """with q17_part as (
        |  select p_partkey from part where
        |  p_brand = 'Brand#23'
        |  and p_container = 'MED BOX'
        |),
        |q17_avg as (
        |  select l_partkey as t_partkey, 0.2 * avg(l_quantity) as t_avg_quantity
        |  from lineitem
        |  where l_partkey IN (select p_partkey from q17_part)
        |  group by l_partkey
        |),
        |q17_price as (
        |  select
        |  l_quantity,
        |  l_partkey,
        |  l_extendedprice
        |  from
        |  lineitem
        |  where
        |  l_partkey IN (select p_partkey from q17_part)
        |)
        |select cast(sum(l_extendedprice) / 7.0 as decimal(32,2)) as avg_yearly
        |from q17_avg, q17_price
        |where
        |t_partkey = l_partkey and l_quantity < t_avg_quantity""".stripMargin,


      """select
        |        sum(l_extendedprice* (1 - l_discount)) as revenue
        |from
        |        lineitem,
        |        part
        |where
        |        (
        |                p_partkey = l_partkey
        |                and p_brand = 'Brand#32'
        |                and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        |                and l_quantity >= 7 and l_quantity <= 7 + 10
        |                and p_size between 1 and 5
        |                and l_shipmode in ('AIR', 'AIR REG')
        |                and l_shipinstruct = 'DELIVER IN PERSON'
        |        )
        |        or
        |        (
        |                p_partkey = l_partkey
        |                and p_brand = 'Brand#35'
        |                and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        |                and l_quantity >= 15 and l_quantity <= 15 + 10
        |                and p_size between 1 and 10
        |                and l_shipmode in ('AIR', 'AIR REG')
        |                and l_shipinstruct = 'DELIVER IN PERSON'
        |        )
        |        or
        |        (
        |                p_partkey = l_partkey
        |                and p_brand = 'Brand#24'
        |                and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        |                and l_quantity >= 26 and l_quantity <= 26 + 10
        |                and p_size between 1 and 15
        |                and l_shipmode in ('AIR', 'AIR REG')
        |                and l_shipinstruct = 'DELIVER IN PERSON'
        |        )""".stripMargin,


      """-- explain formatted
        |with tmp1 as (
        |    select p_partkey from part where p_name like 'forest%'
        |),
        |tmp2 as (
        |    select s_name, s_address, s_suppkey
        |    from supplier, nation
        |    where s_nationkey = n_nationkey
        |    and n_name = 'CANADA'
        |),
        |tmp3 as (
        |    select l_partkey, 0.5 * sum(l_quantity) as sum_quantity, l_suppkey
        |    from lineitem, tmp2
        |    where l_shipdate >= '1994-01-01' and l_shipdate <= '1995-01-01'
        |    and l_suppkey = s_suppkey
        |    group by l_partkey, l_suppkey
        |),
        |tmp4 as (
        |    select ps_partkey, ps_suppkey, ps_availqty
        |    from partsupp
        |    where ps_partkey IN (select p_partkey from tmp1)
        |),
        |tmp5 as (
        |select
        |    ps_suppkey
        |from
        |    tmp4, tmp3
        |where
        |    ps_partkey = l_partkey
        |    and ps_suppkey = l_suppkey
        |    and ps_availqty > sum_quantity
        |)
        |select
        |    s_name,
        |    s_address
        |from
        |    supplier
        |where
        |    s_suppkey IN (select ps_suppkey from tmp5)
        |order by s_name""".stripMargin
    )
/*

    Try {
      evaluateTimeForQuery(queryList, spark)
    } match {
      case Success(queryAndTimeList) => println("\n\n\n Estimated time for queries (in seconds)")
        queryAndTimeList foreach { queryAndTime: (String, Double) =>
          println("\n\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
          println(queryAndTime)
          println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        }
      case Failure(exception) =>
        exception.printStackTrace()
        println(" Exception occured " + exception.getMessage)
    }
*/

    spark.stop()
  }
}