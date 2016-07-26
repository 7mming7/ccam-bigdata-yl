#!/usr/bin/env bash

HQL="
CREATE DATABASE $1;
USE $1;

DROP FUNCTION IF EXISTS $1.sysdate;
DROP FUNCTION IF EXISTS $1.firstday;
DROP FUNCTION IF EXISTS $1.lastday;
DROP FUNCTION IF EXISTS $1.todate;
DROP FUNCTION IF EXISTS $1.roundmonth;
DROP FUNCTION IF EXISTS $1.coal;
DROP FUNCTION IF EXISTS $1.month_add;

CREATE FUNCTION $1.sysdate    AS 'com.annuus.hive.udf.UDFSysDate'    USING JAR 'hdfs://bdpha/user/hive/udf.jar';
CREATE FUNCTION $1.firstday   AS 'com.annuus.hive.udf.UDFFirstDay'   USING JAR 'hdfs://bdpha/user/hive/udf.jar';
CREATE FUNCTION $1.lastday    AS 'com.annuus.hive.udf.UDFLastDay'    USING JAR 'hdfs://bdpha/user/hive/udf.jar';
CREATE FUNCTION $1.todate     AS 'com.annuus.hive.udf.ToDate6UDF'    USING JAR 'hdfs://bdpha/user/hive/udf.jar';
CREATE FUNCTION $1.roundmonth AS 'com.annuus.hive.udf.UDFRoundMonth' USING JAR 'hdfs://bdpha/user/hive/udf.jar';
CREATE FUNCTION $1.coal       AS 'com.annuus.hive.udf.UDFCoal'       USING JAR 'hdfs://bdpha/user/hive/udf.jar';
CREATE FUNCTION $1.month_add  AS 'com.annuus.hive.udf.UDFMonthAdd'   USING JAR 'hdfs://bdpha/user/hive/udf.jar';
"
hive -e "${HQL}"
