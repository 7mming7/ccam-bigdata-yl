--SPARK
create function sysdate as 'com.annuus.hive.udf.UDFSysDate';
create function firstday as 'com.annuus.hive.udf.UDFFirstDay';
create function lastday as 'com.annuus.hive.udf.UDFLastDay';
create function todate as 'com.annuus.hive.udf.ToDate6UDF';
create function roundmonth as 'com.annuus.hive.udf.UDFRoundMonth';
create function coal as 'com.annuus.hive.udf.UDFCoal';
create function month_add as 'com.annuus.hive.udf.UDFMonthAdd';
create function date_diff as 'com.annuus.hive.udf.UDFDatediff';


--HIVE
create function zhgl.sysdate as 'com.annuus.hive.udf.UDFSysDate' using jar 'hdfs://zhgl-d:9000/user/hive/udf.jar';
create function zhgl.firstday as 'com.annuus.hive.udf.UDFFirstDay' using jar 'hdfs://zhgl-d:9000/user/hive/udf.jar';
create function zhgl.lastday as 'com.annuus.hive.udf.UDFLastDay' using jar 'hdfs://zhgl-d:9000/user/hive/udf.jar';
create function zhgl.todate as 'com.annuus.hive.udf.ToDate6UDF' using jar 'hdfs://zhgl-d:9000/user/hive/udf.jar';
create function zhgl.roundmonth as 'com.annuus.hive.udf.UDFRoundMonth' using jar 'hdfs://zhgl-d:9000/user/hive/udf.jar';
create function zhgl.coal as 'com.annuus.hive.udf.UDFCoal' using jar 'hdfs://zhgl-d:9000/user/hive/udf.jar';
create function zhgl.month_add as 'com.annuus.hive.udf.UDFMonthAdd' using jar 'hdfs://zhgl-d:9000/user/hive/udf.jar';

create function zhglinc.sysdate as 'com.annuus.hive.udf.UDFSysDate' using jar 'hdfs://zhgl-d:9000/user/hive/udf.jar';
create function zhglinc.firstday as 'com.annuus.hive.udf.UDFFirstDay' using jar 'hdfs://zhgl-d:9000/user/hive/udf.jar';
create function zhglinc.lastday as 'com.annuus.hive.udf.UDFLastDay' using jar 'hdfs://zhgl-d:9000/user/hive/udf.jar';
create function zhglinc.todate as 'com.annuus.hive.udf.ToDate6UDF' using jar 'hdfs://zhgl-d:9000/user/hive/udf.jar';
create function zhglinc.roundmonth as 'com.annuus.hive.udf.UDFRoundMonth' using jar 'hdfs://zhgl-d:9000/user/hive/udf.jar';
create function zhglinc.coal as 'com.annuus.hive.udf.UDFCoal' using jar 'hdfs://zhgl-d:9000/user/hive/udf.jar';
create function zhglinc.month_add as 'com.annuus.hive.udf.UDFMonthAdd' using jar 'hdfs://zhgl-d:9000/user/hive/udf.jar';
create function zhglinc.date_diff as 'com.annuus.hive.udf.UDFDatediff' using jar 'hdfs://zhgl-d:9000/user/hive/udf.jar';


drop function zhgltest.sysdate;
drop function zhgltest.firstday;
drop function zhgltest.lastday;
drop function zhgltest.todate;
drop function zhgltest.roundmonth;
drop function zhgltest.coal;
drop function zhgltest.month_add;