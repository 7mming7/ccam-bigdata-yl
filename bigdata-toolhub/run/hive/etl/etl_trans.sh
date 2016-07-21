#!/usr/bin/env bash

etl_date=$1   # 参数1: etl_date -  ETL日期
tabname=$2    # 参数2: tabname  -  当前表名
dbfile=$3     # 参数3: dbfile   -  当前S24_XXX.txt(绝对路径)
jarfile=$4    # 参数4: jarfile  -  jar包(绝对路径)
dbInDir=$5    # 参数5: hadoop jar 输入目录(表名文件夹[-increase],绝对路径)
dbOutDir=$6   # 参数6: hadoop jar 输出目录(表名文件夹,绝对路径)
job_type=$7   # 参数7: job_type   - 任务类型(ETL-All,全量任务  ETL,增量任务)
sys_id=$8     # 参数8: sys_id     - 银行号(4位数字,执行脚本时从命令行传入)
errlogfile=$9 # 参数9: errlogfile - 错误日志存放文件

echo "#"
echo "# ETL日期:             ${etl_date}  #"
echo "# 表名(小写):          ${tabname}  #"
echo "# 待转码数据文件名:    ${dbfile}  #"
echo "# jar包路径:           ${jarfile}  #"
echo "# hadoop jar 输入目录: ${dbInDir}  #"
echo "# hadoop jar 输出目录: ${dbOutDir}  #"
echo "# job_type:            ${job_type}  #"
echo "# sys_id:              ${sys_id}  #"
echo "# errlogfile:          ${errlogfile}  #"
echo "#"

echo "# 开始转码!  #"
echo "# 开始时间: $(date '+%Y-%m-%d %T')  #"
echo "#"

bdate=`date '+%Y%m%d'`       # begin date #
btime=`date '+%Y-%m-%d %T'`  # begin time #

echo "# INSERT [1]表名'${tabname}'
               [2]初始状态'0'
               [3]ETL日期'${etl_date}'
               [4]开始日期'${bdate}'
               [5]开始时间'${btime}' 到MySQL表JOB_SCHE  #"
mysql -uroot -Dyinlian -e "INSERT INTO 
                            JOB_SCHE(JOB_NM,JOB_TYPE,JOB_STATUS,JOB_SCHE_DATE,JOB_BEGIN_DT,JOB_BEGIN_TM, SYS_ID)
						     VALUES ('${tabname}', '${job_type}', '0', '${etl_date}', '${bdate}', '${btime}', '${sys_id}')"
# 在 etl_init.sh 中首次访问MySQL时已做过异常处理,故这里略去 #
echo "# INSERT MySQL表JOB_SCHE完成!  #"
echo "#"

hdfs dfs -ls $dbInDir
if [ $? -ne 0 ]; then
  hdfs dfs -mkdir -p $dbInDir  # 目录不存在则创建 #
else
  hdfs dfs -rm -r $dbInDir/*   # 目录存在则先清空 #
fi

# 再上传新待转码文件 #
hdfs dfs -put $dbfile $dbInDir
  
# hadoop jar OutPut目录 - 因周期执行,故每次都需删除,否则报错 #
hdfs dfs -rm -r $dbOutDir

hadoop jar $jarfile com.sky.batch.call.CommonTask com.sky.batch.call.mapreduce.CovertDecode "null" "null" $dbInDir $dbOutDir

# 任务出错,设置JOB_SCHE表JOB_STATUS字段值为'2';并向控制台打印错误信息然后退出 #
if [ $? -ne 0 ]; then
  edate=`date '+%Y%m%d'`       # end date #
  etime=`date '+%Y-%m-%d %T'`  # end time #
  mysql -uroot -Dyinlian -e "UPDATE JOB_SCHE SET JOB_STATUS='2',JOB_END_DT='${edate}',JOB_END_TM='${etime}' 
						      WHERE JOB_NM='${tabname}' AND JOB_TYPE='${job_type}' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' "
  echo "# Error: ${dbfile} 文件转码失败!!! 请查看日志文件并重新执行脚本!  #" >&1
  echo "# Error: ${dbfile} 文件转码失败!!! 请查看日志文件并重新执行脚本!  #" >> $errlogfile 2>&1
  exit 1
fi

edate=`date '+%Y%m%d'`       # end date #
etime=`date '+%Y-%m-%d %T'`  # end time #

echo "# UPDATE [1]结束状态'1'
               [2]结束日期'${edate}'
			   [3]结束时间'${etime}' 到MySQL表JOB_SCHE  #"
mysql -uroot -Dyinlian -e "UPDATE JOB_SCHE SET JOB_STATUS='1',JOB_END_DT='${edate}',JOB_END_TM='${etime}' 
						    WHERE JOB_NM='${tabname}' AND JOB_TYPE='${job_type}' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' "
echo "# UPDATE MySQL表 JOB_SCHE 完成!  #"
echo "#"

echo "# 完成转码!  #"
echo "# 完成时间: $(date '+%Y-%m-%d %T')  #"
echo "#"
  