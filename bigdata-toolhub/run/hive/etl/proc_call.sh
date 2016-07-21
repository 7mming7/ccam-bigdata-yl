#!/usr/bin/env bash

sys_id=$1
db_name=$2
etl_date=$3

echo "# sys_id: ${sys_id}  #"
echo "# db_name: ${db_name}  #"
echo "# etl_date: ${etl_date}  #"
echo "#"

# todo - 加mysql返回值判断
# todo - 加mysql删当天冗余操作


prcpath="../prc"
etl_date=`date +'%Y%m%d'`
job_date=`date +'%Y%m%d'`
job_time=`date +'%Y%m%d %T'`

for proc in `mysql -uroot -Dyinlian -e "select JOB_NM from JOB_METADATA where job_type = 'PROC' "`; do
 echo "$proc"
 if [ -f $prcpath/"$proc".prc ]; then
     mysql -uroot yinlian -e "INSERT INTO 
                          JOB_SCHE(JOB_NM,JOB_STATUS,JOB_TYPE,JOB_SCHE_DATE,JOB_BEGIN_DT,JOB_BEGIN_TM, SYS_ID)
						   VALUES ('${proc}', '0', 'PROC', '${etl_date}', '${job_date}', '${job_time}', '${sys_id}') "
     echo "# INSERT MySQL表JOB_SCHE完成!  #"
     hplsql -f $prcpath/"$proc".prc -d DB_NAME=$db_name -d ETL_DATE=$etl_date

    if [ $? -eq 0 ]; then
      edate=`date '+%Y%m%d'`       # end date #
      etime=`date '+%Y-%m-%d %T'`  # end time #
      echo "${proc}执行成功"
      mysql -uroot -Dyinlian -e "UPDATE JOB_SCHE SET JOB_STATUS='1',JOB_END_DT='${edate}',JOB_END_TM='${etime}' 
						    WHERE JOB_NM='${proc}' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' "
      echo "# UPDATE MySQL表 JOB_SCHE 完成!  #"
    else
      edate=`date '+%Y%m%d'`       # end date #
      etime=`date '+%Y-%m-%d %T'`  # end time #
      mysql -uroot -Dyinlian -e "UPDATE JOB_SCHE SET JOB_STATUS='2',JOB_END_DT='${edate}',JOB_END_TM='${etime}' 
						  WHERE JOB_NM='${proc}' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' "
      echo "${proc}执行失败"
      exit 1
    fi
else
  echo "文件${proc}.prc不存在"

fi
 
done

