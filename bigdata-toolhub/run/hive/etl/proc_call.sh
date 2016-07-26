#!/usr/bin/env bash

DEBUG=$1    # 参数1: DEBUG    - 调试信息打印开关( 1,打印  0,不打印 )
prc_dir=$2  # 参数2: prc_dir  - 14个prc文件存放目录
db_name=$3  # 参数3: db_name  - 数据库名
etl_date=$4 # 参数4: etl_date - ETL日期
sys_id=$5   # 参数5: sys_id   - 银行代号(4位数字)

if [ $DEBUG -eq 1 ]; then
  echo "#"
  echo "# prc_dir : ${prc_dir}  #"
  echo "# db_name : ${db_name}  #"
  echo "# etl_date: ${etl_date}  #"
  echo "# sys_id  : ${sys_id}  #"
  echo "#"
fi

# 指定任务类型(PROC - 对应存储过程) #
job_type="PROC"

# 获取当前脚本名 #
cur_script="$0"

# 获取脚本当前执行开始时'UNIX时间戳' #
begin_secs=$(date '+%s')
  
if [ $DEBUG -eq 1 ]; then
  echo "# 开始PRC调度!  #"
  echo "# 开始时间: $(date '+%Y-%m-%d %T')  #"
  echo "#"
else
  echo "${cur_script} running ..."
fi


# 脚本可能一天被调用多次,删除MySQL下JOB_SCHE表中ETL当天的冗余信息 #
###################################################################
if [ $DEBUG -eq 1 ]; then
  echo "# 正在删除MySQL下yinlian库JOB_SCHE表中的冗余信息 ......  #"
fi

mysql -uroot -Dyinlian -e "DELETE FROM JOB_SCHE 
WHERE JOB_TYPE='${job_type}' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' "
if [ $? -ne 0 ]; then
  if [ $DEBUG -eq 1 ]; then
    echo "# Error 6-1 !!!  ${cur_script}访问MySQL失败,请检查访问权限并重新执行脚本!  #"
  else
    echo "Error 6-1 !!!  Access MySQL failed!"
    echo "Please check log and execute again!"
  fi
  exit 1
fi

if [ $DEBUG -eq 1 ]; then
  echo "# 删除完成!  #"
  echo "#"
fi


# 将ETL日期格式由"yyyymmdd"转换为"yyyy-mm-dd 00:00:00" #
changeFormat() {
  hive -e "SELECT ${db_name}.todate(${etl_date}, 'yyyy-mm-dd') FROM ${db_name}.dual" > temp 2>&1
  if [ $? -eq 0 ]; then 
    # 注意: 仅在服务器上结果数据"可能"保存在 temp 文件的第 16 行!!!
    # P.S. 因查询结果可能为空,即 temp 文件中可能只有INFO信息,
	#      经测试,此时默认只有13行文本,即执行下面的语句 date_temp 中应该为空.
	# 说明: 此处需进一步完善异常处理逻辑 - TODO #
    date_temp=`sed -n '16p' temp`
	
	# 检查 date_temp 的长度是否为0,为0则表明获取失败 #
	if [ -z $date_temp ]; then
      if [ $DEBUG -eq 1 ]; then
        echo "# Error 6-2 !!!  ETL日期格式转换失败,请检查并重新执行脚本!  #"
      else
        echo "Error 6-2 !!!  changeFormat() failed !"
        echo "Please check log and execute again!"
      fi
      exit 1
	fi
	date_temp="${date_temp} 00:00:00"
    rm -f temp > /dev/null 2>&1
  else
    if [ $DEBUG -eq 1 ]; then
      echo "# Error 6-3 !!!  ${db_name}库 dual表/todate()函数 访问异常,请检查并重新执行脚本!  #"
    else
      echo "Error 6-3 !!!  changeFormat() failed !"
      echo "Please check log and execute again!"
    fi
    exit 1
  fi

  echo $date_temp
}


# 获取转换格式后的ETL日期 #
etl_date_new=`changeFormat`
if [ $DEBUG -eq 1 ]; then
  echo "# etl_date_new: ${etl_date_new}  #"	  
  echo "#"
fi

# 切换到prc目录 #
cd $prc_dir

# 检查 prc_dir 是否存在并是一个目录 #
if [ -d $prc_dir ]; then  
  for f in `mysql -uroot -Dyinlian -e "select JOB_NM from JOB_METADATA where JOB_TYPE='${job_type}' AND SYS_ID='${sys_id}' "`; do
	
	# 检查prc文件是否存在且非空(支持小写".prc"后缀和大写".PRC"后缀) #
	############################################################### Start-check
    if [ -f $f.prc -a -s $f.prc ] || [ -f $f.PRC -a -s $f.PRC ]; then
	
	  # 大写".PRC"后缀转换为小写".prc"后缀,统一处理 #
	  if [ -f $f.PRC -a -s $f.PRC ]; then
	    mv $f.PRC $f.prc
	  fi
	  
      if [ $DEBUG -eq 1 ]; then
	    echo ""
        echo "# 开始执行: ${f}.prc ......  #"
        echo "# 开始时间: $(date '+%Y-%m-%d %T')  #"
        echo "#"
		# 获取当前PRC执行开始时'UNIX时间戳' #
		prc_bsecs=$(date '+%s')
      else
	    echo ""
        echo "${f}.prc running ..."
      fi

      bdate=`date '+%Y-%m-%d'`     # job begin date #
      btime=`date '+%Y-%m-%d %T'`  # job begin time #
	
      mysql -uroot -Dyinlian -e "INSERT INTO 
      JOB_SCHE(JOB_NM,JOB_STATUS,JOB_TYPE,JOB_SCHE_DATE,JOB_BEGIN_DT,JOB_BEGIN_TM, SYS_ID) 
      VALUES ('${f}', '0', '${job_type}', '${etl_date}', '${bdate}', '${btime}', '${sys_id}') "  
      if [ $? -ne 0 ]; then
        if [ $DEBUG -eq 1 ]; then
          echo "# Error 6-4 !!!  MySQL表INSERT失败,请检查INSERT语句并重新执行脚本!  #"
        else
          echo "Error 6-4 !!!  INSERT MySQL table failed!"
          echo "Please check log and execute again!"
        fi
        exit 1
      fi
	  
	  # 调用存储过程 #
	  ################ Start-prc
	  if [ "${f}" = "PRC_IMPORT_DATA" -o "${f}" = "PRC_IMPORT_LOG_DATA" ]; then
	    hplsql -f $f.prc -d DATA_BASE=$db_name -d ETL_DATE=$etl_date
      else
	    # "-d ETL_DATE=$etl_date_new"这种传参方式只能传入"yyyy-mm-dd"而不是要求的"yyyy-mm-dd 00:00:00"
	    hplsql -f $f.prc -d DATA_BASE=$db_name -d ETL_DATE="${etl_date_new}"
	  fi
      if [ $? -eq 0 ]; then
	    # 14个存储过程执行成功 #
        edate=`date '+%Y%m%d'`       # job end date #
        etime=`date '+%Y-%m-%d %T'`  # job end time #	 	  
        if [ $DEBUG -eq 1 ]; then
		  prc_esecs=$(date '+%s')
		  prc_tsecs=$[ $prc_esecs - $prc_bsecs ]
		  prc_mins=$[ $prc_tsecs / 60 ]
		  prc_mins=$[ $prc_mins + 1 ]		
          echo "# ${f}.prc 执行成功!  #"
		  echo "# ${f}.prc 执行耗时: ${prc_mins} 分钟!  #"
          echo "#"
        else
          echo "${f}.prc execute successfully!"
        fi
		
        mysql -uroot -Dyinlian -e "UPDATE JOB_SCHE 
		SET JOB_STATUS='1', JOB_END_DT='${edate}', JOB_END_TM='${etime}' 
        WHERE JOB_NM='${f}' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' "
        if [ $? -ne 0 ]; then
          if [ $DEBUG -eq 1 ]; then
            echo "# Error 6-5 !!!  MySQL表UPDATE失败,请检查UPDATE语句并重新执行脚本!  #"
          else
            echo "Error 6-5 !!!  UPDATE MySQL table failed!"
            echo "Please check log and execute again!"
          fi
          exit 1
        fi
      else
	    # 存储过程执行失败 #
        edate=`date '+%Y%m%d'`       # job end date #
        etime=`date '+%Y-%m-%d %T'`  # job end time #	 	  
        if [ $DEBUG -eq 1 ]; then
          echo "# Error 6-6 !!!  ${f}.prc 执行失败,请检查日志和 ${f}.prc 文件并重新执行脚本!  #"
          echo "#"
        else
          echo "Error 6-6 !!!  ${f}.prc execute failed!"
        fi
		
        mysql -uroot -Dyinlian -e "UPDATE JOB_SCHE 
		SET JOB_STATUS='2',JOB_END_DT='${edate}',JOB_END_TM='${etime}' 
        WHERE JOB_NM='${f}' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' "
        if [ $? -ne 0 ]; then
          if [ $DEBUG -eq 1 ]; then
            echo "# Error 6-7 !!!  MySQL表UPDATE失败,请检查UPDATE语句并重新执行脚本!  #"
          else
            echo "Error 6-7 !!!  UPDATE MySQL table failed!"
            echo "Please check log and execute again!"
          fi
          exit 1
        fi
		
		# 退出(报错后) #
        exit 1
      fi
	  ################ End-prc
    fi
	############################# End-check
  done
else
  if [ $DEBUG -eq 1 ]; then
    echo "# Error 6-0 !!!  '${prc_dir}'不是一个目录,请检查并重新执行脚本!  #"
  else
    echo "Error 6-0 !!!  '${prc_dir}' not a dir!"
    echo "Please check log and execute again!"
  fi
  exit 1
fi


# 获取脚本当前执行结束时'UNIX时间戳' #
end_secs=$(date '+%s')
secs=$[ $end_secs - $begin_secs ]
mins=$[ $secs / 60 ]
mins=$[ $mins + 1 ]

if [ $DEBUG -eq 1 ]; then
  echo "# PRC调度完成!  #"
  echo "# 完成时间: $(date '+%Y-%m-%d %T')  #"
  echo "# 本次PRC调度总耗时: ${mins} 分钟!  #"
  echo "#"
else
  echo "${cur_script} successful completion!"
  echo "In summary: ${mins} minutes."
fi
