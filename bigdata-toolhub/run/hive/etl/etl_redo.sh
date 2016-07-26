#!/usr/bin/env bash

#etl_redo.sh
#文件名含义: 断点重跑脚本
#断点重跑对象: PartI)增量数据转码异常断点重跑  PartII)14个PRC执行异常重跑
#断点重跑逻辑: 读MySQL中yinlian库下的JOB_SCHE表,根据ETL日期(JOB_SCHE_DATE),任务状态(JOB_STATUS)和任务类型(JOB_TYPE)
#              1)"ETL_INC"异常,从JOB_SCHE表中提取转码异常的表名,然后从异常处继续执行转码,最后调用"proc_call.sh"
#              2)"PROC"异常,从JOB_SCHE表中提取异常PRC名,与JOB_METADATA表中所有PRC对比,找到的PRC即为断点重跑的第一个PRC

#############################################################
# 获取etl_date - 从脚本执行时传入(8位数字,格式为"yyyymmdd") #
etl_date=$1
if [ -z $etl_date ]; then
  echo "Error!!!"
  echo "Usage: etl_redo.sh <etl_date> <sys_id>"
  exit 1
fi

# 获取sys_id - 从脚本执行时传入(4位数字) #
sys_id=$2
if [ -z $sys_id ]; then
  echo "Error!!!"
  echo "Usage: etl_redo.sh <etl_date> <sys_id>"
  exit 1
fi
#############################################################


############################################################
# 调试信息打印开关( 1,打印  0,不打印 )          [可以修改] #
DEBUG=1

# 测试用'库名'                                  [可以修改] #
DB_NAME="zhgl0726"

# 测试用'提交转码任务'间隔时间(单位: 秒)        [可以修改] #
# 若在本地测试,          SCHE_SLEEP_SECS >= 60s
# 若在服务器测试, 20s >= SCHE_SLEEP_SECS >= 10s
# 注意: 间隔时间小于10s,容易报内存不够导致任务出错!!!
SCHE_SLEEP_SECS=16

# 测试用'查询任务状态'间隔时间(单位: 秒)        [可以修改] #
QUERY_SLEEP_SECS=10

# 目录结构之'顶层目录' # [可以修改,但要保证下面的目录结构] #
TOP_DIR="/home/hadoop/ccam-bigdata/bigdata-toolhub/run/hive"
# 目录结构:
#          $TOP_DIR
#                |-- bin/
#                |-- data/
#                |-- etl/
#                |-- initdata/
#                |-- fun/
#                |-- lib/
#                |-- logs/
#                |-- prc/
#                |-- tables/
#                |-- views/

# 测试用'增量数据解压后存放目录'                [可以修改] #
DATA_DIR="${TOP_DIR}/data/${etl_date}"

# 测试用'增量数据操作日志存放目录'              [可以修改] #
LOG_DIR="${TOP_DIR}/logs/${etl_date}"
############################################################


# 获取当前脚本名 #
cur_script="$0"

# 获取脚本执行开始时'UNIX时间戳' #
beginSecs=$(date '+%s')


if [ $DEBUG -eq 1 ]; then
  echo "# 开始执行${cur_script} !  #"
  echo "# 开始时间: $(date '+%Y-%m-%d %T')  #"
  echo "#"
else
  echo "${cur_script} running ..."
fi


job_type="ETL_INC"


#检查MySQL中yinlian库下JOB_SCHE表中是否有JOB_NM的JOB_STATUS异常('2')#
checkException() {
  counter=`mysql -uroot -Dyinlian -e "SELECT count(*) FROM JOB_SCHE\
            WHERE JOB_TYPE='${job_type}' AND JOB_STATUS='2' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' "`
  eNumbers=`echo ${counter}|awk -F " " '{print $2}'`

  echo $eNumbers
}

#等待所有非空S24_XXX.txt转码完成#
waitForCompletion() {
  counter=`mysql -uroot -Dyinlian -e "SELECT count(*) FROM JOB_SCHE\
            WHERE JOB_TYPE='${job_type}' AND JOB_STATUS='0' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' "`
  unfinishedJobs=`echo ${counter}|awk -F " " '{print $2}'`

  if [ $unfinishedJobs -eq "0" ]; then
    if [ $DEBUG -eq 1 ]; then
      echo "# 所有非空S24_XXX.txt增量数据文件转码完成!  #"
      echo "# 转码完成时间: $(date '+%Y-%m-%d %T')  #"
	  echo "#" 
	else
	  echo "Done."
	fi
  else
	if [ $DEBUG -eq 1 ]; then
      echo "# 后台 ${unfinishedJobs} 个S24_XXX.txt正在转码,请等待 ${QUERY_SLEEP_SECS}s ......  #"
	  echo "# 请查看8088端口 'RUNNING'子项 是否有任务正在执行! 异常请查看日志文件!  #"
    else
      echo "${unfinishedJobs} S24_XXX.txt transcoding in background."
	  echo "Please wait ${QUERY_SLEEP_SECS} seconds ..."
	fi
    sleep $QUERY_SLEEP_SECS
  fi
}

#将ETL日期格式由"yyyymmdd"转换为"yyyy-mm-dd 00:00:00"#
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
        echo "# Error 7-1 !!!  ETL日期格式转换失败,请检查并重新执行脚本!  #"
      else
        echo "Error 7-1 !!!  changeFormat() failed !"
        echo "Please check log and execute again!"
      fi
      exit 1
	fi
	date_temp="${date_temp} 00:00:00"
    rm -f temp > /dev/null 2>&1
  else
    if [ $DEBUG -eq 1 ]; then
      echo "# Error 7-2 !!!  ${db_name}库 dual表/todate()函数 访问异常,请检查并重新执行脚本!  #"
    else
      echo "Error 7-2 !!!  changeFormat() failed !"
      echo "Please check log and execute again!"
    fi
    exit 1
  fi

  echo $date_temp
}

# 执行增量数据断点重跑 #
etl_inc_redo() {
  if [ $DEBUG -eq 1 ]; then
    echo "# 开始断点重跑!  #"
    echo "# 开始时间: $(date '+%Y-%m-%d %T')  #"
    echo "#"
  else
    echo "Breakpoint redo beginning..."
  fi

  for f in `ls ${DATA_DIR}`; do
    temp=`echo ${f}|awk -F . '{print $2}'`
    if [[ $temp = "txt" ]]; then
      cd $DATA_DIR
      if [ -s $f ]; then
        #确认重跑起点#
        if [ $f = $1 -o $f = $2 ]; then
          if [ $DEBUG -eq 1 ]; then
            echo "# **************************************************************** #"
            echo "# 文件名：${f} => 正在提交转码任务,请等待 ${SCHE_SLEEP_SECS}s ......  #"
          else
            echo "'${f}' is going to running..."
          fi
          tabnameTemp=`echo ${f}|awk -F 20 '{print $1}'`
          tabname=`echo $tabnameTemp|tr 'A-Z' 'a-z'`      # tabname  - 表名
          dbfile=$DATA_DIR/$f                             # dbfile   - 数据文件名(绝对路径)
          jarfile=$TOP_DIR/lib/etl_parse.jar              # jarfile  - jar包(绝对路径)
          WAREHOUSE_DIR="/user/hive/warehouse"
          dbInDir=$WAREHOUSE_DIR/$DB_NAME/etl_in/$tabname # dbInDir  - hadoop jar输入目录(表名文件夹,绝对路径)
          dbOutDir=$WAREHOUSE_DIR/$DB_NAME/$tabname       # dbOutDir - hadoop jar输出目录(表名文件夹,绝对路径)		  
          tablog=$LOG_DIR/$tabname.log.$etl_date          # tablog   - 当前S24_XXX.txt对应日志文件(绝对路径)
          
          #注意:有路径切换!!!#
          cd $TOP_DIR/etl
          
          #TODO - 如何捕获 nohup ./etl_trans.sh 的异常#
          nohup ./etl_trans.sh $etl_date $tabname $dbfile $jarfile $dbInDir $dbOutDir $3 $4 $5 >> $tablog 2>&1 &
          
          #为防止任务后台积压,需间隔一定时间再提交任务#
          sleep $SCHE_SLEEP_SECS
          
          #每次提交完任务后,查询是否有转码异常,若有则提示执行重跑脚本etl_redo.sh后退出#
          eNumbers=`checkException`
          if [ $eNumbers -gt 0 ]; then
            if [ $DEBUG -eq 1 ]; then
              echo "# Error 7-3!!! 转码异常,请执行etl_redo.sh脚本重跑ETL!  #"
              echo "#"
            else
              echo "Error 7-3 !!!  hadoop jar failed!"
		      echo "Please check log and execute 'etl_redo.sh'!"
            fi
            exit 1
          fi
          #////////////////////////
        fi
      fi
    fi
  done
  
  #等待所有S24_XXX.txt增量数据文件转码完成#
  unfinishedJobs="1"
  while [[ $unfinishedJobs != "0" ]]; do
    waitForCompletion
  done
  
  #调用proc_call.sh脚本执行14个存储过程#
  ######################################## Start
  if [ $DEBUG -eq 1 ]; then
    echo "# 正在执行14个prc存储过程,请等待 ......  #"
    echo "# 开始时间: $(date '+%Y-%m-%d %T')  #"
    importBeginSecs=$(date '+%s')
  else
    echo "Running 14 prc, please wait ..."
  fi

  ./proc_call.sh $DEBUG $TOP_DIR/prc $DB_NAME $etl_date $sys_id > $logdir/proc_call.log.$etl_date 2>&1
  
  if [ $? -ne 0 ]; then
    if [ $DEBUG -eq 1 ]; then
      echo "# Error 7-4: 调用proc_call.sh失败!!! 请查看日志并重新执行脚本!  #"
      echo "# Error 7-4: 调用proc_call.sh失败!!! 请查看日志并重新执行脚本!  #" >> $errlogfile 2>&1
    else
      echo "Error 7-4: call 'proc_call.sh' failed!!!"
      echo "Error 7-4: call 'proc_call.sh' failed!!!" >> $errlogfile 2>&1
      echo "Please check log and execute again!"  
    fi
    exit 1
  fi
  
  if [ $DEBUG -eq 1 ]; then
    importEndSecs=$(date '+%s')
    echo "# 执行完成!  #"
    echo "# 完成时间: $(date '+%Y-%m-%d %T')  #"
    importSecs=$[ $importEndSecs - $importBeginSecs ]
    importMinutes=$[ $importSecs / 60 ]
    importMinutes=$[ $importMinutes + 1 ]
    echo "#"
    echo "# 本次执行14个存储过程总计耗时: ${importMinutes} 分钟!  #"
    echo "#"
  else
    echo "Done."
  fi
  
  if [ $DEBUG -eq 1 ]; then
    echo "# ETL断点重跑完成!  #"
    echo "# 完成时间: $(date '+%Y-%m-%d %T')  #"
    echo "#"
  else
    echo "Done."
  fi
}


#统计ETL是否有异常需要重跑#
counter=`mysql -uroot -Dyinlian -e "SELECT count(*) FROM JOB_SCHE\
WHERE JOB_TYPE='${job_type}' AND JOB_STATUS='2' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' "`
# 示例: echo ${counter}  结果: count(*) 64
# exceptional jobs #
ejobs=`echo ${counter}|awk -F " " '{print $2}'`
if [ $ejobs -ne "0" ]; then
  if [ $DEBUG -eq 1 ]; then
    echo "# 检测到ETL增量导入异常, 开始重跑!  #"
    echo "# 开始时间: $(date '+%Y-%m-%d %T')  #"
    echo "#"
  else
    echo "ETL redo beginning ..."
	echo ""
  fi
  
  #调用脚本etl_sche.sh开始重跑#
  # ./etl_sche.sh $sys_id

  #查MySQL中yinlian库下JOB_SCHE表中JOB_STATUS='2'的#
  s24_table=`mysql -uroot -Dyinlian -e "SELECT JOB_NM FROM JOB_SCHE\
              WHERE JOB_TYPE='${job_type}' AND JOB_STATUS='2' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' "`
  #将小写表名转换为大写#
  S24_TABLE=`echo ${s24_table}|tr 'A-Z' 'a-z'`
  echo "# s24_table: ${s24_table}  #"  
  echo "# S24_TABLE: ${S24_TABLE}  #"
  #拼接S24_XXX.txt文件名(支持小写,即"s24_xxx.txt")  注意: 暂时不支持大写的TXT后缀!#
  S24_TXT=${S24_TABLE}${etl_date}.txt
  s24_txt=${s24_table}${etl_date}.txt
  echo "# S24_TXT: ${S24_TXT}  #"  
  echo "# s24_txt: ${s24_txt}  #"
  
  #Step1 先删除异常状态的条目#
  res=`mysql -uroot -Dyinlian -e "DELETE FROM JOB_SCHE\
        WHERE JOB_TYPE='${job_type}' AND JOB_STATUS='2' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' "`
  
  #Step2 再调用函数etl_inc_redo($1,$2,$3,$4,$5)完成重跑#
  etl_inc_redo $S24_TXT $s24_txt $job_type $sys_id $LOG_DIR/etl_sche.errlog.$etl_date
fi
  


job_type="PROC"


#获取转换格式后的ETL日期#
etl_date_new=`changeFormat`
if [ $DEBUG -eq 1 ]; then
  echo "# etl_date_new: ${etl_date_new}  #"	  
  echo "#"
fi

# 统计PRC是否有异常需要重跑( $prc_type ) #
counter=`mysql -uroot -Dyinlian -e "SELECT count(*) FROM JOB_SCHE\
WHERE JOB_TYPE='${job_type}' AND JOB_STATUS='2' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' "`
ejobs=`echo ${counter}|awk -F " " '{print $2}'`
if [ $ejobs -ne "0" ]; then
  if [ $DEBUG -eq 1 ]; then
    echo "# 检测到PRC执行异常, 开始断点重跑!  #"
    echo "# 开始时间: $(date '+%Y-%m-%d %T')  #"
    echo "#"
  else
    echo "PRC redo beginning ..."
	echo ""
  fi
  
  # 获取异常的PRC #
  flag=0
  #查询到的异常PRC有且只有一个,同时结果全大写字符串#
  FAILED_PRC=`mysql -uroot -Dyinlian -e "SELECT JOB_NM from JOB_SCHE\
  WHERE JOB_TYPE='${prc_type}' AND JOB_STATUS='2' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' LIMIT 1 "`

  cd $TOP_DIR/prc
  for f in `mysql -uroot -Dyinlian -e "SELECT JOB_NM from JOB_METADATA WHERE JOB_TYPE='${job_type}' AND SYS_ID='${sys_id}' ORDER BY JOB_ID "`; do
    if [ $f != $FAILED_PRC -a $flag = 0 ]; then
	  continue
	else
	  $flag=1
	fi
    
	#检查PRC文件是否存在且非空(支持小写"XXX.prc"后缀和大写"XXX.PRC"后缀)#
    #注意:PRC名称部分必须为大写,否则会找不到文件#
	############################################################### Start-check
    if [ -f $f.prc -a -s $f.prc ] || [ -f $f.PRC -a -s $f.PRC ]; then    
	  #大写".PRC"后缀转换为小写".prc"后缀,以便统一处理#
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
          echo "# Error 7-5 !!!  MySQL表INSERT失败,请检查INSERT语句并重新执行脚本!  #"
        else
          echo "Error 7-5 !!!  INSERT MySQL table failed!"
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
            echo "# Error 7-6 !!!  MySQL表UPDATE失败,请检查UPDATE语句并重新执行脚本!  #"
          else
            echo "Error 7-6 !!!  UPDATE MySQL table failed!"
            echo "Please check log and execute again!"
          fi
          exit 1
        fi
      else
	    # 存储过程执行失败 #
        edate=`date '+%Y%m%d'`       # job end date #
        etime=`date '+%Y-%m-%d %T'`  # job end time #	 	  
        if [ $DEBUG -eq 1 ]; then
          echo "# Error 7-7 !!!  ${f}.prc 执行失败,请检查日志和 ${f}.prc 文件并重新执行脚本!  #"
          echo "#"
        else
          echo "Error 7-7 !!!  ${f}.prc execute failed!"
        fi
		
        mysql -uroot -Dyinlian -e "UPDATE JOB_SCHE 
		SET JOB_STATUS='2',JOB_END_DT='${edate}',JOB_END_TM='${etime}' 
        WHERE JOB_NM='${f}' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' "
        if [ $? -ne 0 ]; then
          if [ $DEBUG -eq 1 ]; then
            echo "# Error 7-8 !!!  MySQL表UPDATE失败,请检查UPDATE语句并重新执行脚本!  #"
          else
            echo "Error 7-8 !!!  UPDATE MySQL table failed!"
            echo "Please check log and execute again!"
          fi
          exit 1
        fi
		
		# 退出(报错后) #
        exit 1
      fi
	  ################ End-prc
    fi
  done
fi


# 获取脚本当前执行结束时'UNIX时间戳' #
end_secs=$(date '+%s')
secs=$[ $end_secs - $beginSecs ]
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
