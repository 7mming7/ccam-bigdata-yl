#!/usr/bin/env bash

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

# 测试用'增量数据存放目录'                      [可以修改] #
INCREASE_DATA_DIR="/data1/20160530_increase/1418_20151213"

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
############################################################

# 获取 sys_id  -  从脚本执行时传入(4位数字) #
sys_id=$1
if [ -z $sys_id ]; then
  echo "Error!!!"
  echo "Usage: etl_sche.sh <sys_id>"
  exit 1
fi

# 指定任务类型(ETL_INC  -  对应增量任务) #
job_type="ETL_INC"

# 获取当前脚本名 #
cur_script="$0"

# 获取当前开始执行脚本时'UNIX时间戳' #
begin_secs=$(date '+%s')

if [ $DEBUG -eq 1 ]; then
  echo "#"
  echo "# 开始ETL调度!  #"
  echo "# 开始时间: $(date '+%Y-%m-%d %T')  #"
  echo "#"  
  echo "# sys_id: ${sys_id}  #"
  echo "#"  
else
  echo "${cur_script} running ..."
  echo "sys_id: ${sys_id}"  
fi


comment() {
# 获取ETL日期(暂定使用 systempara 表中的 curdate ) #
#   如果 systempara 表中有数据,用且仅用第一条
#   如果 systempara 表中无数据,用当前系统日期
######################################################
# etl_date=`hive -e "SELECT curdate FROM zhgl0620.systempara LIMIT 1"` > /dev/null 2>&1
# 获取成功,但是会往控制台打印INFO
# etl_date=`hive -e "SELECT curdate FROM zhgl0620.systempara LIMIT 1" > temp 2>&1`
# etl_date=`hive -e "SELECT curdate FROM zhgl0620.systempara LIMIT 1" > /dev/null 2>&1`
# 获取失败,虽然不再向控制台打印INFO
# 方案二: 使用awk, 利用"OK"进行分割 - TODO
hive -e "SELECT curdate FROM ${DB_NAME}.systempara LIMIT 1" > temp 2>&1
if [ $? -eq 0 ]; then 
  # Note: 仅在服务器上结果数据"可能"保存在 temp 文件的第 13 行! 
  # P.S. 同时,查询结果可能为空,即 temp 文件中可能只有INFO信息!
  etl_date=`sed -n '13p' temp`
  # 确保 1)$ETL_DATE是一个数字 2)$ETL_DATE满足一定格式(即大于一个给定的6位数值) #  # TODO - 与银联确认最小日期 #
  if [ $etl_date -lt 19700101 ]; then
    # 不满足上述条件,即查询为空,则使用当前系统日期
    etl_date=$(date '+%Y%m%d')
	echo "# 测试用 - ETL_DATE获取到的字符类型与整数类型比较成功(需整理逻辑) ...  #"
  fi
  rm -f temp > /dev/null 2>&1
else
  if [ $DEBUG -eq 1 ]; then
    echo "# Error 1-1 !!!  ${DB_NAME}库下systempara表不存在,请检查并重新执行脚本!  #"
  else
    echo "Error 1-1 !!!  'systempara' table not exist in '${DB_NAME}' !"
    echo "Please check log and execute again!"
  fi
  exit 1
fi
}
etl_date=20151213
if [ $DEBUG -eq 1 ]; then
  echo "# ETL日期: ${etl_date}  #"
  echo "#"
else
  echo "ETL date: ${etl_date}"
fi


# 准备'ETL日志文件存放目录' #
################################################## Start
logdir=$TOP_DIR/logs/$etl_date
if [ $DEBUG -eq 1 ]; then
  echo "# 正在准备'ETL日志文件存放目录' ......  #"
  echo "# ${logdir}  #"
fi  
if [ -d $logdir ]; then
  rm -rf $logdir/* > /dev/null 2>&1
else
  mkdir -p $logdir
fi
if [ $DEBUG -eq 1 ]; then
  echo "# 准备完成!  #"
  echo "#"
fi


logfile=$logdir/etl_sche.log.$etl_date
errlogfile=$logdir/etl_sche.errlog.$etl_date


# 检测Hive表和视图是否已初始化 #
############################################## Start
if [ $DEBUG -eq 1 ]; then
  echo "# 正在检测数据库 ${DB_NAME} ......  #"
fi
hive -e "USE ${DB_NAME}" > $logfile 2>&1
if [ $? -ne 0 ]; then
  if [ $DEBUG -eq 1 ]; then
    echo "# Error 2-1 !!! '${DB_NAME}'不存在,请先执行etl_init.sh脚本!  #"
    echo "# Error 2-1 !!! '${DB_NAME}'不存在,请先执行etl_init.sh脚本!  #" > $errlogfile 2>&1
  else
    echo "Error 2-1 !!!  '${DB_NAME}' not exist!"
    echo "Error 2-1 !!!  '${DB_NAME}' not exist!" > $errlogfile 2>&1
	echo "Please run 'etl_init.sh' first!"
  fi
  exit 1
fi
if [ $DEBUG -eq 1 ]; then
  echo "# 当前数据库: ${DB_NAME}  #"
  echo "#"
else
  echo "Current database: ${DB_NAME}"
fi

if [ $DEBUG -eq 1 ]; then
  echo "# 正在检测Hive表和视图是否已初始化 ......  #"
fi
# 检测逻辑: 
#   字典序查询最后一张表和最后一张视图,
#   若两者都存在(无论有无数据)则查询语句退出状态码为0,否则退出状态码非0. #
#   说明: 目前没有想到更好的检测逻辑,先凑合用! # 
hive -e "SELECT * FROM ${DB_NAME}.temp_stmtfeeincreasemonth_l6m LIMIT 1;
         SELECT * FROM ${DB_NAME}.v_s24_stmx LIMIT 1" >> $logfile 2>&1
if [ $? -ne 0 ]; then
  if [ $DEBUG -eq 1 ]; then
    echo "# Error 2-2 !!!  Hive表和视图不存在/完整,请先执行etl_init.sh脚本!  #"
    echo "# Error 2-2 !!!  Hive表和视图不存在/完整,请先执行etl_init.sh脚本!  #" >> $errlogfile 2>&1
  else
    echo "Error 2-2 !!!  Tables and views not exist [all]!"
    echo "Error 2-2 !!!  Tables and views not exist [all]!" >> $errlogfile 2>&1
    echo "Please run 'etl_init.sh' first!"
  fi
  exit 1
fi
if [ $DEBUG -eq 1 ]; then
  echo "# 检测到Hive表和视图已初始化!  #"
  echo "#"
else
  echo "Tables and views have initialized."
fi


# 检查MySQL中yinlian库下JOB_SCHE表中是否有JOB_NM的JOB_STATUS异常('2') #
checkException() {
  counter=`mysql -uroot -Dyinlian -e "SELECT count(*) FROM JOB_SCHE\
            WHERE JOB_TYPE='${job_type}' AND JOB_STATUS='2' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' "`
  # 示例: echo ${counter}  结果: "count(*) 0" or "count(*) 1"
  eNumbers=`echo ${counter}|awk -F " " '{print $2}'`

  echo $eNumbers
}


isTarZFile=0
for file in `ls $INCREASE_DATA_DIR`; do

  # 判断当前文件是否是 xxx.tar.Z 增量数据压缩文件 #
  temp=`echo ${file}|awk -F . '{print $3}'`
  if [[ $temp = "Z" ]]; then
    isTarZFile=$[ $isTarZFile + 1 ]

    # 准备 xxx.tar.Z 文件解压后的存放目录 #
	############################################## Start
    datadir=$TOP_DIR/data/$etl_date
	if [ $DEBUG -eq 1 ]; then
      echo "# 正在准备'解压数据存放目录' ......  #"
	  echo "# ${datadir}  #"
	fi
    if [ -d $datadir ]; then
      rm -rf $datadir/* >> $logfile 2>&1
    else
      mkdir -p $datadir
    fi
	if [ $DEBUG -eq 1 ]; then
      echo "# 准备完成!  #"
      echo "#"
	fi

	# 解压 xxx.tar.Z 文件 #
	######################################################## Start
	if [ $DEBUG -eq 1 ]; then
      echo "# 正在解压 ${INCREASE_DATA_DIR}/${file} 文件  #"
	  echo "# 到 ${datadir} 目录 ......  #"
	fi
    tar -xzf $INCREASE_DATA_DIR/$file -C $datadir
    if [ $? -ne 0 ]; then
	  if [ $DEBUG -eq 1 ]; then
        echo "# Error 3-1 !!!  解压异常,请检查'压缩文件路径'和'解压路径'并重新执行脚本!  #"
        echo "# Error 3-1 !!!  解压异常,请检查'压缩文件路径'和'解压路径'并重新执行脚本!  #" >> $errlogfile 2>&1
      else
        echo "Error 3-1 !!!  Uncompress '${file}' failed!"
        echo "Error 3-1 !!!  Uncompress '${file}' failed!" >> $errlogfile 2>&1
		echo "Please check log and execute again!"
	  fi
      exit 1
    fi
	if [ $DEBUG -eq 1 ]; then
      echo "# 解压完成!  #"
      echo "#"
	fi

    # 遍历'解压数据存放目录',统计S24_XXX.txt文件分布(总数,非空数) #
    ############################################################### Start
    totalFiles=0
    nonEmptyFiles=0
    
    # 注意: 有路径切换!!!
    cd $datadir
    for txtfile in `ls $datadir`; do			      
      temp=`echo ${txtfile}|awk -F . '{print $2}'`		  
      if [[ $temp = "txt" ]]; then
        totalFiles=$[ $totalFiles + 1 ]
        if [ -s $txtfile ]; then
          nonEmptyFiles=$[ $nonEmptyFiles + 1 ]
        fi
      fi
    done
    #/////////////////////////////
    if [ $totalFiles -eq 0 ]; then
      if [ $DEBUG -eq 1 ]; then
        echo "# Error 3-2 !!!  ${datadir}目录下没有S24_XXX.txt文件,请检查并重新执行脚本!  #"
        echo "# Error 3-2 !!!  ${datadir}目录下没有S24_XXX.txt文件,请检查并重新执行脚本!  #" >> $errlogfile 2>&1
      else
        echo "Error 3-2 !!!  No 'txt' file in ${datadir} !"
        echo "Error 3-2 !!!  No 'txt' file in ${datadir} !" >> $errlogfile 2>&1
	    echo "Please check log and execute again!"	  
      fi
      exit 1
    fi
    #/////////////////////////////
    if [ $DEBUG -eq 1 ]; then
      echo "# ${datadir} 目录下 S24_XXX.txt 文件分布:  #"
      echo "# 文件总数: ${totalFiles} 个  #"
      echo "# 非空总数: ${nonEmptyFiles} 个  #"
      echo "#"
    fi

    # 测试期间,脚本可能一天多次跑;为了清晰,删除ETL当天的冗余信息 #
    ############################################################## Start
    if [ $DEBUG -eq 1 ]; then
      echo "# 正在准备MySQL上yinlian库下的JOB_SCHE表 ......  #"
    fi
    mysql -uroot -Dyinlian -e "DELETE FROM JOB_SCHE 
                                WHERE JOB_TYPE='${job_type}' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' " >> $logfile 2>&1
    if [ $? -ne 0 ]; then
	  if [ $DEBUG -eq 1 ]; then
	    echo "# Error 3-3 !!!  访问MySQL出错,请检查访问权限并重新执行脚本!  #"
	    echo "# Error 3-3 !!!  访问MySQL出错,请检查访问权限并重新执行脚本!  #" >> $errlogfile 2>&1
	  else
        echo "Error 3-3 !!!  Access MySQL failed!"
        echo "Error 3-3 !!!  Access MySQL failed!" >> $errlogfile 2>&1
		echo "Please check log and execute again!"         	  
	  fi
      exit 1
    fi
    if [ $DEBUG -eq 1 ]; then
      echo "# 准备完成!  #"
      echo "#"
    fi

	# 开始 hadoop jar 转码所有非空S24_XXX.txt增量文件 #
	################################################### Start
	if [ $DEBUG -eq 1 ]; then
      echo "# 开始转码 ${nonEmptyFiles} 个非空S24_XXX.txt增量文件!  #"
      echo "# 转码开始时间: $(date '+%Y-%m-%d %T')  #"
	  echo "#"
	  hjarBeginSecs=$(date '+%s')
	else
      echo "Begin transcoding ..."	  
	fi
	
    index=0
    for f in `ls ${datadir}`; do			      
      temp=`echo ${f}|awk -F . '{print $2}'`
      if [[ $temp = "txt" ]]; then
	    # 注意: 有路径切换!!!
		cd $datadir
        if [ -s $f ]; then
          index=$[ $index + 1 ]
		  if [ $DEBUG -eq 1 ]; then
            echo "# *********************************************************************** #"
            echo "# ${index}/${nonEmptyFiles}文件名：${f} => 正在提交转码任务,请等待 ${SCHE_SLEEP_SECS}s ......  #"
          fi
          tabnameTemp=`echo ${f}|awk -F 20 '{print $1}'`
          tabname=`echo $tabnameTemp|tr 'A-Z' 'a-z'`      # tabname  - 表名
		  dbfile=$datadir/$f                              # dbfile   - 数据文件名(绝对路径)
		  jarfile=$TOP_DIR/lib/etl_parse.jar              # jarfile  - jar包(绝对路径)
		  WAREHOUSE_DIR="/user/hive/warehouse"
          dbInDir=$WAREHOUSE_DIR/$DB_NAME/etl_in/$tabname # dbInDir  - hadoop jar输入目录(表名文件夹,绝对路径)
	      dbOutDir=$WAREHOUSE_DIR/$DB_NAME/$tabname       # dbOutDir - hadoop jar输出目录(表名文件夹,绝对路径)		  
          tablog=$logdir/$tabname.log.$etl_date           # tablog   - 当前S24_XXX.txt对应日志文件(绝对路径)

          # 注意: 有路径切换!!!
          cd $TOP_DIR/etl
      
          # TODO - 如何捕获 nohup ./etl_trans.sh 的异常 #
          nohup ./etl_trans.sh $etl_date $tabname $dbfile $jarfile $dbInDir $dbOutDir $job_type $sys_id $errlogfile >> $tablog 2>&1 &

          sleep $SCHE_SLEEP_SECS  # 为防止任务后台积压,需间隔一定时间再提交任务
		fi
      fi
      
      # 每次提交完任务后
      # 查询是否有转码异常,若有则提示执行重跑脚本etl_redo.sh后退出 #
      eNumbers=`checkException`
      if [ $eNumbers -gt 0 ]; then
        if [ $DEBUG -eq 1 ]; then
          echo "# Error 3-4!!! 转码异常,请执行etl_redo.sh脚本重跑ETL!  #"
          echo "# Error 3-4!!! 转码异常,请执行etl_redo.sh脚本重跑ETL!  #" >> $errlogfile 2>&1
          echo "#"
        else
          echo "Error 3-4 !!!  hadoop jar failed!"
          echo "Error 3-4 !!!  hadoop jar failed!" >> $errlogfile 2>&1
		  echo "Please check log and execute 'etl_redo.sh'!"
        fi
        exit 1
      fi
      
    done
	#################################################### The end.

    # 处理完有且仅有的一个xxx.tar.Z文件 -> 跳出循环 #
    break
  fi
done


waitForCompletion() {
  counter=`mysql -uroot -Dyinlian -e "SELECT count(*) FROM JOB_SCHE\
                                       WHERE JOB_TYPE='${job_type}' AND JOB_STATUS='0' AND JOB_SCHE_DATE='${etl_date}' AND SYS_ID='${sys_id}' "`
  # 示例: echo ${counter}  结果: count(*) 64
  unfinishedJobs=`echo ${counter}|awk -F " " '{print $2}'`

  if [ $unfinishedJobs -eq "0" ]; then
    if [ $DEBUG -eq 1 ]; then
      echo "# 所有非空S24_XXX.txt增量数据文件转码完成!  #"
      echo "# 转码完成时间: $(date '+%Y-%m-%d %T')  #"
	  hjarEndSecs=$(date '+%s')
	  hjarSecs=$[ $hjarEndSecs - $hjarBeginSecs ]
      hjarMinutes=$[ $hjarSecs / 60 ]
      hjarMinutes=$[ $hjarMinutes + 1 ]
	  echo "#"
      echo "# 本次转码总计耗时: ${hjarMinutes} 分钟!  #"	
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


# 等待所有S24_XXX.txt增量数据文件转码完成 #
unfinishedJobs="1"
while [[ $unfinishedJobs != "0" ]]; do
  waitForCompletion
done


# 调用proc_call.sh脚本执行14个存储过程 #
######################################## Start
if [ $DEBUG -eq 1 ]; then
  echo "# 正在执行14个prc存储过程,请等待 ......  #"
  echo "# 开始时间: $(date '+%Y-%m-%d %T')  #"
  importBeginSecs=$(date '+%s')
else
  echo "Running 14 prc, please wait ..."
fi

# 说明: 经测试,只要shell成功被调用,$?值即为0,即使shell里的语句执行失败!!!
#       结论是,这种方式只能判断调用proc_call.sh是否成功,不能反映proc_call.sh中14个prc是否都执行成功!
./proc_call.sh $DEBUG $TOP_DIR/prc $DB_NAME $etl_date $sys_id > $logdir/proc_call.log.$etl_date 2>&1

echo "Leo: 程序执行到这里!"

if [ $? -ne 0 ]; then
  if [ $DEBUG -eq 1 ]; then
    echo "# Error 4-1: 调用proc_call.sh失败!!! 请查看日志并重新执行脚本!  #"
    echo "# Error 4-1: 调用proc_call.sh失败!!! 请查看日志并重新执行脚本!  #" >> $errlogfile 2>&1
  else
    echo "Error 4-1: call 'proc_call.sh' failed!!!"
    echo "Error 4-1: call 'proc_call.sh' failed!!!" >> $errlogfile 2>&1
    echo "Please check log and execute again!"  
  fi
  exit 1
fi

echo "Step1==================="

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

echo "Step2+++++++++++++++++++++++"


# 获取当前结束执行脚本时'UNIX时间戳' #
endSecs=$(date '+%s')

# 计算脚本总执行时间(单位: 分钟) #
totalSecs=$[ $endSecs - $begin_secs ]
Minutes=$[ $totalSecs / 60 ]
Minutes=$[ $Minutes + 1 ]

# 生产环境中删除过程目录 # 
#rm –rf $logdir/../* > /dev/null 2>&1
#rm –rf $datadir/../* > /dev/null 2>&1


if [ $DEBUG -eq 1 ]; then
  echo "# ETL调度完成!  #"
  echo "# 完成时间: $(date '+%Y-%m-%d %T')  #"
  echo "#"
  echo "# 本次调度总耗时: ${Minutes} 分钟!  #"
  echo "#"
else
  echo "Done."
  echo "Total use ${Minutes} Minutes."
fi
