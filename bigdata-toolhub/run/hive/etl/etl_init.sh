#!/usr/bin/env bash

############################################################
# 调试信息打印开关( 1,打印  0,不打印 )          [可以修改] #
DEBUG=1

# 测试用'库名'                                  [可以修改] #
DB_NAME="zhglinc"

# 测试用'提交转码任务'间隔时间(单位: 秒)        [可以修改] #
# 若在本地测试,          SCHE_SLEEP_SECS >= 60s
# 若在服务器测试, 10s >= SCHE_SLEEP_SECS >= 8s
SCHE_SLEEP_SECS=10

# 测试用'查询任务状态'间隔时间(单位: 秒)        [可以修改] #
QUERY_SLEEP_SECS=10

# 测试用'全量数据目录'                      [后期可能修改] #
FULL_DATA_DIR="/data1/8479_20150816_full"

# 目录结构之'顶层目录'                      [后期可能修改] #
TOP_DIR="/home/hadoop/ccam-bigdata/bigdata-toolhub/run/hive"
# 目录结构:
#          $TOP_DIR
#                |-- bin/
#                |-- data/
#                |-- etl/
#                |-- initdata/
#                |-- fun/
#                |-- lib/
#                |-- load/
#                |-- logs/
#                |-- prc/
#                |-- tables/
#                |-- views/
############################################################

# 获取sys_id - 从脚本执行时传入(4位数字) #
sys_id=$1
if [ -z $sys_id ]; then
  echo "Error!!!"
  echo "Usage: etl_init.sh <sys_id>"
  exit 1
fi

# 指定任务类型(ETL_ALL - 对应全量任务) #
job_type="ETL_ALL"

# 获取全量数据ETL日期(格式: yyyymmdd) #
if [ -d $FULL_DATA_DIR ]; then
  for txt in `ls $FULL_DATA_DIR`; do
    temp=`echo ${txt}|awk -F . '{print $2}'`
    if [[ $temp = "txt" ]]; then
      ETL_DATE=`echo ${txt}|awk '{pos=index($0,"."); print substr($0,pos-8,8)}'`
	  if [ -n $ETL_DATE ]; then
	    if [ $DEBUG -eq 1 ]; then
          echo "# ETL日期: ${ETL_DATE}  #"
          echo "#"
		else
		  echo "ETL date: ${ETL_DATE}"
		fi
	    break
	  else
	    # ETL_DATE 为空,报错退出! #
        if [ $DEBUG -eq 1 ]; then
          echo "# Error!!!  #"
	      echo "# FULL_DATA_DIR 变量所赋值不是一个目录,请重新赋值再执行脚本!  #"
          echo "#"
        else
          echo "Error!!!"
          echo "FULL_DATA_DIR is not a dir. Please check and execute again!"
        fi
        exit 1
	  fi
    fi
  done
else
  if [ $DEBUG -eq 1 ]; then
    echo "# Error!!!  #"
	echo "# FULL_DATA_DIR 变量所赋值不是一个目录,请重新赋值再执行脚本!  #"
    echo "#"
  else
    echo "Error!!!"
    echo "FULL_DATA_DIR is not a dir. Please check and execute again!"
  fi
  exit 1
fi

# 获取当前脚本名 #
curScript="$0"

# 获取当前开始执行脚本时'UNIX时间戳' #
beginSecs=$(date '+%s')

if [ $DEBUG -eq 1 ]; then
  echo "# 开始ETL初始化!  #"
  echo "# 开始时间: $(date '+%Y-%m-%d %T')  #"
  echo "#"
  echo "# sys_id: ${sys_id}  #"
  echo "#"
else
  echo "${curScript} running ..."
  echo "sys_id: ${sys_id}"
fi


comment() {


# 准备'ETL日志文件存放目录' #
################################################## Start
logdir=$TOP_DIR/logs/$ETL_DATE
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


logfile=$logdir/etl_init.log.$ETL_DATE
errlogfile=$logdir/etl_init.errlog.$ETL_DATE



# 检测Hive表和视图是否已初始化 #
#################################################### Start
if [ $DEBUG -eq 1 ]; then
  echo "# 正在检测Hive表和视图是否已初始化 ......  #"
fi
# 检测逻辑: 
#   字典序查询最后一张表和最后一张视图,
#   若两者都存在(无论有无数据)则查询语句退出状态码为0,否则退出状态码非0. #
#   说明: 目前没有想到更好的检测逻辑,先凑合用! # 
hive -e "SELECT * FROM ${DB_NAME}.temp_stmtfeeincreasemonth_l6m LIMIT 1;
         SELECT * FROM ${DB_NAME}.v_s24_stmx LIMIT 1" > $logfile 2>&1
if [ $? -eq 0 ]; then
  if [ $DEBUG -eq 1 ]; then
    echo "# 检测到Hive表和视图已初始化!  #"
	echo "#"
  else
    echo "Tables and views have already been initialized."
  fi
else
  #/////////////////////////////////////////////
  # 检测当前指定数据库是否存在 #
  if [ $DEBUG -eq 1 ]; then
    echo "# 正在确认数据库 ${DB_NAME} ......  #"
  fi
  hive -e "USE ${DB_NAME}" >> $logfile 2>&1
  if [ $? -eq 0 ]; then
    if [ $DEBUG -eq 1 ]; then
      echo "# Error: ${DB_NAME} 已存在!!! ETL初始化将重置 ${DB_NAME} 整个库,请手动删库再重新执行脚本!  #"
    else
      echo "Error: ${DB_NAME} exist!!!"
      echo "Please delete first and execute again!"  
    fi
    exit 1
  else
    #/////////////////////////////////////////////////
	# 调用annuus-udf.sh脚本,加载UDF函数[注意: 有传参!]
    if [ $DEBUG -eq 1 ]; then
      echo "#   正在加载UDF函数 ......  #"
    fi
    ./annuus-udf.sh $DB_NAME >> $logfile 2>&1
	if [ $? -ne 0 ]; then
	  if [ $DEBUG -eq 1 ]; then
	    echo "# Error: 加载UDF函数失败! 请检查并重新执行脚本!  #"
      else
        echo "Error: Upload UDF failed!!!"
	    echo "Please check UDF jar file and execute again!"	
	  fi
      exit 1
	fi
    if [ $DEBUG -eq 1 ]; then
      echo "#   加载完成!  #"
    fi	
  fi
  if [ $DEBUG -eq 1 ]; then
    echo "# 确认完成!  #"
    echo "#"
  else
    echo "Current database: ${DB_NAME}"  
  fi

  #///////////////////////////////////////////////////////////
  # 在MySQL中yinlian库下创建 JOB_SCHE 和 JOB_METADATA 两张表 #
  mysql -uroot -Dyinlian </home/hadoop/ccam-bigdata/bigdata-toolhub/run/hive/etl/mysql_JOB_SCHE.sql >> $logfile 2>&1
  if [ $? -ne 0 ]; then
    if [ $DEBUG -eq 1 ]; then
      echo "# Error: 执行 mysql_JOB_SCHE.sql 出错!!! 请查看日志文件并重新执行脚本!  #"
      echo "# Error: 执行 mysql_JOB_SCHE.sql 出错!!! 请查看日志文件并重新执行脚本!  #" >> $errlogfile 2>&1
    else
      echo "Error: mysql_JOB_SCHE.sql execute failed!!!"
      echo "Error: mysql_JOB_SCHE.sql execute failed!!!" >> $errlogfile 2>&1
      echo "Please check log and execute again!"
    fi
    exit 1
  else
    if [ $DEBUG -eq 1 ]; then
      echo "# mysql_JOB_SCHE.sql 执行成功!  #"
    else
      echo "mysql_JOB_SCHE.sql execute success!"
    fi 
  fi
  mysql -uroot -Dyinlian </home/hadoop/ccam-bigdata/bigdata-toolhub/run/hive/etl/mysql_JOB_METADATA.sql >> $logfile 2>&1
  if [ $? -ne 0 ]; then
    if [ $DEBUG -eq 1 ]; then
      echo "# Error: 执行 mysql_JOB_METADATA.sql 出错!!! 请查看日志文件并重新执行脚本!  #"
      echo "# Error: 执行 mysql_JOB_METADATA.sql 出错!!! 请查看日志文件并重新执行脚本!  #" >> $errlogfile 2>&1
    else
      echo "Error: mysql_JOB_METADATA.sql execute failed!!!"
      echo "Error: mysql_JOB_METADATA.sql execute failed!!!" >> $errlogfile 2>&1
      echo "Please check log and execute again!"
    fi
    exit 1
  else
    if [ $DEBUG -eq 1 ]; then
      echo "# mysql_JOB_METADATA.sql 执行成功!  #"
	  echo "#"
    else
      echo "'mysql_JOB_METADATA.sql' execute success!"
    fi	
  fi
  

  #////////////////////////////////////////////////////
  # Step-1/2. 调用'表和视图创建sql'完成表和视图的创建 #
  if [ $DEBUG -eq 1 ]; then
    echo "# 开始初始化Hive表和视图 [总计219+1张表,60张视图] !  #"
    echo "# 开始时间: $(date '+%Y-%m-%d %T')  #"
	echo "# Step-1/2. 正在创建Hive表和视图,大约52分钟完成,请等待 ......  #"
	curBeginSecs=$(date '+%s')
	echo "#"
  else
    echo "Tables and views are initializing, please wait 51 minutes ..."  
  fi
  
  . $TOP_DIR/bin/1_start_pretreatment.sh $DB_NAME >> $logfile 2>&1
  . $TOP_DIR/bin/2_start_initdb.sh >> $logfile 2>&1
 
  if [ $DEBUG -eq 1 ]; then
    curEndSecs=$(date '+%s')
	curTotalSecs=$[ $curEndSecs - $curBeginSecs ]
	curMinutes=$[ $curTotalSecs / 60 ]
	curMinutes=$[ $curMinutes + 1 ]
    echo "# Step-1/2. 创建完成!  #"
	echo "#"
    echo "# 本次创建总计耗时: ${curMinutes} 分钟!  #"	
	echo "#"	
  fi
  #////////////////////////////////////////////////////
  # Step-2/2. 数据初始化 - 为多张特定表加载初始化数据 #
  if [ $DEBUG -eq 1 ]; then
    echo "# Step-2/2. 正在为多张特定表加载初始化数据 ......  #"
  fi
  d=$TOP_DIR/initdata
  if [ -f $d/T_FDM_TABLENAME.txt -a -f $d/SYSTEMPARA.txt -a -f $d/CCAM_CODES.txt -a -f $d/TBL_BANKS.txt ]; then
    hive -e "LOAD DATA LOCAL INPATH '${d}/T_FDM_TABLENAME.txt' OVERWRITE INTO TABLE ${DB_NAME}.t_fdm_tablename;
             LOAD DATA LOCAL INPATH '${d}/SYSTEMPARA.txt' OVERWRITE INTO TABLE ${DB_NAME}.systempara;
             LOAD DATA LOCAL INPATH '${d}/CCAM_CODES.txt' OVERWRITE INTO TABLE ${DB_NAME}.ccam_codes;
             LOAD DATA LOCAL INPATH '${d}/TBL_BANKS.txt' OVERWRITE INTO TABLE ${DB_NAME}.tbl_banks;" >> $logfile 2>&1
    if [ $? -ne 0 ]; then
	  if [ $DEBUG -eq 1 ]; then
	    echo "# Error: 加载'表初始化数据'错误!!! 请检查'表初始化数据'是否存在并重新执行脚本!  #"
	    echo "# Error: 加载'表初始化数据'错误!!! 请检查'表初始化数据'是否存在并重新执行脚本!  #" >> $errlogfile 2>&1
      else
        echo "Error: Load table data error!!!"
        echo "Error: Load table data error!!!" >> $errlogfile 2>&1
	    echo "Please check whether data-file (all) exist and execute again!"	  
	  fi
      exit 1
    fi
	#////////////////////////
	hive -e "INSERT INTO TABLE ${DB_NAME}.dual VALUES('X')" >> $logfile 2>&1
    if [ $? -ne 0 ]; then
	  if [ $DEBUG -eq 1 ]; then
	    echo "# Error: 加载'dual表初始化数据'错误!!! 请检查并重新执行脚本!  #"
	    echo "# Error: 加载'dual表初始化数据'错误!!! 请检查并重新执行脚本!  #" >> $errlogfile 2>&1
      else
        echo "Error: Load dual data error!!!"
        echo "Error: Load dual data error!!!" >> $errlogfile 2>&1
	    echo "Please check log and execute again!"	  
	  fi
      exit 1
    fi	
	#////////////////////////
    if [ $DEBUG -eq 1 ]; then
      echo "# Step-2/2. 加载完成!  #"
      echo "#"
    fi
  else
	if [ $DEBUG -eq 1 ]; then
	  echo "# Error: '表初始化数据'不存在/完整!!! 请检查并重新执行脚本!  #"
	  echo "# Error: '表初始化数据'不存在/完整!!! 请检查并重新执行脚本!  #" >> $errlogfile 2>&1
    else
      echo "Error: Table data error!!!"
      echo "Error: Table data error!!!" >> $errlogfile 2>&1
	  echo "Please check whether data-file (all) exist and execute again!"	  
	fi
    exit 1
  fi
fi


# 遍历'全量数据目录',统计S24_XXX.txt文件的分布情况(总数,非空数) #
################################################################# Start
totalFiles=0
nonEmptyFiles=0

# 注意: 有路径切换!!!  
cd $FULL_DATA_DIR

for f in `ls $FULL_DATA_DIR`; do			      
  temp=`echo ${f}|awk -F . '{print $2}'`		  
  if [[ $temp = "txt" ]]; then
    totalFiles=$[ $totalFiles + 1 ]
    if [ -s $f ]; then
      nonEmptyFiles=$[ $nonEmptyFiles + 1 ]
    fi
  fi
done
#/////////////////////////////
if [ $totalFiles -eq 0 ]; then
  if [ $DEBUG -eq 1 ]; then
    echo "# Error: ${FULL_DATA_DIR} 目录下没有'txt'数据文件!!! 请检查并重新执行脚本!  #"
    echo "# Error: ${FULL_DATA_DIR} 目录下没有'txt'数据文件!!! 请检查并重新执行脚本!  #" >> $errlogfile 2>&1
  else
    echo "Error: No 'txt' file in ${FULL_DATA_DIR} !!!"
    echo "Error: No 'txt' file in ${FULL_DATA_DIR} !!!" >> $errlogfile 2>&1
    echo "Please check log and execute again!"  
  fi
  exit 1
fi
#/////////////////////////////
if [ $DEBUG -eq 1 ]; then
  echo "# ${FULL_DATA_DIR} 目录下 S24_XXX.txt 文件分布:  #"
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
                            WHERE JOB_TYPE='${job_type}' AND JOB_SCHE_DATE='${ETL_DATE}' AND SYS_ID='${sys_id}' " >> $logfile 2>&1
if [ $? -ne 0 ]; then
  if [ $DEBUG -eq 1 ]; then
    echo "# Error: 访问MySQL出错!!! 请检查访问权限并重新执行脚本!  #"
    echo "# Error: 访问MySQL出错!!! 请检查访问权限并重新执行脚本!  #" >> $errlogfile 2>&1
  else
    echo "Error: Access MySQL failed!!!"
    echo "Error: Access MySQL failed!!!" >> $errlogfile 2>&1
    echo "Please check log and execute again!"  
  fi
  exit 1
fi
if [ $DEBUG -eq 1 ]; then
  echo "# 准备完成!  #"
  echo "#"
fi


# TODO - 通过统计ETL日期和JOB_SCHE状态跳过重复转码 - 2016.07.06


# 开始hadoop jar转码所有非空S24_XXX.txt全量文件 #
################################################# Start
if [ $DEBUG -eq 1 ]; then
  echo "# 开始转码 ${nonEmptyFiles} 个非空 S24_XXX.txt 全量文件!  #"
  echo "# 转码开始时间: $(date '+%Y-%m-%d %T')  #"
  echo "#"
  hjarBeginSecs=$(date '+%s')
else
  echo "Begin transcoding, please wait ..."
fi

index=0
for f in `ls $FULL_DATA_DIR`; do
  temp=`echo ${f}|awk -F . '{print $2}'`
  if [[ $temp = "txt" ]]; then
    # 注意: 有路径切换!!!  
    cd $FULL_DATA_DIR
    if [ -s $f ]; then
      index=$[ $index + 1 ]
	  if [ $DEBUG -eq 1 ]; then
        echo "# *********************************************************************** #"
        echo "# ${index}/${nonEmptyFiles}文件名：${f} => 正在提交转码任务,请等待 ${SCHE_SLEEP_SECS}s ......  #"
	  fi
      tabnameTemp=`echo ${f}|awk -F 20 '{print $1}'`
      tabname=`echo $tabnameTemp|tr 'A-Z' 'a-z'` # tabname  - 表名
      dbfile=$FULL_DATA_DIR/$f                   # dbfile   - 数据文件名(绝对路径)
      jarfile=$TOP_DIR/lib/etl_parse.jar         # jarfile  - jar包(绝对路径)
	  WAREHOUSE_DIR="/user/hive/warehouse"
      dbInDir=$WAREHOUSE_DIR/$DB_NAME/etl_in/$tabname # dbInDir  - hadoop jar输入目录(表名文件夹,绝对路径)
      dbOutDir=$WAREHOUSE_DIR/$DB_NAME/$tabname       # dbOutDir - hadoop jar输出目录(表名文件夹,绝对路径)   
      tablog=$logdir/$tabname.log.$ETL_DATE           # tablog   - 当前S24_XXX.txt对应日志文件(绝对路径)
	
      # 注意: 有路径切换!!!
      cd $TOP_DIR/etl
      
      # TODO - 如何捕获 nohup ./etl_trans.sh 的异常 #
      #nohup ./etl_trans.sh $ETL_DATE $tabname $dbfile $jarfile $dbInDir $dbOutDir $sys_id $errlogfile >> $tablog 2>&1 &
      ./etl_trans.sh $ETL_DATE $tabname $dbfile $jarfile $dbInDir $dbOutDir $job_type $sys_id $errlogfile >> $tablog 2>&1 &
      
      sleep $SCHE_SLEEP_SECS  # 为防止任务后台积压,需间隔一定时间再提交任务(因为全量数据文件有的很大)
    fi
  fi
done


}


waitForCompletion() {
  counter=`mysql -uroot -Dyinlian -e "SELECT count(*) FROM JOB_SCHE 
                                       WHERE JOB_TYPE='${job_type}' AND JOB_STATUS='0' AND JOB_SCHE_DATE='${ETL_DATE}' AND SYS_ID='${sys_id}' "`
  unfinishedJobs=`echo ${counter}|awk -F " " '{print $2}'`

  if [ $unfinishedJobs -eq "0" ]; then
    if [ $DEBUG -eq 1 ]; then
      echo "# 所有非空S24_XXX.txt全量数据文件转码完成!  #"
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
	else
      echo "${unfinishedJobs} S24_XXX.txt transcoding in background."
	  echo "Please wait ${QUERY_SLEEP_SECS} seconds ..."	
	fi
    sleep $QUERY_SLEEP_SECS
  fi
}


# 等待所有S24_XXX.txt全量数据文件转码完成 #
unfinishedJobs="1"
while [[ $unfinishedJobs != "0" ]]; do
  waitForCompletion
done


# 调用数据融合存储过程 #
if [ $DEBUG -eq 1 ]; then
  echo "# 正在执行全量数据融合存储过程,请等待 ......  #"
  echo "# 融合开始时间: $(date '+%Y-%m-%d %T')  #"
  importBeginSecs=$(date '+%s')
else
  echo "Running import-data-prc, please wait ..."  
fi

dir=$TOP_DIR/etl
hplsql -f $dir/PRC_IMPORT_DATA.prc -d db_name=$DB_NAME -d etl_date=$ETL_DATE >> $logfile 2>&1
if [ $? -ne 0 ]; then
  if [ $DEBUG -eq 1 ]; then
    echo "# Error: 'PRC_IMPORT_DATA.prc'执行失败!!! 请查看日志并重新执行脚本!  #"
    echo "# Error: 'PRC_IMPORT_DATA.prc'执行失败!!! 请查看日志并重新执行脚本!  #" >> $errlogfile 2>&1
  else
    echo "Error: 'PRC_IMPORT_DATA.prc' execute failed!!!"
    echo "Error: 'PRC_IMPORT_DATA.prc' execute failed!!!" >> $errlogfile 2>&1
    echo "Please check log and execute again!"  
  fi
  exit 1
fi

hplsql -f $dir/PRC_IMPORT_LOG_DATA.prc -d db_name=$DB_NAME -d etl_date=$ETL_DATE >> $logfile 2>&1
if [ $? -ne 0 ]; then
  if [ $DEBUG -eq 1 ]; then
    echo "# Error: 'PRC_IMPORT_LOG_DATA.prc'执行失败!!! 请查看日志并重新执行脚本!  #"
    echo "# Error: 'PRC_IMPORT_LOG_DATA.prc'执行失败!!! 请查看日志并重新执行脚本!  #" >> $errlogfile 2>&1
  else
    echo "Error: 'PRC_IMPORT_LOG_DATA.prc' execute failed!!!"
    echo "Error: 'PRC_IMPORT_LOG_DATA.prc' execute failed!!!" >> $errlogfile 2>&1
    echo "Please check log and execute again!"  
  fi
  exit 1
fi

if [ $DEBUG -eq 1 ]; then
  importEndSecs=$(date '+%s')
  echo "# 融合完成!  #"
  echo "# 融合完成时间: $(date '+%Y-%m-%d %T')  #"
  importSecs=$[ $importEndSecs - $importBeginSecs ]
  importMinutes=$[ $importSecs / 60 ]
  importMinutes=$[ $importMinutes + 1 ]
  echo ""
  echo "# 本次融合总计耗时: ${importMinutes} 分钟!  #"
  echo ""
fi


# 获取当前结束执行时UNIX时间戳 #
endSecs=$(date '+%s')

# 计算脚本总执行时间(单位: 分钟) #
totalSecs=$[ $endSecs - $beginSecs ]
Minutes=$[ $totalSecs / 60 ]
Minutes=$[ $Minutes + 1 ]


if [ $DEBUG -eq 1 ]; then
  echo "# ETL初始化完成!  #"
  echo "# 完成时间: $(date '+%Y-%m-%d %T')  #"
  echo "#"
  echo "# 本次初始化总耗时: ${Minutes} 分钟!  #"
  echo "#"
else
  echo "Done."
  echo "Total use ${Minutes} Minutes."
fi
