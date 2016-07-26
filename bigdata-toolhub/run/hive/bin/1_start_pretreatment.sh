annuus-udf.sh
etl_clean.sh
etl_init.sh
etl_redo.sh
etl_sche.sh
etl_trans.sh
mysql_JOB_METADATA.sql
mysql_JOB_SCHE.sql
PRC_IMPORT_DATA_ROLLBACK.prc
PRC_IMPORT_DATA.prc
PRC_IMPORT_LOG_DATA.prc
proc_call.sh#!/usr/bin/env bash

THIS="$0"
THIS_DIR=`dirname "$THIS"`
SCRIPT_HOME=`cd "$THIS_DIR/.."; pwd`

# 接收参数 - 数据库名(DB_NAME)
DB_NAME=$1

echo ""
echo "# 进入脚本 1_start_pretreatment.sh 开始执行 ......  #"
echo "# 开始时间: $(date '+%Y-%m-%d %T')  #"
echo ""
echo "# DATABASE_NAME更新为: ${DB_NAME}  #"
echo ""
echo "# tables/views所在目录为: ${SCRIPT_HOME}  #"
echo ""

echo "# #################################################### #"
echo "# 开始更新tables目录下所有建表sql中的'DB_NAME'!  #"
echo ""

if [ -d "${SCRIPT_HOME}/tables" ]; then
  tbs=$(ls ${SCRIPT_HOME}/tables)
  for table in $tbs; do
    echo "# 正在更新表: ${table} ......  #"

    # 2016-06-22, append, leo #
    sed -i s/zhgl/$DB_NAME/g $SCRIPT_HOME/tables/$table
    sed -i s/HDFSDIRNAME/$DB_NAME/g $SCRIPT_HOME/tables/$table
	
	# 2016-07-07, append, leo #
	#sed s/HDFSDIRNAME/$DB_NAME/g $SCRIPT_HOME/initdata/tables/$table > $SCRIPT_HOME/tables/$table

  done
fi


echo ""
echo "# ##################################################### #"
echo "# 开始更新views目录下所有建视图sql中的'DB_NAME'!  #"
echo ""

if [ -d "${SCRIPT_HOME}/views" ]; then
  vws=$(ls ${SCRIPT_HOME}/views)
  for view in $vws; do
    echo "# 正在更新视图: ${view} ......  #"
	
    # 2016-06-22, append, leo #
    sed -i s/zhgl/$DB_NAME/g $SCRIPT_HOME/views/$view
	
	# 2016-07-07, append, leo #
	#sed s/HDFSDIRNAME/$DB_NAME/g $SCRIPT_HOME/initdata/views/$view > $SCRIPT_HOME/views/$view	
	
  done
fi

echo "# 脚本 1_start_pretreatment.sh 执行结束!  #"
echo "# 结束时间: $(date '+%Y-%m-%d %T')  #"
echo ""
