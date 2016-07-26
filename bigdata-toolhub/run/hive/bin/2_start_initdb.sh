#!/usr/bin/env bash

THIS="$0"
THIS_DIR=`dirname "$THIS"`
SCRIPT_HOME=`cd "$THIS_DIR/.." ; pwd`

echo ""
echo "# 进入脚本 2_start_initdb.sh 开始执行 ......  #"
echo "# 开始时间: $(date '+%Y-%m-%d %T')  #"
echo ""
echo "# tables/views所在目录为: ${SCRIPT_HOME}  #"
echo ""


echo "# ############################################# #"
echo "# 开始建表(字典序执行tables目录下所有建表sql)!  #"
echo ""

if [ -d "${SCRIPT_HOME}/tables" ]; then
  tbs=$(ls ${SCRIPT_HOME}/tables)
  for table in $tbs; do
    echo "# 正在创建表: ${table} ......  #"
    hive -f $SCRIPT_HOME/tables/$table
  done
fi

echo ""
echo "# 建表完成!                                     #"
echo "# ############################################# #"
echo ""


echo "# ################################################ #"
echo "# 开始建视图(字典序执行views目录下所有建视图sql)!  #"
echo ""

if [ -d "${SCRIPT_HOME}/views" ]; then
  vws=$(ls ${SCRIPT_HOME}/views)
  for view in $vws; do
    echo "# 正在创建视图: ${view} ......  #"
    hive -f $SCRIPT_HOME/views/$view
  done
fi

echo ""
echo "# 建视图完成!                                      #"
echo "# ################################################ #"
echo ""

echo "# 脚本 2_start_initdb.sh 执行结束!  #"
echo "# 结束时间: $(date '+%Y-%m-%d %T')  #"
echo ""
