#!/usr/bin/env bash


# TODO - 跟杜工确认:
#        1. 原始增量数据保留90天  -  ok
#        2. 解压后 data 和 logs 目录下的文件用完就删 or  保留90天 ???


# 测试用'增量数据目录'之上级目录 [可以修改] #
INCREASE_TOP_DIR="/data1/20160530_increase"

# 目录结构之'顶层目录' #
TOP_DIR="/home/hadoop/ccam-bigdata/bigdata-toolhub/run/hive"

# 删除周期(单位: 天) #
DEL_CYCLE=90

# 获取当前日期 #
cur_date=$(date '+%Y%m%d')


for dir in `ls ${INCREASE_TOP_DIR}`; do

  echo "# dir: ${dir}  #"
  
  if [ -d $dir ]; then
    dir_date=`echo ${dir}|awk -F _ '{print $2}'`
	
	echo "# dir_date: ${dir_date}  #"
	
	interval=$[ $cur_date - $dir_date ]
	if [ $interval -ge $DEL_CYCLE ]; then
	  rm -r $INCREASE_TOP_DIR/dir > $TOP_DIR/logs/etl_clean.log.$date 2>&1
	fi
  fi
done
