#!/usr/bin/env bash

source_dir=/home/hadoop/zwgl
target_dir=/user/hive/warehouse/zwgl

echo  "upload local datafile to hdfs;"

if [ -d "${source_dir}" ];then
   dfs=$(ls ${source_dir})

   for datafile in $dfs;do
      echo "start upload datafile=> ${source_dir} " `date`

      #根据文件名称截取日期(month_day=20160505)
      temp_date='month_day='${datafile:0-12:8}

      table_name_length=${#datafile}-12

      table_name=${datafile:0:${table_name_length}}

      echo 'hadoop fs -mkdir ${target_dir}/${table_name}/${temp_date}'
      hadoop fs -mkdir ${target_dir}/${table_name}/${temp_date}

      echo 'hadoop fs -put ${source_dir}/${datafile} ${target_dir}/${table_name}/${temp_date}/'
      hadoop fs -put ${source_dir}/${datafile} ${target_dir}/${table_name}/${temp_date}/

      echo "end " `date`
   done
fi