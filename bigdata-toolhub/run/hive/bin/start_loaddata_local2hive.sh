#!/usr/bin/env bash
source_dir=/home/hadoop/zwgl

echo  "load local datafile to hive;"

if [ -d "${source_dir}" ];then
   dfs=$(ls ${source_dir})

   for datafile in $dfs;do

      #根据文件名称截取日期(month_day=20160505)
      temp_date=${datafile:0-12:8}

      table_name_length=${#datafile}-12

      table_name=${datafile:0:${table_name_length}}

      data_file_path=${source_dir}/${datafile}

      echo ${data_file_path}
      echo ${table_name}
      echo ${temp_date}

      hive -e "
      use zwgl;
      set hive.exec.compress.output=true;
      set mapred.output.compress=true;
      set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
      set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
      load data local inpath '${data_file_path}' into table ${table_name} partition(month_day='${temp_date}');
      "

      echo "end " `date`
   done
fi