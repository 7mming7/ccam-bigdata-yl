#!/usr/bin/env bash

THIS="$0"
THIS_DIR=`dirname "$THIS"`

SCRIPT_HOME=`cd "$THIS_DIR/.." ; pwd`

DatabaseName="zhgl"

echo "THIS_DIR:$THIS_DIR"
echo "SCRIPT_HOME:$SCRIPT_HOME"

echo "Replace all create table head file(use database)."

if [ -d "${SCRIPT_HOME}/tables" ];then
   tbs=$(ls ${SCRIPT_HOME}/tables)

   for table in $tbs;do
     echo "replace table => ${SCRIPT_HOME}/tables/${table} " `date`

     sed -i "s/INCLUDE INCLUDE_HEAD.sql/use ${DatabaseName};/g" ${SCRIPT_HOME}/tables/${table}

     sed -i "s/HDFSDIRNAME/${DatabaseName}/g" ${SCRIPT_HOME}/tables/${table}

     echo "end " `date`
   done
fi

echo "Create all view"

if [ -d "${SCRIPT_HOME}/views" ];then
   vws=$(ls ${SCRIPT_HOME}/views)

   for view in $vws;do
     echo "replace view => ${SCRIPT_HOME}/views/${view} " `date`

     sed -i "s/INCLUDE INCLUDE_HEAD.sql/use ${DatabaseName};/g" ${SCRIPT_HOME}/views/${view}

     echo "end " `date`
   done
fi

echo "Replace all headfile finished!!!"