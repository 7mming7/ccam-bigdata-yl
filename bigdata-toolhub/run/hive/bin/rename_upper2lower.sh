#!/usr/bin/env bash

THIS="$0"
THIS_DIR=`dirname "$THIS"`

SCRIPT_HOME=`cd "$THIS_DIR/.." ; pwd`

echo "THIS_DIR:$THIS_DIR"
echo "SCRIPT_HOME:$SCRIPT_HOME"

echo "Create all table"

if [ -d "${SCRIPT_HOME}/tables" ];then
   tbs=$(ls ${SCRIPT_HOME}/tables)
   cd ${SCRIPT_HOME}/tables
   for table in $tbs;do
     echo "start upper to lower => ${SCRIPT_HOME}/tables/${table} " `date`

     str=`echo $table|tr  'A-Z' 'a-z'`
     mv $table $str

     echo "end " `date`
   done
fi