#!/usr/bin/env bash
export RUNNER=$HIVE_HOME/bin/hive

THIS="$0"
THIS_DIR=`dirname "$THIS"`

SCRIPT_HOME=`cd "$THIS_DIR/.." ; pwd`

echo "THIS_DIR:$THIS_DIR"
echo "SCRIPT_HOME:$SCRIPT_HOME"

echo "Create all table"

if [ -d "${SCRIPT_HOME}/tables" ];then
   tbs=$(ls ${SCRIPT_HOME}/tables)

   for table in $tbs;do
     echo "start create table => ${SCRIPT_HOME}/tables/${table} " `date`

     $RUNNER -f  ${SCRIPT_HOME}/tables/${table}

     echo "end " `date`
   done
fi

echo "Create all view"

if [ -d "${SCRIPT_HOME}/views" ];then
   vws=$(ls ${SCRIPT_HOME}/views)

   for view in $vws;do
     echo "start create view => ${SCRIPT_HOME}/views/${view} " `date`

     $RUNNER -f  ${SCRIPT_HOME}/views/${view}

     echo "end " `date`
   done
fi

echo "Create views finished!!!"

