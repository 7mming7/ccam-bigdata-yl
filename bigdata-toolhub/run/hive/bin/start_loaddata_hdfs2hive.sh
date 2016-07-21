#!/usr/bin/env bash
export RUNNER=$HIVE_HOME/bin/hive

YEAR_MONTH=`date +%Y%m`
YESTERDAY=`date -d '-1 day' +%Y%m%d`
TODAY=`date +%Y%m%d`

SPECIFIC_DAY=$TODAY

DATABASE_NAME="hive"

THIS="$0"
THIS_DIR=`dirname "$THIS"`

SCRIPT_HOME=`cd "$THIS_DIR/.." ; pwd`

if [ ! -d "${SCRIPT_HOME}/logs" ];then
   mkdir -p ${SCRIPT_HOME}/logs
fi

echo "$RUNNER -f  ${SCRIPT_HOME}/load/load_table_data.sql > $SCRIPT_HOME/logs/load_table_data.log";

$RUNNER -f  ${SCRIPT_HOME}/load/load_table_data.sql > $SCRIPT_HOME/logs/load_table_data.log 2>&1

echo "load end:" `date`

echo "finished!!!"
