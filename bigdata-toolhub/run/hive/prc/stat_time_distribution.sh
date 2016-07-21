#!/usr/bin/env bash

echo "START EXEC PRC FILE!"

function appendLogFile() {
    date=$(date +%Y%m%d)

    #存储过程执行打印的日志目录
    log_dir=/hivelog/prc_exec_cost_time$date.log

    cd ..
    if [ ! -d "/hivelog" ];
        then
            echo "不存在hivelog目录，新建hivelog目录."
            mkdir hivelog

        else
            cd hivelog

            if [ ! -e "prc_exec_cost_time$date.log" ];
                then
                    echo "不存在prc_exec_cost_time$date.log文件，新建prc_exec_cost_time$date.log文件."
                    touch prc_exec_cost_time$date.log
            fi

    fi
    cd ../prc
}

#获取存储过程文件执行的时间消耗情况->>打印到日志文件中
# arg1=start, arg2=end, format: %s.%N
function getCostTiming() {
    start=$1
    end=$2
    prc=$3

    date=$(date +%Y%m%d)

    start_s=$(echo $start | cut -d '.' -f 1)
    start_ns=$(echo $start | cut -d '.' -f 2)
    end_s=$(echo $end | cut -d '.' -f 1)
    end_ns=$(echo $end | cut -d '.' -f 2)

    time=$(( ( 10#$end_s - 10#$start_s ) * 1000 + ( 10#$end_ns / 1000000 - 10#$start_ns / 1000000 ) ))

    appendLogFile

    echo "Prc $prc cost $time ms." >> ../hivelog/prc_exec_cost_time$date.log
}

#执行存储过程，打印出时间戳
function executePrc() {
    prcFileName=$1

    start=$(date +%s.%N)
    hplsql -f $prcFileName
    end=$(date +%s.%N)

    getCostTiming $start $end $prcFileName;
}

prcFileList=('PRC_MDM_CARD.prc'
             'PRC_MDM_CARD_INFO.prc'
             'PRC_MDM_COMMON_MP_INFO.prc'
             'PRC_MDM_LIMIT_ADJ_INFO.prc'
             'PRC_MDM_ACCT_X.prc'
             'PRC_MDM_CARD_CUSTR_INFO.prc'
             'PRC_MDM_CUSTR_INFO.prc'
             'PRC_MDM_STMT_X.prc'
             'PRC_MDM_MP_INFO.prc'
             'PRC_MDM_ACCT_INFO.prc'
             'PRC_ACCOUNT.prc'
             'PRC_CUSTUM.prc')

for prc in ${prcFileList[@]};
do
    echo $prc
    executePrc $prc;
done