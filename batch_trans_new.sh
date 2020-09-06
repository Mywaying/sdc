#!/usr/bin/env bash
source /etc/profile
SQL=$1
FDATE=$2
if [ -z $FDATE ];then
    FDATE=`date -d '-1 day' '+%Y-%m-%d'`
fi
TMPLOG=/var/log/`echo ${SQL} |awk -F '/' '{print $3}'`.log
echo "">$TMPLOG
HOST=$HOST
BATCHSTART=`date +'%F %T'`
MAILS=wangwei@ddjf.com.cn
MOBILE="15994732290"
echo "batch tran starting...hold on[$BATCHSTART]...." | tee -a $TMPLOG
IFS=","
array=($HOST)
rn=${#array[@]}
n=$[RANDOM%${rn}+0]
NHOST=`echo ${array[$n]}`
LOG=/var/log/batch_order.log_`date +%F`
NHOST=DATA-DW-DEV-181
echo "LOGFILE=/tmp/log/${filename}.log" |tee -a $TMPLOG
workdir=`pwd`
filename=$(basename "$SQL")
touch /tmp/${filename}.pid
mkdir -pv /tmp/log/
if [ ! -f ${LOG} ];then
    touch ${LOG}
fi
LOGFILE=/tmp/log/${filename}.log
STARTTIME=`date +%F`
NUM=1
WHITELIST="orders_batch_init_1.sql dwd_rsc_credit.sql"
c=`echo ${WHITELIST} |grep "${filename}"`
if [ -n "$c" ];then #1h
    NUM=-3600
fi
FSQL="${SQL} at: \[`date +%F`"
echo "$FSQL" |tee -a $TMPLOG
F=`grep "$FSQL" $LOG |wc -l`
rm -rvf /tmp/log/${filename}.log
echo "JOB_OUTPUT_PROP_FILE:${JOB_OUTPUT_PROP_FILE}" |tee -a $TMPLOG
#RETRY=`cat ${JOB_OUTPUT_PROP_FILE} | grep retries | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
RETRY=3
if [ ! -n "$RETRY" ];then
    RETRY=3
fi
echo "RETRY:${RETRY}" |tee -a $TMPLOG
while [ -f /tmp/${filename}.pid ];
do
    if [ ${NUM} -eq 1 -o ${NUM} -eq -3600 ];then
        echo "impala-shell -i $NHOST --var=logdate=${FDATE} -f $SQL 2>&1 " |tee -a $TMPLOG
        impala-shell -i $NHOST --var=logdate=${FDATE} -f $SQL 2>&1 |tee -a $TMPLOG
    fi
    a=`grep -E -C 1 "Error connecting|CAUSED BY: Exception|Error: Error|Could not execute command|ERROR - ERROR:" $TMPLOG`
    a=`echo $a`
    b=`echo $b`
    if [ -n "$a" -o ${NUM} -gt ${HOlDTIME} ];then
        cat ${LOGFILE} |tee -a $TMPLOG
        msg=`echo ${SQL} |awk -F '/' '{print $3}'`
        b=$(( ${F} % $RETRY ))
        if [ "${F}" -eq "0" ];then
            b=1
        fi
        if [ ${b} -eq 0 ];then
#        if [ -n "$a" -o ${b} -eq 0 ];then
            echo "BATCH ${msg} at ${BATCHSTART}" |mail -a ${LOGFILE} -s "[FAILED]${msg} at ${BATCHSTART}" $MAILS
            content="[BD-DATA]FAILED (`hostname`) UPDATE PACKET JOB ${SQL} at `date +'%F %T'`"
            if [ -n "$MOBILE" ];then
                /usr/bin/python /usr/bin/run/sendsms2.py "${MOBILE}" "${content}"
            fi
        fi
        echo "batch tran failed[$SQL][${BATCHSTART}--`date +'%F %T'`]...." | tee -a $TMPLOG
        echo "kill ${SQL} at: [`date +'%F %T']`" |tee -a ${LOG}
        exit 1
    elif [ -n "$b" ];then
        echo "batch tran finished[${BATCHSTART}--`date +'%F %T'`]...." | tee -a $TMPLOG
        rm /tmp/${filename}.pid
        exit 0
    fi
    NUM=`expr $NUM + 1`
    echo "$SQL doing.... [`date +'%F %T'`][$NUM][$F]"
    sleep 1
done
rm /tmp/${filename}.pid