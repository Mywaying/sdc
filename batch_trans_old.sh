#!/usr/bin/env bash
source /etc/profile
SQL=$1
FDATE=$2
if [ -z $FDATE ];then
    FDATE=`date -d '-1 day' '+%Y-%m-%d'`
fi
if [ ! -n "$JDBCURL" ];then
    echo "please give the env in the profile"
    exit
fi
JDBCURL=$JDBCURL
TMPLOG=/var/log/`echo ${SQL} |awk -F '/' '{print $3}'`.log
echo "">$TMPLOG
HOST=$HOST
BATCHSTART=`date +'%F %T'`
MAILS=wangwei@ddjf.com.cn
MOBILE="15994732290"
echo "batch order starting...hold on[$BATCHSTART]...." | tee -a $TMPLOG
#echo "beeline -u \"$JDBCURL\" -n hdfs -hivevar logdate=$FDATE  -f $SQL" |tee -a $TMPLOG
IFS=","
array=($HOST)
rn=${#array[@]}
n=$[RANDOM%${rn}+0]
NHOST=`echo ${array[$n]}`
impala-shell -i $NHOST --var=logdate=${FDATE} -f $SQL 2>&1 |tee -a $TMPLOG
#a=`grep -E -C 1 "Error connecting|CAUSED BY: Exception|Error: Error|Could not execute command|ERROR - ERROR:" $TMPLOG`
a=`grep -E -C 1 "Error connecting|CAUSED BY: Exception|Error: Error|Could not execute command|ERROR - ERROR:" $TMPLOG`
a=`echo $a`
if [ -n "$a" ];then
    msg=`echo ${SQL} |awk -F '/' '{print $3}'`
    echo "BATCH ${msg} at ${BATCHSTART}" |mail -a $TMPLOG -s "[FAILED]${msg} at ${BATCHSTART}" $MAILS
    content="[BD-DATA]FAILED (`hostname`) UPDATE PACKET JOB ${SQL} ${a} at `date +'%F %T'`"
    if [ -n "$MOBILE" ];then
        /usr/bin/python /usr/bin/run/sendsms2.py "${MOBILE}" "${content}"
    fi
    echo "batch order failed[$SQL][${BATCHSTART}--`date +'%F %T'`]...." | tee -a $TMPLOG
    exit 1
else
    echo "batch order finished[${BATCHSTART}--`date +'%F %T'`]...." | tee -a $TMPLOG
fi