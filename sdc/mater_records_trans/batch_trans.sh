#!/usr/bin/env bash

FDATE=`date '+%Y%m%d'`
JDBCURL=jdbc:hive2://DATA-DW-DEV-180:10000/ods
TMPLOG=/var/log/mater_records_trans.log
echo "">$TMPLOG
HOST=data-dwn-prod-50
BATCHSTART=`date +'%F %T'`
echo "batch order starting...hold on[$BATCHSTART]...." | tee -a $TMPLOG
echo "beeline -u \"$JDBCURL\" -n hdfs -hivevar logdate=$FDATE  -f $SQL" |tee -a $TMPLOG
SQL=./script/mater_records_trans.sql
#beeline -u "$JDBCURL" -n hdfs -hivevar logdate=$FDATE  -f ./script/mater_records_trans.sql
impala-shell -i $HOST -f $SQL 2>&1 |tee -a $TMPLOG
#beeline -u "$JDBCURL" -n hdfs -hivevar logdate=$FDATE  -f ./script/mater_records_wx_trans.sql
HOST=./script/mater_records_wx_trans.sql
impala-shell -i $HOST -f $SQL 2>&1 |tee -a $TMPLOG
echo "batch order finished[${BATCHSTART}--`date +'%F %T'`]...." | tee -a $TMPLOG
a=`grep -E -C 3 "CAUSED BY: Exception|Error: Error|Could not execute command|ERROR - ERROR:" $TMPLOG`
a=`echo $a`
if [ -n "$a" ];then
    a=''
    msg=`echo ${SQL} |awk -F '/' '{print $3}'`
    echo "BATCH ${msg} at ${BATCHSTART}" |mail -a $TMPLOG -s "[FAILED]${msg} at ${BATCHSTART}" $MAILS
    content="[BD-DATA]FAILED UPDATE PACKET JOB ${SQL} ${a} at `date +'%F %T'`"
    /usr/bin/python /usr/bin/run/sendsms2.py "${MOBILE}" "${content}"
    exit 1
fi
