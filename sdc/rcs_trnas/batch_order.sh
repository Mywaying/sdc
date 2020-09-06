#!/usr/bin/env bash
SQL=$1
FDATE=`date '+%Y%m%d'`
JDBCURL=jdbc:hive2://DATA-DW-DEV-180:10000/ods
TMPLOG=/var/log/`echo ${SQL} |awk -F '/' '{print $3}'`.log
echo "">$TMPLOG
BATCHSTART=`date +'%F %T'`
echo "batch order starting...hold on[$BATCHSTART]...." | tee -a $TMPLOG
echo "beeline -u \"$JDBCURL\" -n hdfs -hivevar logdate=$FDATE  -f $SQL" |tee -a $TMPLOG
beeline -u "$JDBCURL" -n hdfs -hivevar logdate=$FDATE  -f $SQL 2>&1 |tee -a $TMPLOG
echo "batch order finished[${BATCHSTART}--`date +'%F %T'`]...." | tee -a $TMPLOG
if grep -q -E "Error: Error|Could not execute command" $TMPLOG; then
    msg=`echo ${SQL} |awk -F '/' '{print $3}'`
    echo "BATCH ${msg} at ${BATCHSTART}" |mail -a $TMPLOG -s "[FAILED]${msg} at ${BATCHSTART}" $MAILS
fi
