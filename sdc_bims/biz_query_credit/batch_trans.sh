#!/usr/bin/env bash
SQL=$1
FDATE=`date '+%Y%m%d'`
JDBCURL=jdbc:hive2://data-dwm-prod-53:10000/ods
TMPLOG=/var/log/`echo ${SQL} |awk -F '/' '{print $3}'`.log
echo "">$TMPLOG
HOST=data-dwn-prod-50
BATCHSTART=`date +'%F %T'`
echo "batch order starting...hold on[$BATCHSTART]...." | tee -a $TMPLOG
#echo "beeline -u \"$JDBCURL\" -n hdfs -hivevar logdate=$FDATE  -f $SQL" |tee -a $TMPLOG
impala-shell -i $HOST -f $SQL |tee -a $TMPLOG
echo "batch order finished[${BATCHSTART}--`date +'%F %T'`]...." | tee -a $TMPLOG
