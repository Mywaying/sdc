#!/usr/bin/env bash
SQL=$1
FDATE=`date '+%Y%m%d'`
JDBCURL=jdbc:hive2://DATA-DW-DEV-180:10000/ods
TMPLOG=/var/log/dim_company.log
echo "">$TMPLOG
BATCHSTART=`date +'%F %T'`
echo "batch order starting...hold on[$BATCHSTART]...." | tee -a $TMPLOG
echo "beeline -u \"$JDBCURL\" -n hdfs -hivevar logdate=$FDATE  -f $SQL" |tee -a $TMPLOG
beeline -u "$JDBCURL" -n hdfs -hivevar logdate=$FDATE  -f $SQL |tee -a $TMPLOG
echo "batch order finished[${BATCHSTART}--`date +'%F %T'`]...." | tee -a $TMPLOG