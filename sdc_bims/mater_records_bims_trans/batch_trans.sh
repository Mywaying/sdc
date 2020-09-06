#!/usr/bin/env bash

FDATE=`date '+%Y%m%d'`
JDBCURL=jdbc:hive2://data-dwm-prod-53:10000/ods
TMPLOG=/var/log/mater_records_trans.log
echo "">$TMPLOG
HOST=data-dwn-prod-50
BATCHSTART=`date +'%F %T'`
echo "batch order starting...hold on[$BATCHSTART]...." | tee -a $TMPLOG
echo "beeline -u \"$JDBCURL\" -n hdfs -hivevar logdate=$FDATE  -f $SQL" |tee -a $TMPLOG
SQL=./script/mater_records_bims_trans.sql
#beeline -u "$JDBCURL" -n hdfs -hivevar logdate=$FDATE  -f ./script/mater_records_trans.sql
impala-shell -i $HOST -f $SQL |tee -a $TMPLOG
