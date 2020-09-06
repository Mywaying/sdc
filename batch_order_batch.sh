#!/usr/bin/env bash
SQL=/app/software/run/t.sql
FDATE=`date '+%Y%m%d'`
JDBCURL=jdbc:hive2://DATA-DW-DEV-180:10000/ods
TMPLOG=/var/log/`echo ${SQL} |awk -F '/' '{print $3}'`.log
echo "">$TMPLOG
SQLL=`ls /app/software/run/*.sql`
SQL=`echo $SQLL |awk -F" " '{print $1}'`
sed -i "/set hive.execution.engine=spark/d" $SQL
sed -i '1i\set hive.execution.engine=spark;\n' $SQL
BATCHSTART=`date +'%F %T'`
echo "batch order starting...hold on[$BATCHSTART]...." | tee -a $TMPLOG
echo "beeline -u \"$JDBCURL\" -n hdfs -hivevar logdate=$FDATE  -f $SQL" |tee -a $TMPLOG
beeline -u "$JDBCURL" -n hdfs -hivevar logdate=$FDATE  -f $SQL 2>&1 |tee -a $TMPLOG
sed -i "/set hive.execution.engine=spark/d" $SQL
echo "batch order finished[${BATCHSTART}--`date +'%F %T'`]...." | tee -a $TMPLOG
if grep -q "ERROR - Error" $TMPLOG; then
    msg=`echo ${SQL} |awk -F '/' '{print $3}'`
    echo "BATCH ${msg} at ${BATCHSTART}" |mail -a $TMPLOG -s "${msg}" $MAILS
fi