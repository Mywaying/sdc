#!/usr/bin/env bash
# --------------------------------------------------------------------------
# Filename:    rbase.sh
# Revision:    1.1
# Date:        2019/08/28
# Author:      ravid
# Description:
# Notes:       This plugin uses the "" command
#Version 1.0
#rbase databse line from date
V_TABLE_FILE=$1
FDATE=$2
HOST=10.11.0.180
USER=ravid
PASSWORD=321
MATEDATA=hive
TMPLOG=/tmp/rbase.log
if [ -z $FDATE ];then
   echo "please give then rbase date suffix"
   exit 1
fi
MAILS=wangwei@ddjf.com.cn
JDBCURL=jdbc:hive2://DATA-DW-DEV-180:10000/ods
if [ ! -f ${V_TABLE_FILE} ];then
  echo "${V_TABLE_FILE} not exists" >>${V_MAIN_LOG}
  exit 1
fi
STARTTIME=`date "+%F %H:%M:%S"`
echo "rebase starting...hold on[$STARTTIME]...." | tee -a $TMPLOG
FILETIME=`date "+%Y%m%d%H%M%S"`
TABLELIST=`cat $V_TABLE_FILE | grep TABLELIST | awk -F'=' '{ print $2 }'`
MAILS=`cat $V_TABLE_FILE | grep MAILS | awk -F'=' '{ print $2 }'`
V_SOURCE_DB=`cat $V_TABLE_FILE | grep V_SOURCE_DB | awk -F'=' '{ print $2 }'`
BASESQL=/tmp/${V_SOURCE_DB}_tablelist`date +%F`.sql
echo "">/tmp/beeline.log
echo "use ods;" >$BASESQL
for tab_name in ${TABLELIST}
do
   echo "mysql -u${USER} -p${PASSWORD} -h ${HOST} -N -e \"select * from ${MATEDATA}.TBLS ts where ts.TBL_NAME like '%ods_${V_SOURCE_DB}_${tab_name}_${FDATE}%'\" "  | tee -a $TMPLOG
   a=`mysql -u${USER} -p${PASSWORD} -h ${HOST} -N -e "select * from ${MATEDATA}.TBLS ts where ts.TBL_NAME like '%ods_${V_SOURCE_DB}_${tab_name}_${FDATE}%'"`
   b=`mysql -u${USER} -p${PASSWORD} -h ${HOST} -N -e "select * from ${MATEDATA}.TBLS ts where ts.TBL_NAME ='ods_${V_SOURCE_DB}_${tab_name}'"`
   if [ -n "$a" -a -n "$b" ];then
       echo "alter table ods.ods_${V_SOURCE_DB}_${tab_name} rename to ods.ods_${V_SOURCE_DB}_${tab_name}_${FILETIME}" | tee -a $TMPLOG
       echo "alter table ods.ods_${V_SOURCE_DB}_${tab_name} rename to ods.ods_${V_SOURCE_DB}_${tab_name}_${FILETIME};">>$BASESQL
       echo "alter table ods.ods_${V_SOURCE_DB}_${tab_name}_${FDATE} rename to ods.ods_${V_SOURCE_DB}_${tab_name};">>$BASESQL
   fi
done
echo "beeline -u \"$JDBCURL\" -n hdfs -f $BASESQL" |tee -a $TMPLOG
beeline -u "$JDBCURL" -n hdfs -f $BASESQL 2>&1 |tee -a /tmp/beeline.log
if grep -q -E "Error: Error|Could not execute command" /tmp/beeline.log; then
    msg=`echo ${SQL} |awk -F '/' '{print $3}'`
    echo "ALTER ${msg} at ${STARTTIME}" |mail -a /tmp/beeline.log -s "[FAILED]${msg} at `date +'%F %T'`" $MAILS
else
    echo "rebase finished[$STARTTIME--`date +'%F %T'`]." | tee -a $TMPLOG
    echo "数据源已经切换，请查阅！"  |mail -a ${BASESQL} -s "[SUCCESS]数据源已经切换" ${MAILS}
fi