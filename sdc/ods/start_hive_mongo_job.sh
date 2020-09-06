# -*- coding: utf8 -*-
#!/bin/bash
TAB_NAME=$1
JOB_FILE=$2
CFG=$3
V_LOG_DIR=/root/dwsys/ods/logs/`date "+%Y%m%d"`
V_KETTLE_DIR=/usr/local/kettle/pdi-ce-6.1.0.1-196/data-integration
GENSQL_CMD=./mongo_tpl_hsql.py
INITFILE=/root/dwsys/ods/bin/sh/init
TIME=`date "+%F %H:%M:%S"`
TMPLOG=/tmp/start_hive_job.log
REP=sit
PREFIX=ods
SUFFIX=$4
ODS_CFG=~/.kettle/kettle.properties
echo "">$TMPLOG
if [ ! -f ${CFG} ];then
   echo "$CFG is lost " |tee -a $TMPLOG
fi

if [ ! -d /tmp/ods ];then
   mkdir -pv /tmp/ods
fi

if [ -f ${INITFILE} ];then
   source  ${INITFILE}
fi

if [ ! -e ${V_LOG_DIR} ];then
  mkdir -p ${V_LOG_DIR}
fi

if [ -e ${SUFFIX} ];then
 SUFFIX=`date  +"%Y%m%d" -d  "-1 days"`
fi
HOST=`cat $CFG | grep HOST | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
PORT=`cat $CFG | grep PORT | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
USER=`cat $CFG | grep USER | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
PASSWORD=`cat $CFG | grep PASSWORD | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
HOST=`cat $ODS_CFG | grep "^$HOST" | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
PORT=`cat $ODS_CFG | grep "^$PORT" | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
USER=`cat $ODS_CFG | grep "^$USER" | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
PASSWORD=`cat $ODS_CFG | grep "^$PASSWORD" | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
SOURCE_DB=`cat $CFG | grep V_SOURCE_DB | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
MAILS=`cat $CFG | grep MAILS | awk -F'=' '{ print $2 }'`
TBSQL=`cat $CFG | grep TBSQL | awk -F'=' '{ print $2 }'`
S_SOURCE_DB='`'"${SOURCE_DB}"'`'
echo "TAB_NAME:$TAB_NAME"
echo "SOURCE_DB:$SOURCE_DB"
echo "HOST:$HOST"
echo "PORT:$PORT"
echo "USER:$USER"
echo "PASSWORD:$PASSWORD"
echo ""
#echo "HOST:$HOST PORT:$PORT,USER:$USER,PASSWORD:$PASSWORD,TAB_NAME:$TAB_NAME"
echo "/usr/bin/python $GENSQL_CMD --host=$HOST --port=$PORT --user=$USER --password=$PASSWORD --db=$SOURCE_DB --tables=$TAB_NAME --prefix=$PREFIX" |tee -a $TMPLOG
#a=`/usr/bin/python $GENSQL_CMD --host=$HOST --port=$PORT --user=$USER --password=$PASSWORD --db=$SOURCE_DB --tables=$TAB_NAME --prefix=$PREFIX`
tabcol=''
tabhsql=$TBSQL
echo "$tabhsql" |tee -a $TMPLOG
touch /tmp/${TAB_NAME}.pid
mkdir -pv /tmp/log
KETTLE_LOG=/tmp/log/${TAB_NAME}.log
NUM=20
while [ -f /tmp/${TAB_NAME}.pid -a ${NUM} -gt 0 ];
do
    echo "sh ${V_KETTLE_DIR}/kitchen.sh -rep $REP -user admin -pass admin -dir /  /job ${JOB_FILE} -param:datestr=${SUFFIX} -param:srcdb=`${SOURCE_DB}` -param:tabnn=${TAB_NAME} -param:tabcol=\"${tabcol}\"  -param:tabhsql=\"${tabhsql}\"" |tee -a $KETTLE_LOG
    sh ${V_KETTLE_DIR}/kitchen.sh -rep $REP -user admin -pass admin -dir /  /job ${JOB_FILE} -param:datestr=${SUFFIX} -param:srcdb=${SOURCE_DB} -param:tabnn=${TAB_NAME} -param:tabcol="${tabcol}"  -param:tabhsql="${tabhsql}"  2>&1| tee -a $KETTLE_LOG
    if grep -q "Finished with error" $KETTLE_LOG; then
        echo "TABLE ${TAB_NAME} FROM ${SOURCE_DB} TO ODS FAILED AT ${TIME}" >>${V_LOG_DIR}/ODS_ERROR_`date +%F`.txt
        echo "TABLE ${TAB_NAME} FROM ${SOURCE_DB} TO ODS FAILED at ${TIME}" |mail -a $KETTLE_LOG -s "[FAILED]TABLE ${TAB_NAME} FROM ${SOURCE_DB} TO ODS" $MAILS
        if [ $? -ne 0 ];then
           echo "send mail failed" >>${V_LOG_DIR}/mail.log`date "+%F"`
        fi
        echo "">$KETTLE_LOG
    else
        echo "TABLE ${TAB_NAME} FROM ${SOURCE_DB} TO ODS SUCCESSED at ${TIME}" >>${V_LOG_DIR}/ODS_SUCCESSE_`date +%F`.txt
        sed -i "/^TABLE*.${TAB_NAME}/d" ${V_LOG_DIR}/ODS_ERROR_`date +%F`.txt
        rm -rvf /tmp/${TAB_NAME}.pid
        echo "">$KETTLE_LOG
    fi
    NUM=`expr $NUM - 1`
done

