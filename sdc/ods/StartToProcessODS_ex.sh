# -*- coding: utf8 -*-
#demo:sh /root/dwsys/ods/bin/StartToProcessODS_ex.sh /root/dwsys/ods/conf/test.cfg test mysql2hive
#!/bin/bash
V_TABLE_FILE=$1
V_SOURCE_DB=$2
V_JOB_FILE=$3
SUFFIX=$4
V_LOG_DIR=/root/dwsys/ods/logs/`date "+%Y%m%d"`
V_SH_DIR=./


if [ ! -e ${V_LOG_DIR} ];then
  mkdir -p ${V_LOG_DIR}
fi

#V_TABLE_FILE=/root/dwsys/ods/conf/table.cfg
TMPLOG=/tmp/StartToProcessODS_ex.log
initfile=/root/dwsys/ods/bin/sh/init
BATCHTART=`date +'%F %T'`
echo "">$TMPLOG
echo "">${V_LOG_DIR}/ODS_ERROR_`date +%F`.txt
echo "">${V_LOG_DIR}/ODS_SUCCESSE_`date +%F`.txt

CMDSTART=`date +'%F %T'`
echo "batch starting...hold on[$BATCHTART]...." | tee -a $TMPLOG

if [ -f ${initfile} ];then
   source  ${initfile}
fi

if [ -e ${SUFFIX} ];then
 SUFFIX=`date  +"%Y%m%d" -d  "-1 days"`
fi

V_MAIN_LOG=${V_LOG_DIR}/MAIN.LOG`date "+%F"`
if [ ! -f ${V_TABLE_FILE} ];then
  echo "${V_TABLE_FILE} not exists" >>${V_MAIN_LOG}
  exit 1
fi



if [ ! -d ${V_LOG_DIR} ];then
  mkdir -p ${V_LOG_DIR}
fi

#v_begin_time=`date "+%Y%m%d %H:%M:%S"`
#echo "START TO LOAD BPMS TABLE TO ODS  AT $v_begin_time" >>${V_MAIN_LOG}

V_SOURCE_DB=`cat $V_TABLE_FILE | grep V_SOURCE_DB | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
TABLELIST=`cat $V_TABLE_FILE | grep TABLELIST | awk -F'=' '{ print $2 }'`
MAILS=`cat $V_TABLE_FILE | grep MAILS | awk -F'=' '{ print $2 }'`

#MAIN PROGRAM
#cat ${v_mailfile} |while read receiver
#do
#    /root/dwsys/ods/bin/sh/mail.sh ${receiver} "同步数据库${V_SOURCE_DB}"  "BEGIN TO PULL ${V_SOURCE_DB} DB TO ODS AT $HOSTNAME"
#    #echo "step 1:Begin to Synchronize ${V_SOURCE_DB} database at $HOSTNAME" |mail -s "${V_SOURCE_DB} to ods begin" liaowenjun@ddjf.com.cn
#done
for tab_name in ${TABLELIST};
do
      echo "V_TABLE_FILE:${V_TABLE_FILE}"
      BATCHTAB=`date +'%F %T'`
      echo "start_job [$tab_name] start[$BATCHTAB]...." |tee -a $TMPLOG
      echo "sh ${V_SH_DIR}/start_hive_job.sh ${tab_name} ${V_JOB_FILE} ${V_TABLE_FILE} ${SUFFIX}>${V_LOG_DIR}/${tab_name}.log`date "+%F"` 2>&1" |tee -a $TMPLOG
      sh ${V_SH_DIR}/start_hive_job.sh ${tab_name} ${V_JOB_FILE} ${V_TABLE_FILE} ${SUFFIX}>${V_LOG_DIR}/${tab_name}.log`date "+%F"` 2>&1
      echo "start_job [$tab_name] finished[$BATCHTAB -`date +'%F %T'`]...." |tee -a $TMPLOG
done
#last execute error
file=ODS_ERROR_`date +%F`.txt
ERROR_LIST=`cat ${V_LOG_DIR}/${file} |awk -F" " '{print $2}'`
for tab_name in ${ERROR_LIST};
do
     echo "start last execute error [$tab_name] start[$BATCHTAB]...." |tee -a $TMPLOG
     echo "sh ${V_SH_DIR}/start_hive_job_last.sh ${tab_name} ${V_JOB_FILE} ${V_TABLE_FILE} ${SUFFIX}>${V_LOG_DIR}/${tab_name}.log`date "+%F"` 2>&1" |tee -a $TMPLOG
done
if [ -f ${V_LOG_DIR}/ODS_ERROR_LAST`date +%F`.txt ];then
    echo "同步数据详情可见附件，请查阅！"  |mail -a ${V_LOG_DIR}/ODS_SUCCESSE_`date +%F`.txt -s "[FAILED]${V_SOURCE_DB}同步ODS数据" ${MAILS}
else
    echo "同步数据详情可见附件，请查阅！"  |mail -a ${V_LOG_DIR}/ODS_SUCCESSE_`date +%F`.txt -s "[SUCCESS]${V_SOURCE_DB}同步ODS数据" ${MAILS}
fi
echo "batch finished[$BATCHTART -`date +'%F %T'`]]...." | tee -a $TMPLOG
if [ ! -e ${MAILS} ];then
    echo "同步数据详情可见附件，请查阅！"  |mail -a ${V_LOG_DIR}/ODS_SUCCESSE_`date +%F`.txt -s "[SUCCESS]${V_SOURCE_DB}同步ODS数据" ${MAILS}
fi