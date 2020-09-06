# -*- coding: utf8 -*-
#demo:sh /root/dwsys/ods/bin/StartToProcessODS_ex.sh /root/dwsys/ods/conf/test.cfg test mysql2hive
#!/bin/bash
source /etc/profile
PROC_NUM=$4
fd_fifo=/tmp/fd_1
rm -rvf $fd_fifo
mkfifo $fd_fifo      #创建命令管道(pipe类型文件)
exec 6<>$fd_fifo     #将管道的fd与6号fd绑定

count=0;
if [ -e ${PROC_NUM} ];then
 PROC_NUM=5 #进程个数
fi

#预分配资源
for ((i=0;i<$PROC_NUM;i++))
do
    echo >& 6        #写入一个空行
done
HOST=$HOST
V_TABLE_FILE=$1
V_SOURCE_DB=$2
V_JOB_FILE=$3

SUFFIX=$5
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
V_SOURCE_DB=`cat $V_TABLE_FILE | grep V_SOURCE_DB | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
TABLELIST=`cat $V_TABLE_FILE | grep TABLELIST | awk -F'=' '{ print $2 }'`
MAILS=`cat $V_TABLE_FILE | grep MAILS | awk -F'=' '{ print $2 }'`

start=`date +"%s"`
for tab_name in ${TABLELIST}
do
  read -u 6          #读取一个空行
  {
      echo "V_TABLE_FILE:${V_TABLE_FILE}"
      BATCHTAB=`date +'%F %T'`
      echo "start_job [$tab_name] start[$BATCHTAB]...." |tee -a $TMPLOG
      echo "sh ${V_SH_DIR}/start_hive_job.sh ${tab_name} ${V_JOB_FILE} ${V_TABLE_FILE} ${SUFFIX}>${V_LOG_DIR}/${tab_name}.log`date "+%F"` 2>&1" |tee -a $TMPLOG
      sh ${V_SH_DIR}/start_hive_job.sh ${tab_name} ${V_JOB_FILE} ${V_TABLE_FILE} ${SUFFIX}>${V_LOG_DIR}/${tab_name}.log`date "+%F"`
      echo "start_job [$tab_name] finished[$BATCHTAB -`date +'%F %T'`]...." |tee -a $TMPLOG
      sleep 1
      echo >& 6      #完成任务，写入一个空行
  }&                 #后台执行
done
wait                #等待所有的任务完成
exec 6>&-           #关闭fd 6描述符，stdou和stdin
exec 6<&-
rm -f $fd_fifo          #删除管道
#last execute error
file=ODS_ERROR_`date +%F`.txt
ERROR_LIST=`cat ${V_LOG_DIR}/${file} |awk -F" " '{print $2}'`
#for tab_name in ${ERROR_LIST};
#do
#     echo "start last execute error [$tab_name] start[$BATCHTAB]...." |tee -a $TMPLOG
#     echo "sh ${V_SH_DIR}/start_hive_job_last.sh ${tab_name} ${V_JOB_FILE} ${V_TABLE_FILE} ${SUFFIX}>${V_LOG_DIR}/${tab_name}.log`date "+%F"` 2>&1" |tee -a $TMPLOG
#     sh ${V_SH_DIR}/start_hive_job.sh ${tab_name} ${V_JOB_FILE} ${V_TABLE_FILE} ${SUFFIX}>${V_LOG_DIR}/${tab_name}.log`date "+%F"`
#done
CFG=`echo "$V_TABLE_FILE" |cut -d . -f2 |cut -d / -f2`
if [ -n "$ERROR_LIST" ];then
   echo "同步数据详情可见附件，请查阅！"  |mail -a ${V_LOG_DIR}/${file} -s "[FAILED]${V_SOURCE_DB}同步ODS数据" ${MAILS}
else
   echo "/bin/sh ./check_ods.sh ${CFG} " |tee -a $TMPLOG
   /bin/sh ./check_ods.sh ${CFG}
   echo "同步数据详情可见附件，请查阅！"  |mail -a ${V_LOG_DIR}/ODS_SUCCESSE_`date +%F`.txt -s "[SUCCESS]${V_SOURCE_DB}同步ODS数据" ${MAILS}
fi
echo "batch finished[$BATCHTART -`date +'%F %T'`]]...." | tee -a $TMPLOG
if [ ! -e ${MAILS} ];then
    echo "同步数据详情可见附件，请查阅！"  |mail -a ${V_LOG_DIR}/ODS_SUCCESSE_`date +%F`.txt -s "[SUCCESS]${V_SOURCE_DB}同步ODS数据" ${MAILS}
fi
end=`date +"%s"`
impala-shell -i $HOST -d ods -q "INVALIDATE METADATA;"
echo "time: " `expr $end - $start`