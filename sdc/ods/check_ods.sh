#!/usr/bin/env bash
source /etc/profile
JDBCURL=$JDBCURL
HOST=$HOST
CHKCONIG='bpms_flow entit_platform guaranty-apps guaranty-bims'
CHKCONIG=$1
if [ -z "$CHKCONIG" ];then
    echo "please get the config"
    exit 0
fi
TMPLOG=/tmp/check_ods.log
LOG=/var/log/check_ods_`date +%F`.log
echo "" >$TMPLOG
BATCHTAB=`date +'%F %T'`
echo "check_ods start [${BATCHTAB}"|tee -a $TMPLOG

for obj in ${CHKCONIG};
do
    CFG=./${obj}.cfg
    JOB=./${obj}.job
    V_SOURCE_DB=`cat $CFG | grep V_SOURCE_DB | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
    TABLELIST=`cat $CFG | grep TABLELIST | awk -F'=' '{ print $2 }'`
    KETTLE=`cat ${JOB} |grep "^command" | awk -F'=' '{ print $2 }' |awk -F ' ' '{print $5}'`
    echo "CFG:$CFG"
    echo "KETTLE:$KETTLE"
    echo "TABLELIST:$TABLELIST"
    echo "V_SOURCE_DB:$V_SOURCE_DB"
    for tab_name in ${TABLELIST};
    do
         IFS=","
         array=($HOST)
         rn=${#array[@]}
         n=$[RANDOM%${rn}+0]
         NHOST=`echo ${array[$n]}`
         echo "V_TABLE_FILE:${tab_name}"
         sql="select count(*),'${V_SOURCE_DB}','ods_${V_SOURCE_DB}_${tab_name}' from ods.ods_${V_SOURCE_DB}_${tab_name}"
         echo $sql |tee -a $TMPLOG
         echo "a=impala-shell -i $NHOST --quiet --delimited -q \"$sql\"" |tee -a $TMPLOG
         echo "[`date +'%F %T'`]" |tee -a $LOG
         a=`impala-shell -i $NHOST --quiet --delimited -q "$sql" |tee -a $LOG`
         echo $a| awk -F '' '{print $1}' |tee -a $TMPLOG
         v=`echo $a| awk -F '' '{print $1}'`
         if [ "${v}" = "0" -o "${v}" = "" ];then
          echo "sh ./start_hive_job.sh ${tab_name} ${KETTLE} ${CFG}" |tee -a $LOG
          sh ./start_hive_job.sh ${tab_name} ${KETTLE} ${CFG} |tee -a $TMPLOG
          echo "impala-shell -i $NHOST -d ods -q \"INVALIDATE METADATA ods_${V_SOURCE_DB}_${tab_name};\"" |tee -a $TMPLOG
          impala-shell -i $NHOST -d ods -q "INVALIDATE METADATA ods_${V_SOURCE_DB}_${tab_name};"
         fi
    done
done
echo "check_ods finished [${BATCHTAB}--`date +'%F %T'`}"|tee -a $TMPLOG