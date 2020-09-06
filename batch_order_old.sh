#!/usr/bin/env bash
SQL=$1
FDATE=$2
if [ -z $FDATE ];then
    FDATE=`date -d '-1 day' '+%Y-%m-%d'`
fi
JDBCURL=jdbc:hive2://DATA-DW-DEV-180:10000/ods
TMPLOG=/var/log/`echo ${SQL} |awk -F '/' '{print $3}'`.log
echo "">$TMPLOG
BATCHSTART=`date +'%F %T'`
HOST=DATA-DW-DEV-181
LOG=/var/log/batch_order.log
MAILS=wangwei@ddjf.com.cn
HOlDTIME=900
HOlDTIME=60
CMD=/opt/cloudera/parcels/CDH-5.12.1-1.cdh5.12.1.p0.3/bin/yarn
MOBILE="15994732290|13670055771"
echo "batch order starting...hold on[$BATCHSTART]...." | tee -a $TMPLOG
str=`grep -E -C 1 "set hive.execution.engine=spark|ods_rcs_icr" $SQL`
str=`echo $str`
if [ -n "$str" ];then
    if ! grep -q "set spark.app.name=" ${SQL};then
	    sed -i "1i\set spark.app.name=${SQL};" ${SQL}
	    sed -i "1i\set mapred.job.name=${SQL};" ${SQL}
    fi
fi
echo "LOGFILE=/tmp/log/${filename}.log" |tee -a $TMPLOG
filename=$(basename "$SQL")
touch /tmp/${filename}.pid
mkdir -pv /tmp/log/
LOGFILE=/tmp/log/${filename}.log
STARTTIME=`date +%F`
NUM=1
WHITELIST="orders_batch_init_1.sql dwd_rsc_credit.sql"
c=`echo ${WHITELIST} |grep "${filename}"`
if [ -n "$c" ];then #1h
    NUM=-3600
fi
rm -rvf /tmp/log/${filename}.log
while [ -f /tmp/${filename}.pid ];
do
    if [ ${NUM} -eq 1 -o ${NUM} -eq -3600 ];then
        echo "beeline -u \"$JDBCURL\" -n hdfs -hivevar logdate=$FDATE  -f $SQL" |tee -a $TMPLOG
        beeline -u "$JDBCURL" -n hdfs -hivevar logdate=$FDATE  -f $SQL >/tmp/log/${filename}.log  2>&1 &
    fi
    a=`grep -E -C 1 "Error connecting|CAUSED BY: Exception|Error: Error|Could not execute command|ERROR - ERROR:" ${LOGFILE}`
    b=`grep -E -C 1 "Closing: 0: jdbc:hive2" ${LOGFILE}`
    a=`echo $a`
    b=`echo $b`
    if [ -n "$a" -o ${NUM} -gt ${HOlDTIME} ];then
        cat ${LOGFILE} |tee -a $TMPLOG
        msg=`echo ${SQL} |awk -F '/' '{print $3}'`
        echo "BATCH ${msg} at ${BATCHSTART}" |mail -a ${LOGFILE} -s "[FAILED]${msg} at ${BATCHSTART}" $MAILS
        content="[BD-DATA]FAILED UPDATE PACKET JOB ${SQL} at `date +'%F %T'`"
        /usr/bin/python /usr/bin/run/sendsms2.py "${MOBILE}" "${content}"
        echo "batch order failed[$SQL][${BATCHSTART}--`date +'%F %T'`]...." | tee -a $TMPLOG
        $CMD application -list |grep "^application_"|awk '{print $1" "$2" "$6" "$9}'|grep ${SQL}  |tee /var/log/${filename}.log
        appid=`cat /var/log/${filename}.log |awk '{print $1}'`
        echo "$CMD application -kill ${appid}" |tee -a $TMPLOG
        $CMD application -kill ${appid}
        echo "kill ${SQL} at: [`date +'%F %T']`" |tee -a $LOG
        exit 1
    elif [ -n "$b" ];then
        a=`grep "ALTER TABLE " ${SQL} |awk '{print $6}'|cut -d ';' -f1`
        if [ -n "$a" ];then
            echo "impala-shell -i $HOST -d ods -q \"INVALIDATE METADATA $a;\"" | tee -a $TMPLOG
            impala-shell -i $HOST -d ods -q "INVALIDATE METADATA $a;"
        fi
        echo "batch order finished[${BATCHSTART}--`date +'%F %T'`]...." | tee -a $TMPLOG
        rm /tmp/${filename}.pid
        exit 0
    fi
    NUM=`expr $NUM + 1`
    echo "$SQL doing.... [`date +'%F %T'`][$NUM]"
    sleep 1
done
rm /tmp/${filename}.pid