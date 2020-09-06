#!/usr/bin/env bash
SQL=$1
MAILS=wangwei@ddjf.com.cn
BATCHSTART=`date +'%F %T'`
MOBILE=""
#MOBILE="15994732290"
START=`date +'%F '`
log=`ls -l /var/log/*sql.log -rt |tac |head -1|awk  '{print $9}'`
content="[BD-DATA]SUCCESS UPDATE PACKET JOB ${SQL} at `date +'%F %T'` log:$log"
echo "$log"
a=`grep -E -C 1 "batch order finished\[${START}" $log`
a=`echo $a`
if [ -n "$a" ];then
    echo "BATCH SUCCESS ${SQL}" |mail -s "[SUCCESS]${SQL} at ${BATCHSTART}" $MAILS
    if [ -n "$MOBILE" ];then
        /usr/bin/python /usr/bin/run/sendsms2.py "${MOBILE}" "${content}"
    fi
fi