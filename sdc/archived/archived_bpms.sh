#!/usr/bin/env bash
# --------------------------------------------------------------------------
# Filename:    sqoop_full.sh
# Revision:    1.1
# Date:        2015/06/27
# Author:      ravid
# Email:       wwv2006@163.com
# Description: sqoop_full.sh
# Notes:       This plugin uses the "" command
#Version 1.0
#The first one , sqoop_full.sh
#update 加入全实例数据
. /etc/bashrc
MYSQLCMD=/usr/bin/mysql
SQOOPCMD=/usr/bin/sqoop
CFG=$1
TMPLOG="/var/log/sqoop_full.log"
HIVE_HOST=10.11.0.181
THRIFT_PORT=21050
#HOST=192.168.1.10
#PORT=3306
#USER=ravid
#PASSWORD=321
HOST=`cat $CFG | grep HOST | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
PORT=`cat $CFG | grep PORT | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
USER=`cat $CFG | grep USER | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
PASSWORD=`cat $CFG | grep PASSWORD | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
MAILS=`cat $CFG | grep MAILS | awk -F'=' '{ print $2 }'`
M=8
TYPE=-'--direct'
TYPE=''
#--direct 参数目前只有MySQL支持，不支持BLOB, CLOB, LONGVARBINARY 列
#--direct不支持视图
HIVEDB=ods
echo "">$TMPLOG
WAREDEST=/user/hive/warehouse
#DATABASELIST=`cat $CFG | grep $i.extables | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g`
DATABASELIST=`cat $CFG | grep DATABASES | awk -F'=' '{ print $2 }' `
echo "$DATABASELIST"|tee -a $TMPLOG
DUMPSTART=`date +'%F %T'`
echo "dump start[$DUMPSTART]...." |tee -a $TMPLOG
if [ ! "`${MYSQLCMD}admin -h$HOST -u$USER -p$PASSWORD ping`" == "mysqld is alive" ];then
	 echo "`date +%F" "%T` mysqld is not b alive" |tee -a /var/log/sqoop_full.log
	 exit
fi
if [ -e "$DATABASELIST" ];then
	echo "full instance dump start[`date +'%F %T'`]...." |tee -a $TMPLOG
	DATABASELIST=`mysql -h $HOST -N -u$USER -p$PASSWORD -e 'show databases'`
else
	for i in $DATABASELIST;
	do
		echo "dump $i start[`date +'%F %T'`]...." |tee -a $TMPLOG
		HIVEDB=bak_${i/-/_}_`date +%Y%m%d`
        hive -f ./script/init_database.sql -hivevar v_dbname=${HIVEDB}
        if [ $? -ne 1 ];then
            echo CREATE database failed!" >>${V_LOG_DIR}/mail.log`date "+%F"`
        else
            echo CREATE database SUCCESSED!"  |tee -a $TMPLOG
        fi
		echo "cat $CFG | grep ${i}.extables | awk -F'=' '{ print $2 }' | sed s/[[:space:]]//g"|tee -a $TMPLOG
        extables=`cat $CFG | grep ${i}.extables | awk -F'=' '{ print $2 }'`
        if [ -e $extables ];then
            echo "$SQOOPCMD import-all-tables -m $M --connect jdbc:mysql://$HOST:$PORT/$i --username=$USER --password=$PASSWORD --compression-codec=snappy --hive-overwrite $TYPE--warehouse-dir=${WAREDEST}/${HIVEDB} --hive-import --hive-database=$HIVEDB --null-string '\\N' --null-non-string '\\N' --outdir /tmp/sqoop/java --as-parquetfile --compression-codec Snappy" | tee -a $TMPLOG
            $SQOOPCMD import-all-tables -m $M --connect jdbc:mysql://$HOST:$PORT/$i --username=$USER --password=$PASSWORD --compression-codec=snappy --hive-overwrite $TYPE --warehouse-dir=${WAREDEST}/${HIVEDB} --hive-import --hive-database=$HIVEDB --null-string '\\N' --null-non-string '\\N' --outdir /tmp/sqoop/java --as-parquetfile --compression-codec Snappy 2>&1| tee -a $TMPLOG
        else
            echo "$SQOOPCMD import-all-tables -m $M --connect jdbc:mysql://$HOST:$PORT/$i --username=$USER --password=$PASSWORD --exclude-tables=$extables --compression-codec=snappy --hive-overwrite $TYPE --warehouse-dir=${WAREDEST}/${HIVEDB} --hive-import --hive-database=$HIVEDB --null-string '\\N' --null-non-string '\\N' --outdir /tmp/sqoop/java --as-parquetfile --compression-codec Snappy"tee -a log
            $SQOOPCMD import-all-tables -m $M --connect jdbc:mysql://$HOST:$PORT/$i --username=$USER --password=$PASSWORD --exclude-tables=$extables --compression-codec=snappy --hive-overwrite $TYPE --warehouse-dir=${WAREDEST}/${HIVEDB} --hive-import --hive-database=$HIVEDB --null-string '\\N' --null-non-string '\\N' --outdir /tmp/sqoop/java --as-parquetfile --compression-codec Snappy 2>&1| tee -a $TMPLOG
        fi
        if grep ERROR "$TMPLOG" > /dev/null; then
           echo "${i} TO ARCHIVED ODS FAILED at `date +%F`" |mail -a $TMPLOG -s "[FAILED] ${i} TO ARCHIVED FAILED" $MAILS
        else
           echo "${i} TO ARCHIVED ODS SUCCESSE at `date +%F`" |mail -s "[SUCCESSE] ${i} TO ARCHIVED SUCCESSE" $MAILS
           echo "dump $i finished[`date +'%F %T'`]...." |tee -a $TMPLOG
        fi
	done
fi
echo "dump finished[$DUMPSTART--`date +'%F %T'`]...." |tee -a $TMPLOG
#/usr/bin/python impala_metadata.py $HIVE_HOST $THRIFT_PORT