#!/bin/bash
# Program:
#	执行sqoop脚本，并将结果邮件发送出去。
# History:
# 	20191104 v1.1
# parameters:
#	$1, sqoop脚本
#   $2, 需要truncate的表名
sqoopScript=$1
truncateTableName=$2
day=`date "+%Y%m%d"`
time_s=$(date "+%Y-%m-%d %H:%M:%S")
email_receiver=/root/dwsys/ods/conf/mail.cfg # 接收邮件的人员名单
v_log_dir=/tmp/dw_logs/sqoop_script_logs/${day} # 日志存储的路径
#如果日志的路径不存在则创建该路径
if [ ! -e ${v_log_dir} ];then
  mkdir -p ${v_log_dir}
fi
# 判断文件是否存在,不存在则输出相关信息并退出脚本
test ! -e $sqoopScript && echo "Sqoop script '$sqoopScript' do not exist" && exit 0 
# 清空目标表, 如果清空失败则发送邮件并退出脚本
sqoop eval --connect jdbc:mysql://ddjfprod.mysql.rds.aliyuncs.com:3306/spp_platform --username ddjf_spp --password ddJF@spp --query "TRUNCATE TABLE ${truncateTableName}" >> ${v_log_dir}/${sqoopScript##*/}.log`date "+%F"` 2>&1
if [ $? -ne 0 ]; then 
	echo "Sqoop script '$sqoopScript' error"
	cat ${email_receiver} |while read receiver  # 获取接收邮件的人员名单
    do
    	/root/dwsys/ods/bin/sh/mail.sh ${receiver} "SqoopScript_Failed" "TRUNCATE  TABLE ${truncateTableName} FAILED AT ${time_s}"  # 发送脚本执行错误的邮件
    done
    exit 0
fi
# 使用hdfs 执行sqoop脚本并且存储相关日志
# su hdfs -c "sqoop --options-file $sqoopScript" 2>&1 | tee ${v_log_dir}/${sqoopScript##*/}.log`date "+%F"`
su hdfs -c "sqoop --options-file $sqoopScript" >> ${v_log_dir}/${sqoopScript##*/}.log`date "+%F"` 2>&1

# 发送脚本执行错误的邮件
if [ $? -ne 0 ]; then 
	echo "Sqoop script '$sqoopScript' error"
	cat ${email_receiver} |while read receiver  # 获取接收邮件的人员名单
    do
    	/root/dwsys/ods/bin/sh/mail.sh ${receiver} "SqoopScript_Failed" "EXECUTE ${sqoopScript##*/} FAILED AT ${time_s}"  # 发送脚本执行错误的邮件
    done
    exit 0

# 发送脚本执行成功的邮件
else
	cat ${email_receiver} |while read receiver  # 获取接收邮件的人员名单
    do
    	/root/dwsys/ods/bin/sh/mail.sh ${receiver} "SqoopScript_Succeed" "EXECUTE ${sqoopScript##*/} SUCCEED AT ${time_s}"  # 发送脚本执行成功的邮件
    done
fi
