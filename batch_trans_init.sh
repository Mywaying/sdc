#!/usr/bin/env bash
SQL=$1
first=$2
second=$3
source /etc/profile
echo $SQL
echo $first
echo $second
while [ "$first" != "$second" ]
do
    echo "$first"
    echo "sh ../../batch_trans.sh ${SQL} ${first}"
    sh ../../batch_trans.sh $SQL $first
    first=`date -d "-1 days ago ${first}" +%Y-%m-%d`
done