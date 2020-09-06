#!/usr/bin/env python
#coding:utf8
# --------------------------------------------------------------------------
# Filename:    mysql_tpl_hsql.py
# Revision:    1.1
# Date:        2016/08/19
# Author:      ravid
# Email:       wwv2006@163.com
# Description: mysql2hsql bash script
# Notes:       This plugin uses the "" command
#Version 1.0
#The first one , can monitor the screen memory
#python mysql_tpl_hsql.py --host=10.11.0.147 --port=3306 --user=ravid --password=321 --db=test --tables=tests
import re
import pymysql
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
def get_table_info(host,user,password,schema,predb,port,table,ispartition=False,prefix=''):
    '''
    table =  为表名，mysql,hive表名一致
    schema = 为hive中的库名
    ispartition : 是否分区默认为分区
    '''
    cols = []
    create_head = '''
create table if not exists {0}.{1}('''.format(prefix, prefix+"_"+predb+"_"+table)
    if ispartition:
        create_tail = r'''
partitioned by(inc_day string)
row format delimited fields terminated by '\t'
'''
    else:
        create_tail = r'''
row format delimited fields terminated by '\t'
'''
    connection = pymysql.connect(host=host,
                                 user=user,
                                 password=password,
                                 db=schema,
                                 port=port,
                                 charset='utf8'
                                 )
    try:
        # 获取一个游标
        with connection.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            sql = 'SHOW FULL FIELDS FROM  `{0}`.{1}'.format(schema, table)
            cout = cursor.execute(sql)  # 返回记录条数
            try:
                for row in cursor:  # cursor.fetchall()
                    # print(row)
                    Field="`"+row['Field']+"`"
                    cols.append(Field)
                    if 'bigint' in row['Type']:
                        row['Type'] = "bigint"
                    elif 'int' in row['Type'] or 'tinyint' in row['Type'] or 'smallint' in row['Type'] or 'mediumint' in \
                            row['Type'] or 'integer' in row['Type']:
                        row['Type'] = "bigint"
                    elif 'double' in row['Type'] or 'float' in row['Type'] or 'decimal' in row['Type']:
                        row['Type'] = "double"
                    elif 'timestamp' in row['Type'] or 'datetime' in row['Type']:
                        row['Type'] = "timestamp"
                    else:
                        row['Type'] = "string"
                    create_head += Field + ' ' + row['Type'] + ' comment \'' + row['Comment'].replace(",","").replace(";","").replace("\'","")+ '\' ,\n'
            except:
                print('程序异常!')
    finally:
        connection.close()
    create_str = create_head[:-2] + '\n' + ')' + create_tail
    return cols, create_str  # 返回字段列表与你建表语句

def create_sql(options):
    tables=[]
    prefix='ods'
    ispartition=0
    host=options.host
    port= int(options.port)
    user= options.user
    password= options.password
    dbnanme= options.dbnanme
    predb=options.predbnanme
    cols=''
    str1=''
    if options.tables:
        tables=options.tables
    if options.prefix:
        prefix=options.prefix
    if options.partition:
        ispartition = options.partition
    for tbname in tables[0].split(','):
        cols, str1 = get_table_info(host, user, password, dbnanme,predb, port, tbname, ispartition, prefix)
        # f1 = open(u'F:\\DDJF\\xys_dbm\\kettle\\'+dbnanme+'_'+tbname+'hcols.txt','w')
        # f2 = open(u'F:\\DDJF\\xys_dbm\\kettle\\'+dbnanme+'_'+tbname+'hsql.txt', 'w')
        f1 = open(u'/tmp/ods/'+predb+"_"+tbname+'hcols.txt', 'w')
        f2 = open(u'/tmp/ods/'+predb+"_"+tbname+'hsql.txt', 'w')
        tmp = str(cols).replace('u\'','').replace(']','').replace('\'','').replace('[','')
        f1.write(tmp)
        tmp = str(str1)
        f2.write(tmp)
        # f1.write(str)
        f1.close()
        f2.close()
        print '---------------------------------------------------------------------'
        print cols,str1
    return cols,str

if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option('--host',dest='host', help='mysql server ip')
    parser.add_option('--port', dest='port',default=3306,help='mysql port')
    parser.add_option('--user', dest='user', help='mysql user')
    parser.add_option('--password',dest='password', help='mysql user password')
    parser.add_option('--db',dest='dbnanme', help='mysql default db name')
    parser.add_option('--predb',dest='predbnanme', help='mysql default db name')
    parser.add_option('--tables',dest='tables',action='append', help='many mysql tables,separator by ","')
    parser.add_option('--prefix',dest='prefix',help='table prefix')
    parser.add_option('-p',dest='partition', help='partition')
    options,args=parser.parse_args()

    if not options.host:
        print "mysql ip must the one have a value"
        sys.exit(1)

    if not options.user:
        print "mysql user must the one have a value"
        sys.exit(1)

    if not options.password:
        print "mysql password must the one have a value"
        sys.exit(1)

    if not options.dbnanme:
        print "mysql default db name must the one have a value"
        sys.exit(1)
    if not options.predbnanme:
        print "mysql default predb name must the one have a value"
        sys.exit(1)

    if not options.port:
        print "mysql host port default is 3306"
        sys.exit(1)

    if not options.tables:
        print "mysql tables must get least one "
        sys.exit(1)
    create_sql(options)