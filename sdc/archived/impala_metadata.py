import time
from impala.dbapi import connect
import sys
def main(HOST, port,database):
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+' INVALIDATE METADATA start'
    conn = connect(host=HOST, port=port,database=None)
    cur = conn.cursor()
    cur.execute('INVALIDATE METADATA')
    cur.close()
    conn.close()
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+' INVALIDATE METADATA finished'
    # print 'INVALIDATE METADATA'
    # a=cur.fetchall()
    # print a

if __name__ == '__main__':
    host=sys.argv[1] # '10.11.0.181'
    port=sys.argv[2] #'21050'
    database=sys.argv[3] if len(sys.argv) >=4 else 'default'
    main(host, int(port), database)