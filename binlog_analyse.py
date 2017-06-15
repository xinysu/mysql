import pymysql
from pymysql.cursors import DictCursor
import re
import os
import sys
import datetime
import time
import logging
import importlib
importlib.reload(logging)
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(levelname)s %(message)s ')

usage=''' usage: python [script's path] [option]
ALL options need to assign:

-h    : host, the database host，which database will store the results after analysis
-u    : user, the db user
-p    : password, the db user's password
-P    : port, the db port
-f    : file path, the binlog file
-tr    : table name for record , the table name to store the row record
-tt    : table name for transaction, the table name to store transactions
Example: python queryanalyse.py -h=127.0.0.1 -P=3310 -u=root -p=password -f=/tmp/stock_binlog.log -tt=flashback.tbtran -tr=flashback.tbrow

'''

class queryanalyse:
    def __init__(self):
        #初始化
        self.host=''
        self.user=''
        self.password=''
        self.port='3306'
        self.fpath=''
        self.tbrow=''
        self.tbtran=''

        self._get_db()
        logging.info('assign values to parameters is done:host={},user={},password=***,port={},fpath={},tb_for_record={},tb_for_tran={}'.format(self.host,self.user,self.port,self.fpath,self.tbrow,self.tbtran))

        self.mysqlconn = pymysql.connect(host=self.host, user=self.user, password=self.password, port=self.port,charset='utf8')
        self.cur = self.mysqlconn.cursor(cursor=DictCursor)
        logging.info('MySQL which userd to store binlog event connection is ok')

        self.begin_time=''
        self.end_time=''
        self.db_name=''
        self.tb_name=''

    def _get_db(self):
        #解析用户输入的选项参数值，这里对password的处理是明文输入，可以自行处理成是input格式，
        #由于可以拷贝binlog文件到非线上环境分析，所以password这块，没有特殊处理
        logging.info('begin to assign values to parameters')
        if len(sys.argv) == 1:
            print(usage)
            sys.exit(1)
        elif sys.argv[1] == '--help':
            print(usage)
            sys.exit()
        elif len(sys.argv) > 2:
            for i in sys.argv[1:]:
                _argv = i.split('=')
                if _argv[0] == '-h':
                    self.host = _argv[1]
                elif _argv[0] == '-u':
                    self.user = _argv[1]
                elif _argv[0] == '-P':
                    self.port = int(_argv[1])
                elif _argv[0] == '-f':
                    self.fpath = _argv[1]
                elif _argv[0] == '-tr':
                    self.tbrow = _argv[1]
                elif _argv[0] == '-tt':
                    self.tbtran = _argv[1]
                elif _argv[0] == '-p':
                    self.password = _argv[1]
                else:
                    print(usage)

    def create_tab(self):
        #创建两个表格：一个用户存储事务情况，一个用户存储每一行数据修改的情况
        #注意，一个事务可以存储多行数据修改的情况
        logging.info('creating table ...')
        create_tb_sql ='''CREATE TABLE IF NOT EXISTS  {} (
                          `auto_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
                          `begin_time` datetime NOT NULL,
                          `end_time` datetime NOT NULL,
                          PRIMARY KEY (`auto_id`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
                        CREATE TABLE IF NOT EXISTS  {} (
                          `auto_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
                          `sqltype` int(11) NOT NULL COMMENT '1 is insert,2 is update,3 is delete',
                          `tran_num` int(11) NOT NULL COMMENT 'the transaction number',
                          `dbname` varchar(50) NOT NULL,
                          `tbname` varchar(50) NOT NULL,
                          PRIMARY KEY (`auto_id`),
                          KEY `sqltype` (`sqltype`),
                          KEY `dbname` (`dbname`),
                          KEY `tbname` (`tbname`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
                        truncate table {};
                        truncate table {};
                        '''.format(self.tbtran,self.tbrow,self.tbtran,self.tbrow)

        self.cur.execute(create_tb_sql)
        logging.info('created table {} and {}'.format(self.tbrow,self.tbtran))

    def rowrecord(self):
        #处理每一行binlog
        #事务的结束采用 'Xid =' 来划分
        #分析结果，按照一个事务为单位存储提交一次到db
        try:
            tran_num=1    #事务数
            record_sql='' #行记录的insert sql
            tran_sql=''  #事务的insert sql

            self.create_tab()

            with open(self.fpath,'r') as binlog_file:
                logging.info('begining to analyze the binlog file ,this may be take a long time !!!')
                logging.info('analyzing...')

                for bline in binlog_file:

                    if bline.find('Table_map:') != -1:
                        l = bline.index('server')
                        n = bline.index('Table_map')
                        begin_time = bline[:l:].rstrip(' ').replace('#', '20')

                        self.begin_time = begin_time[0:4] + '-' + begin_time[4:6] + '-' + begin_time[6:]
                        self.db_name = bline[n::].split(' ')[1].replace('`', '').split('.')[0]
                        self.tb_name = bline[n::].split(' ')[1].replace('`', '').split('.')[1]
                        bline=''

                    elif bline.startswith('### INSERT INTO'):
                      record_sql=record_sql+"insert into {}(sqltype,tran_num,dbname,tbname) VALUES (1,{},'{}','{}');".format(self.tbrow,tran_num,self.db_name,self.tb_name)

                    elif bline.startswith('### UPDATE'):
                      record_sql=record_sql+"insert into {}(sqltype,tran_num,dbname,tbname) VALUES (2,{},'{}','{}');".format(self.tbrow,tran_num,self.db_name,self.tb_name)

                    elif bline.startswith('### DELETE FROM'):
                      record_sql=record_sql+"insert into {}(sqltype,tran_num,dbname,tbname) VALUES (3,{},'{}','{}');".format(self.tbrow,tran_num,self.db_name,self.tb_name)

                    elif bline.find('Xid =') != -1:

                        l = bline.index('server')
                        end_time = bline[:l:].rstrip(' ').replace('#', '20')
                        self.end_time = end_time[0:4] + '-' + end_time[4:6] + '-' + end_time[6:]
                        tran_sql=record_sql+"insert into {}(begin_time,end_time) VALUES ('{}','{}')".format(self.tbtran,self.begin_time,self.end_time)

                        self.cur.execute(tran_sql)
                        self.mysqlconn.commit()
                        record_sql = ''
                        tran_num += 1

        except Exception:
            return 'funtion rowrecord error'

    def binlogdesc(self):
        sql=''
        t_num=0
        r_num=0
        logging.info('Analysed result printing...\n')
        #分析总的事务数跟行修改数量
        sql="select 'tbtran' name,count(*) nums from {}  union all select 'tbrow' name,count(*) nums from {};".format(self.tbtran,self.tbrow)
        self.cur.execute(sql)
        rows=self.cur.fetchall()
        for row in rows:
            if row['name']=='tbtran':
                t_num = row['nums']
            else:
                r_num = row['nums']
        print('This binlog file has {} transactions, {} rows are changed '.format(t_num,r_num))

        # 计算 最耗时 的单个事务
        # 分析每个事务的耗时情况,分为5个时间段来描述
        # 这里正常应该是 以毫秒来分析的，但是binlog中，只精确时间到second
        sql='''select
                      count(case when cost_sec between 0 and 1 then 1 end ) cos_1,
                      count(case when cost_sec between 1.1 and 5 then 1 end ) cos_5,
                      count(case when cost_sec between 5.1 and 10 then 1 end ) cos_10,
                      count(case when cost_sec between 10.1 and 30 then 1 end ) cos_30,
                      count(case when cost_sec >30.1 then 1 end ) cos_more,
                      max(cost_sec) cos_max
                from
                (
                        select
                            auto_id,timestampdiff(second,begin_time,end_time) cost_sec
                        from {}
                ) a;'''.format(self.tbtran)
        self.cur.execute(sql)
        rows=self.cur.fetchall()

        for row in rows:
            print('The most cost time : {} '.format(row['cos_max']))
            print('The distribution map of each transaction costed time: ')
            print('Cost time between    0 and  1 second : {} , {}%'.format(row['cos_1'],int(row['cos_1']*100/t_num)))
            print('Cost time between  1.1 and  5 second : {} , {}%'.format(row['cos_5'], int(row['cos_5'] * 100 / t_num)))
            print('Cost time between  5.1 and 10 second : {} , {}%'.format(row['cos_10'], int(row['cos_10'] * 100 / t_num)))
            print('Cost time between 10.1 and 30 second : {} , {}%'.format(row['cos_30'], int(row['cos_30'] * 100 / t_num)))
            print('Cost time                    > 30.1 : {} , {}%\n'.format(row['cos_more'], int(row['cos_more'] * 100 / t_num)))

        # 计算 单个事务影响行数最多 的行数量
        # 分析每个事务 影响行数 情况,分为5个梯度来描述
        sql='''select
                    count(case when nums between 0 and 10 then 1 end ) row_1,
                    count(case when nums between 11 and 100 then 1 end ) row_2,
                    count(case when nums between 101 and 1000 then 1 end ) row_3,
                    count(case when nums between 1001 and 10000 then 1 end ) row_4,
                    count(case when nums >10001 then 1 end ) row_5,
                    max(nums) row_max
              from
                  (
                    select
                            count(*) nums
                    from {} group by tran_num
                  ) a;'''.format(self.tbrow)
        self.cur.execute(sql)
        rows=self.cur.fetchall()

        for row in rows:
            print('The most changed rows for each row: {} '.format(row['row_max']))
            print('The distribution map of each transaction changed rows : ')
            print('Changed rows between    1 and    10 second : {} , {}%'.format(row['row_1'],int(row['row_1']*100/t_num)))
            print('Changed rows between  11 and  100 second : {} , {}%'.format(row['row_2'], int(row['row_2'] * 100 / t_num)))
            print('Changed rows between  101 and  1000 second : {} , {}%'.format(row['row_3'], int(row['row_3'] * 100 / t_num)))
            print('Changed rows between 1001 and 10000 second : {} , {}%'.format(row['row_4'], int(row['row_4'] * 100 / t_num)))
            print('Changed rows                      > 10001 : {} , {}%\n'.format(row['row_5'], int(row['row_5'] * 100 / t_num)))

        # 分析 各个行数 DML的类型情况
        # 描述 delete，insert，update的分布情况
        sql='select sqltype ,count(*) nums from {} group by sqltype ;'.format(self.tbrow)
        self.cur.execute(sql)
        rows=self.cur.fetchall()

        print('The distribution map of the {} changed rows : '.format(r_num))
        for row in rows:

            if row['sqltype']==1:
                print('INSERT rows :{} , {}% '.format(row['nums'],int(row['nums']*100/r_num)))
            if row['sqltype']==2:
                print('UPDATE rows :{} , {}% '.format(row['nums'],int(row['nums']*100/r_num)))
            if row['sqltype']==3:
                print('DELETE rows :{} , {}%\n '.format(row['nums'],int(row['nums']*100/r_num)))

        # 描述 影响行数 最多的表格
        # 可以分析是哪些表格频繁操作，这里显示前10个table name
        sql = '''select
                      dbname,tbname ,
                      count(*) ALL_rows,
                      count(*)*100/{} per,
                      count(case when sqltype=1 then 1 end) INSERT_rows,
                      count(case when sqltype=2 then 1 end) UPDATE_rows,
                      count(case when sqltype=3 then 1 end) DELETE_rows
                from {}
                group by dbname,tbname
                order by ALL_rows desc
                limit 10;'''.format(r_num,self.tbrow)
        self.cur.execute(sql)
        rows = self.cur.fetchall()

        print('The distribution map of the {} changed rows : '.format(r_num))
        print('tablename'.ljust(50),
              '|','changed_rows'.center(15),
              '|','percent'.center(10),
              '|','insert_rows'.center(18),
              '|','update_rows'.center(18),
              '|','delete_rows'.center(18)
              )
        print('-------------------------------------------------------------------------------------------------------------------------------------------------')
        for row in rows:
            print((row['dbname']+'.'+row['tbname']).ljust(50),
                  '|',str(row['ALL_rows']).rjust(15),
                  '|',(str(int(row['per']))+'%').rjust(10),
                  '|',str(row['INSERT_rows']).rjust(10)+' , '+(str(int(row['INSERT_rows']*100/row['ALL_rows']))+'%').ljust(5),
                  '|',str(row['UPDATE_rows']).rjust(10)+' , '+(str(int(row['UPDATE_rows']*100/row['ALL_rows']))+'%').ljust(5),
                  '|',str(row['DELETE_rows']).rjust(10)+' , '+(str(int(row['DELETE_rows']*100/row['ALL_rows']))+'%').ljust(5),
                  )
        print('\n')

        logging.info('Finished to analyse the binlog file !!!')

    def closeconn(self):
        self.cur.close()
        logging.info('release db connections')

def main():
    p = queryanalyse()
    p.rowrecord()
    p.binlogdesc()
    p.closeconn()

if __name__ == "__main__":
    main()
