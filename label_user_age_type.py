# -*- coding:utf-8 -*-
import urllib2
import urllib
import requests
import json
import datetime
from urllib import quote
import hashlib
import pandas as pd
import MySQLdb
import os
from tqdm import tqdm
import sys
import numpy
reload(sys)
sys.setdefaultencoding('utf-8')

###读取uuid
def user_uuid_get(date):
    conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
    sql = "select uuid from combine_%s " %date
    print 'start to get uuid daily data %s'%date
    try:
        uuid = pd.read_sql(sql,conn)
        print 'get uuid data %s success!'%date
    except:
        uuid = pd.DataFrame()
        print 'get uuid data %s fail!'%date
    return uuid



###读取uuid和birthday
def user_birthday_get(date,uuid_str):
    conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',port=3306,db='live_user',charset='utf8')
    sql = "select uuid,birthday from new_user where uuid in (%s) " %uuid_str
    print 'start to get uuid,birthday daily data %s'%date
    try:
        uuid_birthday = pd.read_sql(sql,conn)
        print 'get uuid,birthday data %s success!'%date
    except  MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[ 0 ], e.args[ 1 ])
        uuid_birthday = pd.DataFrame()
        print 'get uuid,birthday %s fail!'%date
    return uuid_birthday

####建立用户标签总表
def  label_create(date_str):
    try:
        conn = MySQLdb.connect(host='10.100.3.64', user='hefang', passwd='NYH#dfjirD872C$d&ss',db='live_user',port=3306, charset='utf8')
        #conn.select_db('live_user')
        cur = conn.cursor()
        cur = conn.cursor()
        cur.execute('drop table if exists label;')
        cur.execute('create table label (`date` int(8),uuid varchar(50),label_id varchar(10),label_value varchar(10))ENGINE=MERGE;')
        cur.execute('alter table label add index label_index (`date`);')
        cur.execute('alter table label union=(%s);' % date_str)
        conn.commit()
        cur.close
        conn.close()
        print "label update"
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[ 0 ], e.args[ 1 ])
        print "update label fail"
        pass
# 将校验数据的信息写入文件
def write_checkinfo(check_date, orignal_rows, success_rows, percentage):
    file_path = '/root/hf/live_user/label'  # 存放检验文本的目录
    if not os.path.exists(file_path):
        os.mkdir(file_path)
    file_name = '/root/hf/live_user/label/label_user_age_type_check.txt'  # 检验文本的名称
    f = open(file_name, 'a')
    print "start write checkfile"
    f.write(str(check_date) + '\t\t' + str(orignal_rows) + '\t\t' + str(success_rows) + '\t\t' + str( '%.5f%%' % percentage) + '\n')
    print "write checkfile success"
    f.close()

###插入用户类型标签
def label_user_type_daily_insert(conn,cur,date,label_insert):
    error_path = '/root/hf/live_user/label'  #
    if not os.path.exists(error_path):
        os.mkdir(error_path)
    insert_tag = 1  ###插入成功表示1
    try:
        sql = 'insert into label_'+date+' values('+','.join(map(lambda o: "%s",range(0,4)))+')'
        cur.executemany(sql,label_insert)
        conn.commit()
    except MySQLdb.Error,e:
        insert_tag = 0  ###插入失败表示0
        error_insertlog_path = '/root/hf/live_user/label/label_user_age_type_error_' + date + ".txt"  # 存放插入错误的日志信�?
        f = open(error_insertlog_path, 'a')
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        f.write("Mysql Error %d: %s,%s" % ( e.args[0], e.args[1],label_insert) + "\n")
        f.close()
        pass
    return insert_tag
#获取前一天的日期
def day_get(d):
    oneday = datetime.timedelta(days=1)
    day = d - oneday
    date_end=datetime.date(int(day.year),int(day.month),int(day.day))
    return date_end

###获取从开始日期到现在的日期间�?
def datelist(start_date,end_date):
    result = []
    curr_date = start_date
    while curr_date != end_date:
        result.append("%04d%02d%02d" % (curr_date.year, curr_date.month, curr_date.day))
        curr_date += datetime.timedelta(1)
    result.append("%04d%02d%02d" % (curr_date.year, curr_date.month, curr_date.day))
    result_1 = list()
    for i in range(len(result)):
        result_1.append('label_'+result[i])
    datestr = ','.join(result_1)
    return datestr


###获取从开始日期到现在的日期列表，日期的格式为yyyy,mm,dd
def datelist_new(start_date,end_date):
    result = []
    curr_date = start_date
    while curr_date != end_date:
        result.append("%04d,%02d,%02d" % (curr_date.year, curr_date.month, curr_date.day))
        curr_date += datetime.timedelta(1)
    result.append("%04d,%02d,%02d" % (curr_date.year, curr_date.month, curr_date.day))
    return result

###计算年龄标签，返回插入到数据库中的格式
def insert_label_datas(date,uuid_str):
    uuid_birthday = user_birthday_get(date,uuid_str)
    uuid_birthday_length = len(uuid_birthday)
    label_id=3
    insert_list = list()
    if uuid_birthday_length > 0:
        for i in range(uuid_birthday_length):
            data_everyrow=list()
            uuid_birthday_target = uuid_birthday.loc[ i ]
            try:
                uuid = uuid_birthday_target[ 'uuid' ]
                birthday = uuid_birthday_target[ 'birthday' ]
                if birthday=='' or  birthday==None:
                    label_value = 0
                else:
                    birthday_info = birthday.split('-')
                    birthday_num = "%04d%02d%02d" % (int(birthday_info[ 0 ]), int(birthday_info[ 1 ]), int(birthday_info[ 2 ]))
                    birthday_num=int(birthday_num)
                    if 20160101>birthday_num>20000101:
                        label_value=1
                    elif 19991231>birthday_num>19950101:
                        label_value = 2
                    elif 19941231>birthday_num>19900101:
                        label_value = 3
                    elif 19891231>birthday_num>19850101:
                        label_value = 4
                    elif 19841231>birthday_num>19800101:
                        label_value = 5
                    elif 19791231>birthday_num>19700101:
                        label_value = 6
                    elif birthday_num>20160101 or birthday_num<19691231:
                        label_value = -1
                data_everyrow.extend((date,uuid,label_id,label_value))
                insert_list.append(data_everyrow)
            except:
                print "get birthday fails"
    return insert_list



if __name__ == '__main__':
    # 获取当前时间
    start_date = datetime.date(2016,8,11)  ###总表数据统计开始时间
    d = datetime.datetime.now()
    end_date = day_get(d)  ##当前日期的前一天
    date_list = datelist_new(start_date,end_date);
    for i in range(1,21):
    #for i in range(20,len(date_list)):
        date_info = date_list[ i ].split(',')
        end_date = datetime.date(int(date_info[ 0 ]), int(date_info[ 1 ]), int(date_info[ 2 ]))  ###本次数据插入时间
        date = end_date.strftime('%Y%m%d')  ###本次数据插入时间格式�?
        label_date_str = datelist(start_date,end_date)  # 获得日期列表
        user_uuid=user_uuid_get(date)
        ##获取用户uuid
        length=len(user_uuid)
        if length>0:
            print "%s get user_type info data success" %date
            print "%s start to insert data into label" %date
            try:
                conn = MySQLdb.connect(host='10.100.3.64', user='hefang', passwd='NYH#dfjirD872C$d&ss', port=3306,db='live_user', charset='utf8')
                cur = conn.cursor()
                length_list = 10000 ####每一万行一插
                insert_success=0 #####记录插入成功
                length_split=(length-1)/length_list+1
                for j in tqdm(range(length_split)):
                    label_insert=list()
                    uuid_list=list()
                    if j<length_split-1:
                        xrange_length = length_list
                    elif j==length_split-1:
                        xrange_length=length-length_list*j
                    for k in xrange(xrange_length):
                        ##初始化每行数据
                        data_everyrow=list()
                        k_loc=j*10000+k
                        label_target=user_uuid.loc[k_loc]
                        try:
                            uuid=label_target['uuid']
                            ##将uuid转化为‘uuid’形式，符合sql查询规则
                            uuid_new="'"+uuid+"'"
                            uuid_list.append(uuid_new)
                        except:
                            print "get uuid,vv_num,mglive_vv_num fails"
                    ##将一万个uuid列表转为字符串
                    uuid_str = ",".join(uuid_list)
                    label_insert=insert_label_datas(date,uuid_str)
                    ###每一万行插入到label中
                    if len(label_insert)!=0:
                        insert_tag=label_user_type_daily_insert(conn,cur,date,label_insert)
                        if insert_tag==1:
                            insert_success+=xrange_length
                        else:
                           pass
                    else:
                        print "have no data to insert"
                ### 将校验数据信息写入文件中
                percentage = insert_success / float(length) * 100  ####成功的百分比
                write_checkinfo(date,length, insert_success, percentage)
                cur.close
                conn.close()
                print 'insert into label for user type daily %s data success' % date
            except MySQLdb.Error,e:
                print "Mysql Error %d: %s" % (e.args[ 0 ], e.args[ 1 ])
                print 'insert into label for user type daily %s data fails' % date
            # label_create(date_str=label_date_str)  ###更新当日的汇总表
        else:
            print "%s have no user_type_info"%date

