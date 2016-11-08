# -*- coding:utf-8 -*-

"""
 @author:csp
 @file label_cycle_new.py
 @2016 11 02 11:30
"""
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
reload(sys)
sys.setdefaultencoding('utf-8')

###读取当日的uuid
def uuid_get(date):
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

###加密签名
def make_sign(data,secret_key):
    string = ''
    keys = data.keys()
    keys.sort()
    for i in keys:
        string += '%s=%s&'%(i,data[i])
    string = (string + 'secret_key=' + secret_key).lower()
    return hashlib.sha1(string.lower()).hexdigest().lower()
###通过接口获取用户信息
def uuid_info_get(date,uuid_str):
    data = dict()
    data['uip'] = '10.32.0.100'
    data['uuid'] = uuid_str
    data['from'] = '123'
    datajson = json.dumps(data)
    http_data = {'invoker':'pc','data':datajson}
    secret_key = '&^khiwf*#%1'
    sign = make_sign(http_data,secret_key)
    url = 'http://idp.hifuntv.com/in/GetUserInfoByUuid?invoker=pc&data=%s&sign=%s' % (quote(datajson), sign)
    resp = urllib2.urlopen(url)
    content = resp.read()
    msg = json.loads(content)['msg']
    uuid_info= list()###用户信息列表
    if type(msg)==dict:
           try:
            uuid_info = uuid_dict_process(date,msg)
           except:
               print uuid_str
               pass
    else:
        print msg
        pass
    return uuid_info
###处理用户信息dict
def uuid_dict_process(date,uuid_dict):
    uuid_info = list()
    ##判断是否�?在uuid，如果不在不添加内容
    if uuid_dict.has_key('uuid'):
        uuid = uuid_dict['uuid']
        label_id = '1'               ###代表是否为ip的标�?
        label_value = uuid_dict['isVip'] if uuid_dict.has_key('isVip') else ''  ###0表示不是vip  1表示是vip
        uuid_info.extend((date,uuid,label_id,label_value))
    else:
        pass
    return uuid_info
        # birthday = uuid_dict['birthday'] if uuid_dict.has_key('birthday') else ''
        # sex = uuid_dict['sex'] if uuid_dict.has_key('sex') else ''
        # province = uuid_dict['province'] if uuid_dict.has_key('province') else ''
        # city = uuid_dict['city'] if uuid_dict.has_key('city') else ''
        # nickname = uuid_dict['nickname'] if uuid_dict.has_key('nickname') else ''
        # mobile = uuid_dict['mobile'] if uuid_dict.has_key('mobile') else ''
        # vipExpiretime = uuid_dict['vipExpiretime'] if uuid_dict.has_key('vipExpiretime') else ''
        ####表示的是vip标签

###建立每日用户的标签表
def label_daily_create(date):
    try:
        conn=MySQLdb.connect(host='10.100.3.64',user='hefang',passwd='NYH#dfjirD872C$d&ss',db='live_user',port=3306,charset='utf8')
        #conn.select_db('live_user')
        cur=conn.cursor()
        cur = conn.cursor()
        cur.execute('drop table if exists label_%s;' % date)
        cur.execute('create table label_%s (`date`int(8),uuid varchar(50),label_id varchar(10),label_value varchar(10))ENGINE=MyISAM;' % date)
        cur.execute('alter table label_%s add index label_index_%s (`date`)' % (date,date))
        conn.commit()
        cur.close
        conn.close()
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[ 0 ], e.args[ 1 ])
        pass

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

###插入每日新增用户的标�?
def label_daily_insert(conn,cur,date,label_insert):
    error_path = '/root/hf/live_user/label'  #
    if not os.path.exists(error_path):
        os.mkdir(error_path)
    insert_tag = 1  ###插入成功表示1
    try:
        sql = 'insert into label_'+date+' values('+','.join(map(lambda o: "%s",range(0,4)))+')'
        cur.execute(sql,label_insert)
        conn.commit()
    except MySQLdb.Error,e:
        insert_tag = 0  ###插入失败表示0
        error_insertlog_path = '/root/hf/live_user/label/label_error_' + date + ".txt"  # 存放插入错误的日志信�?
        f = open(error_insertlog_path, 'a')
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        f.write("Mysql Error %d: %s,%s" % ( e.args[0], e.args[1],label_insert) + "\n")
        f.close()
        pass
    return insert_tag

###存放取不到用户信息的uuid信息
def uuid_error_info(date, uuid):
    error_path = '/root/hf/live_user/label'  # 存放new_user插入信息的表
    if not os.path.exists(error_path):
        os.mkdir(error_path)
    error_insertlog_path = '/root/hf/live_user/label/uuid_error_' + date + '.txt'  # 存放错误的uuid的文件名�?
    f = open(error_insertlog_path, 'a')
    f.write(uuid + "\n")
    f.close()

# 将校验数据的信息写入文件�?
def write_checkinfo(check_date, orignal_rows, success_rows, percentage):
    file_path = '/root/hf/live_user/label'  # 存放检验文本的目录
    if not os.path.exists(file_path):
        os.mkdir(file_path)
    file_name = '/root/hf/live_user/label/label_check.txt'  # 检验文本的名称
    f = open(file_name, 'a')
    print "start write checkfile"
    f.write(str(check_date) + '\t\t' + str(orignal_rows) + '\t\t' + str(success_rows) + '\t\t' + str(
        '%.5f%%' % percentage) + '\n')
    print "write checkfile success"
    f.close()
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

###获取取得uuid数据
def new_uuid_list(uuid_insert):
    new_uuids=list()
    for i in range(len(uuid_insert)):
        new_uuids.append(uuid_insert[i][1])
    return new_uuids


if __name__ == '__main__':
    # 获取当前时间
    start_date = datetime.date(2016,8,11)  ###总表数据统计开始时�?
    d = datetime.datetime.now()
    end_date = day_get(d)  ##当前日期的前一�?
    date_list=datelist_new(start_date,end_date);
    #for i in range(0,1):
    for i in range(1,len(date_list)):
        date_info = date_list[ i ].split(',')
        end_date = datetime.date(int(date_info[ 0 ]), int(date_info[ 1 ]), int(date_info[ 2 ]))  ###本次数据插入时间
        date = end_date.strftime('%Y%m%d')  ###本次数据插入时间格式�?
        label_date_str = datelist(start_date,end_date)  # 获得日期列表
        uuid_data = uuid_get(date)###获取combine中的uuid
        length = len(uuid_data)
        print length
        if length > 0:
            print 'start insert %s label  daily  into database' % date
            label_daily_create(date)
            try:
                conn = MySQLdb.connect(host='10.100.3.64', user='hefang', passwd='NYH#dfjirD872C$d&ss', port=3306, db='live_user', charset='utf8')
                cur = conn.cursor()
                length_list = 50  ###50行插入一次，50的限制是接口限制
                length_split = (length - 1) / length_list + 1  ###将数据分段，5行为一�?
                insert_success = 0  ###插入成功次数
                for j in tqdm(range(length)):
                    uuid_target = uuid_data.loc[j][ 'uuid' ]
                    #uuid_list.append(uuid_target)
                    #uuid_str = '|'.join(uuid_list)  ###传入的参数需要以|进行分割
                    uuid_insert = uuid_info_get(date,uuid_target)
                    ##输出没有获得用户信息的uuid
                    ##new_uuids = new_uuid_list(uuid_insert)
                    # diff_uuids = list(set(uuid_list).difference(set(new_uuids)))
                    # if len(diff_uuids) != 0:
                    #     print diff_uuids
                    if len(uuid_insert) != 0:
                        insert_tag = label_daily_insert(conn,cur,date,uuid_insert)
                        if insert_tag == 1:
                            insert_success += 1  # 记录�?功插入的条数
                        else:
                             pass
                    else:
                        ##将未获取用户信息的uuid写入到文件中
                        uuid_error_info(date,uuid_target)
                ### 将校验数据信息写入文件中
                percentage = insert_success / float(length) * 100  ####成功的百分比
                write_checkinfo(date,length,insert_success, percentage)
                cur.close
                conn.close()
                print 'insert into label daily %s data success' %date
            except:
                print 'insert into label daily %s data fails'  %date
            label_create(date_str=label_date_str) ###更新当日的汇总表
        else:
            print "get uuid fali"
            pass
