# -*-: coding: utf-8 -*-

import os
import time
import redis
import logging
import datetime
from test_get_data import GetMySQLData

dst_dir = os.path.split(os.path.realpath(__file__))[0]
os.chdir(dst_dir)

if __name__ == '__main__':
    try:
        logging.basicConfig(level = logging.DEBUG,
                            format = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt = '%a, %d %b %Y %H:%M:%S',
                            filename = '../log/Mytest.log',
                            filemode = 'a')

        getMySQLData = GetMySQLData()
        getMySQLData.set_table('user')
        getMySQLData.set_field('data_from,sex')
        getMySQLData.set_ts_field('reg_time')
        getMySQLData.set_output_file('../data/test_get_data')
        getMySQLData.scan_table()
        logging.info('different gender, platform register in 24h and total numbers register in 24h')
        lastDay = (datetime.datetime.now() - datetime.timedelta(days = 1))
        otherStyleTime = lastDay.strftime("%Y%m%d")
        print otherStyleTime

        os.system('bash ../sh/Mytest.sh ')

        getMySQLData.set_table('user join card')
        getMySQLData.set_field('user.id,sex,data_from,type,status^is_del')
        getMySQLData.set_ts_field('user.id=card.uid')
        getMySQLData.set_ts('reg_time')
        getMySQLData.set_output_file('../data/get_usercard_data')
        getMySQLData.scan_table_platform_gender()
        logging.info('get the table user join card')

        os.system('bash ../sh/solve_card.sh ')

        os.system('python get_recently_active_user.py') 

        for i in range(1, 4):
            getMySQLData.set_table('user')
            getMySQLData.set_field('id,sex,data_from')
            getMySQLData.set_ts('reg_time')
            getMySQLData.scan_table_recent_active(i,'../data/' + str(i) + 'days_ago')
        os.system('bash ../sh/active_user_3days.sh')
    except Exception, e:
        logging.debug('Exception info ' + str(e))


