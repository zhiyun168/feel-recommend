# -*-: coding: utf-8 -*-

import os
import time
import redis
import logging
import datetime
from test_get_data import GetMySQLData
from get_recently_active_user import get_recently_active_user

dst_dir = os.path.split(os.path.realpath(__file__))[0]
os.chdir(dst_dir)

if __name__ == '__main__':
    try:
        logging.basicConfig(level = logging.DEBUG,
                            format = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt = '%a, %d %b %Y %H:%M:%S',
                            filename = '../log/recovery.log',
                            filemode = 'a')

        getMySQLData = GetMySQLData()

        for i in range(3, 0, -1):
            getMySQLData.set_table('user')
            getMySQLData.set_field('data_from,sex')
            getMySQLData.set_ts_field('reg_time')
            getMySQLData.set_output_file('../data/test_get_data')
            getMySQLData.set_date(i)
            getMySQLData.scan_table()
            logging.info('different gender, platform register in 24h and total numbers register in 24h of ' + str(i) + ' day(s)')
            lastDay = (datetime.datetime.now() - datetime.timedelta(days = (1 + i)))
            otherStyleTime = lastDay.strftime("%Y%m%d")
            print otherStyleTime

            os.system('rm -r ../data/' + otherStyleTime)
            os.system('bash ../sh/Mytest.sh ' + str(i + 1))

            getMySQLData.set_table('user join card')
            getMySQLData.set_field('user.id,sex,data_from,type,status^is_del')
            getMySQLData.set_ts_field('user.id=card.uid')
            getMySQLData.set_ts('reg_time')
            getMySQLData.set_ts_create('created')
            getMySQLData.set_date(i)
            getMySQLData.set_output_file('../data/get_usercard_data')
            getMySQLData.scan_table_platform_gender()
            logging.info('get the table user join card of ' + str(i))

            os.system('bash ../sh/solve_card.sh ' + str(i + 1))

            get_recently_active_user(i + 1)


            for j in range(0, 4):
                getMySQLData.set_table('user')
                getMySQLData.set_field('id,sex,data_from')
                getMySQLData.set_ts('reg_time')
                getMySQLData.set_date(i)
                getMySQLData.scan_table_recent_active(j,'../data/' + str(j) + 'days_ago')
            os.system('bash ../sh/active_user_3days.sh ' + str(i + 2) + ' '  + str(i + 3) + ' ' + str(i + 4))

            os.system('bash ../sh/noStop.sh ' + str(i + 1))
            logging.info('cal no stop user of ' + str(i))

    except Exception, e:
        logging.debug('Exception info ' + str(e))


