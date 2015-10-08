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

        os.system('bash ../sh/Mytest.sh 1')

        getMySQLData.set_table('user join card')
        getMySQLData.set_field('user.id,sex,data_from,type,status^is_del')
        getMySQLData.set_ts_field('user.id=card.uid')
        getMySQLData.set_ts('reg_time')
        getMySQLData.set_ts_create('created')
        getMySQLData.set_output_file('../data/get_usercard_data')
        getMySQLData.scan_table_platform_gender()
        logging.info('get the table user join card')

        os.system('bash ../sh/solve_card.sh 1')

        os.system('python get_recently_active_user.py') 

        for i in range(0, 4):
            getMySQLData.set_table('user')
            getMySQLData.set_field('id,sex,data_from')
            getMySQLData.set_ts('reg_time')
            getMySQLData.scan_table_recent_active(i,'../data/' + str(i) + 'days_ago')
        os.system('bash ../sh/active_user_3days.sh 2 3 4')

        os.system('bash ../sh/noStop.sh 1')

        logging.info('send email')
        os.system('bash ../sh/merge_active_total.sh')
        os.system('bash ../sh/merge_active_gender.sh')
        os.system('bash ../sh/merge_active_platform.sh')
        os.system('bash ../sh/merge_active_GP.sh')
        Day3 = (datetime.datetime.now() - datetime.timedelta(days = 4))
        Time = Day3.strftime("%Y%m%d")
        print Time
        os.system('cat ../data/' + str(Time) + '/gender/part-0000* ../data/' + str(Time) + '/platform/part-0000* ../data/' + str(Time) + '/card_result/part-0000* ../data/blank ../data/' + str(Time) + '/active_total/part-0000* ../data/blank ../data/' + str(Time) + '/active_gender/part-0000* ../data/blank ../data/' + str(Time) + '/active_platform/part-0000* ../data/blank ../data/' + str(Time) + '/active_gen_plat/part-0000* ../data/blank ../data/' + str(Time) + '/post_card/part-0000* ../data/blank ../data/' + str(Time) + '/gender_card/part-0000* | python send_email.py')
    except Exception, e:
        logging.debug('Exception info ' + str(e))


