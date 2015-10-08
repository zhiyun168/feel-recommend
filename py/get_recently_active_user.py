# -*-: utf-8 -*-
# @Author: zhihang.fan@zhiyun168.com

import redis
import time
import datetime

def get_recently_active_user(day):

	last = day - 1
	today = datetime.datetime.now()
	threshold_day = today + datetime.timedelta(days = -day)
	threshold_timestamp = time.mktime(time.strptime(threshold_day.strftime("%Y%m%d"), "%Y%m%d"))
#threshold_timestamp = time.mktime(time.strptime(str(threshold_day).split('.')[0], "%Y-%m-%d %H:%M:%S"))
#print threshold_timestamp
	threshold_day_start = today + datetime.timedelta(days = -last)
	threshold_timestamp_start = time.mktime(time.strptime(threshold_day_start.strftime("%Y%m%d"), "%Y%m%d"))
#	threshold_timestamp_start = time.mktime(time.strptime(str(threshold_day_start).split('.')[0], "%Y-%m-%d %H:%M:%S"))
	try:
		r = redis.StrictRedis(host = '172.31.12.252', port = 6379)
		output_file = '../data/recently_active_user_file'
		file = open(output_file, 'w')
		for key, value in r.hscan_iter('feel:login'):
			if float(value) > threshold_timestamp and float(value) < threshold_timestamp_start:
				file.write(key + '\t' + value + '\n')
		file.close()
		return True
	except Exception, e:
		print e
		return False

if __name__ == '__main__':
	get_recently_active_user(1)
