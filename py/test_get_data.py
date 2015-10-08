# -*-: coding: utf-8 -*-
import sys
import MySQLdb
import time
import datetime

reload(sys)
sys.setdefaultencoding('utf-8')

class GetMySQLData(object):

    def __init__(self):
        self.table = ''
        self.field = ''
        self.ts_field = ''
        self.ts_create = ''
        self.ts = ''
        self.output_file = ''
        lastDay = (datetime.datetime.now() - datetime.timedelta(days = 1))
        a = lastDay.strftime("%Y%m%d")
        timeArray = time.strptime(a, "%Y%m%d")
        self.t = int(time.mktime(timeArray))
        self.tt = self.t
        self.currentTime = str(self.t)
        self.lastDay = str(self.t - 86400)

    def set_table(self, table):
        self.table = table

    def set_field(self, field):
        self.field = field

    def set_ts(self, ts):
        self.ts = ts

    def set_ts_field(self, ts_field):
        self.ts_field = ts_field

    def set_ts_create(self, ts_create):
		self.ts_create = ts_create

    def set_output_file(self, output_file):
        self.output_file = output_file

    def set_date(self, day):
        self.tt = self.t - day * 86400
        self.currentTime = str(self.tt)
        self.lastDay = str(self.tt - 86400)

    def scan_table(self):
        try:
            db = MySQLdb.connect(host='172.31.10.151', user='vreader', db='veryplus', passwd='veryplus100200', charset='utf8')
            cur = db.cursor()
            sql_command = 'select ' + self.field + ' from ' + self.table + ' where ' + self.ts_field + ' >= ' + self.lastDay  + ' && ' + self.ts_field + ' < ' + self.currentTime
            print sql_command
            cur.execute(sql_command)
            output_file = self.output_file
            with open(output_file, 'w') as f:
                for row in cur.fetchall():
                    row = '\t'.join([str(item) for item in row]) + '\n'
                    f.write(row)
            cur.close()
            db.close()
            return True
        except Exception, e:
            print e
            return False

    def scan_table_platform_gender(self):
        try:
            db = MySQLdb.connect(host='172.31.10.151', user='vreader', db='veryplus', passwd='veryplus100200', charset='utf8')
            cur = db.cursor()
            sql_command = 'select ' + self.field + ' from ' + self.table + ' where ' + self.ts_field + ' && ' + self.ts + ' >= ' + self.lastDay  + ' && ' + self.ts + ' < ' + self.currentTime + ' && ' + self.ts_create + ' >= ' + self.lastDay  + ' && ' + self.ts_create + ' < ' + self.currentTime
            print sql_command
            cur.execute(sql_command)
            output_file = self.output_file
            with open(output_file, 'w') as f:
		        for row in cur.fetchall():
		            row = '\t'.join([str(item) for item in row]) + '\n'
		            f.write(row)
            cur.close()
            db.close()
            return True
        except Exception, e:
			print e
			return False

    def scan_table_recent_active(self, daysago, output_file):
        st = str(self.tt - (daysago + 1) * 86400)
        ed = str(self.tt - daysago * 86400)
        try:
			db = MySQLdb.connect(host='172.31.10.151', user='vreader', db='veryplus', passwd='veryplus100200', charset='utf8')
			cur = db.cursor()
			sql_command = 'select ' + self.field + ' from ' + self.table + ' where '  + self.ts + ' >= ' + st  + ' && ' + self.ts + ' < ' + ed
			print sql_command
			cur.execute(sql_command)
			with open(output_file, 'w') as f:
				for row in cur.fetchall():
					row = '\t'.join([str(item) for item in row]) + '\n'
					f.write(row)
			cur.close()
			db.close()
			return True
        except Exception, e:
            print e
            return False

if __name__ == '__main__':
	getMySQLData = GetMySQLData()
	getMySQLData.scan_table_platform_gender()



