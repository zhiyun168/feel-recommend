# -*- coding: utf-8 -*-
__author__ = 'canoe'

import sys
import smtplib
import datetime 
from email.mime.text import MIMEText

def send_email(to, content, subject):
    try:
        me = "<auto_data@zhiyun168.com>"
        msg = MIMEText(content, _subtype = 'plain', _charset = 'gb2312')
        msg['Subject'] = subject
        msg['From'] = me
        msg['To'] = ";".join(to)

        s = smtplib.SMTP()
        s.connect("smtp.qq.com", 587)
        s.starttls()
        s.login("auto_data@zhiyun168.com", "autodata123")
        s.sendmail(me, to, msg.as_string())
        s.close()
        return True
    except Exception, e:
        print str(e)
        return False

if __name__ == '__main__':
    to = ['zhihang.fan@zhiyun168.com', 'yang.liu@zhiyun168.com', 'jincheng.dai@zhiyun168.com', 'mingyang.lv@zhiyun168.com']
    content_list = []
    for line in sys.stdin:
        content_list.append(line.strip())
    content = '\n'.join(content_list)
    content.decode('utf-8')
    Day3 = (datetime.datetime.now() - datetime.timedelta(days = 4))
    Time = Day3.strftime("%Y%m%d")
    subject = u'新增用户统计-' + Time
    send_email(to, content, subject)

