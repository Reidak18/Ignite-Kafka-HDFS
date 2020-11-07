#!/usr/bin/python
# Nov  7 06:31:52 localhost rsyslogd: [origin software="rsyslogd" swVersion="8.24.0-41.el7_7.4" x-pid="1456" x-info="http://www.rsyslog.com"] rsyslogd was HUPed
import sys
import random

day_str = "Nov  7 0" 

file = open(str(sys.argv[2]), 'w')
for number in range(int(sys.argv[1])):
    hour_str = day_str + str(random.randint(0, 9)) + ":" + str(random.randint(0, 59)) + ":" + str(random.randint(0, 59));
    
    prior = random.randint(0, 7)
    log_str = hour_str + " "
    if prior == 0:
	log_str += "emerg"
    if prior == 1:
	log_str += "alert"
    if prior == 2:
	log_str += "crit"
    if prior == 3:
	log_str += "err"
    if prior == 4:
	log_str += "warn"
    if prior == 5:
	log_str += "notice"
    if prior == 6:
	log_str += "info"
    if prior == 7:
	log_str += "debug"

    file.write(log_str + "\n")

file.close()
