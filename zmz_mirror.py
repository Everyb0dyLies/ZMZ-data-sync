#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 

import sys
import urllib2
import json
import pymysql
import time
import math

timestamp = "1456319856"
accesskey = "8fd5d23951f65ab903f72c2c9823b921"
uid = ""
token = "cc0cfac52e875f19f888ecf18193676d"

sql_user = ""
sql_pass = ""
sql_db = ""

log_pass = "/home/log/zimuzu_mirror_log/"


def log(msg):
	print msg
	f = file((log_pass + "zmz_mirror_log_" + time.strftime("%Y-%m-%d",time.localtime()) + ".log"), 'a')
	f.write("%s %s\n" % (time.asctime(), msg))
	f.close()


def fetch(url):
	req = urllib2.Request(url)
	fails = 0
	while True:
		try:
			if fails >= 3:
				log("Error in fetch().")
				log(url)
				return None
			res_data = urllib2.urlopen(req, timeout = 5)
			res = res_data.read()
		except KeyboardInterrupt, e:
			raise e
		except Exception, e:
			log("Fetch timeout, URL : " + url)
			log(e)
			fails += 1
		else:
			break

	try:
		json_data = json.loads(res)
	except ValueError, e:
		log("Error in fetch().")
		log(e)
		log(url)
		return None
	else:
		if json_data["status"] != 1:
			log("Fetch error!")
			log(url)
			log(json_data["status"])
			log(json_data["info"])
			exit(1)
		else :
			return json_data["data"]


def connDB():
	conn = pymysql.connect(host = 'localhost', user = sql_user, passwd = sql_pass, db = sql_db, charset = 'utf8')
	cur = conn.cursor()
	return cur


def sql_check(str):
	if str == None:
		return ""
	if not isinstance(str, basestring):
		return ""
	new_str = str.replace("'", "\\'")
	return new_str


def main():
	fetchlist_base_url = "http://api.ousns.net/subtitle/fetchlist?cid=8&limit=20" + "&timestamp=" + timestamp + "&accesskey=" + accesskey + "&uid=" + uid + "&token=" + token
	getinfo_base_url = "http://api.ousns.net/subtitle/getinfo?cid=8&client=1" + "&timestamp=" + timestamp + "&accesskey=" + accesskey + "&uid=" + uid + "&token=" + token
	if len(sys.argv) > 1:
		start_page = int(sys.argv[1])
	else :
		start_page = 1

	fetchlist_url = fetchlist_base_url + "&page=1"
	list_data = fetch(fetchlist_url)
	if list_data == None:
		log("Error in main().")
		return
	sub_count = int(list_data["count"])
	page_count = int(math.ceil(float(sub_count) / 20.0))

	conn = pymysql.connect(host = 'localhost', user = sql_user, passwd = sql_pass, db = sql_db, charset = 'utf8')
	cur = conn.cursor()

	for page in xrange(start_page, page_count + 1):
		fetchlist_url = fetchlist_base_url + "&page=" + "%d" % page
		list_data = fetch(fetchlist_url)
		if list_data == None:
			continue
		
		log("Page = %d" % page)
		for sub_data in list_data["list"]:
			sub_id = sub_data["id"]
			sub_updatetime = int(sub_data["updatetime"])
			getinfo_url = getinfo_base_url + "&id=" + sub_id
			
			log("Sub ID = " + sub_id)
			count = cur.execute("SELECT * FROM `zmz_subtitle` WHERE `zmz_subtitleid` = " + sub_id)
			if count == 0:
				sub_info = fetch(getinfo_url)
				if sub_info == None:
					continue
				datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(sub_info["dateline"])))
				cur.execute("INSERT INTO `zimuzu`.`zmz_subtitle` (`zmz_subtitleid`, `zmz_resourceid`, `subtitle_cn_name`, `subtitle_en_name`, `subtitle_segment`, `subtitle_source`, `subtitle_category`, `subtitle_lang`, `subtitle_format`, `subtitle_file_name`, `subtitle_file_link`, `subtitle_release_time`) VALUES ('" + sub_id + "', '" + sub_info["resourceid"] + "', '" + sql_check(sub_info["cnname"]) + "', '" + sql_check(sub_info["enname"]) + "', '" + sql_check(sub_info["segment"]) + "', '" + sql_check(sub_info["source"]) + "', '" + sql_check(sub_info["category"]) + "', '" + sql_check(sub_info["lang"]) + "', '" + sql_check(sub_info["format"]) + "', '" + sql_check(sub_info["filename"]) + "', '" + sql_check(sub_info["file"]) + "', '" + datetime + "')")
				conn.commit()
				log("Insert " + sub_info["id"] + " success.")
			else :
				res = cur.fetchall()[0]
				sub_indb_dateline = time.mktime(res[-1].timetuple())
				if sub_updatetime > sub_indb_dateline:
					sub_info = fetch(getinfo_url)
					if sub_info == None:
						continue
					datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(sub_info["updatetime"])))
					cur.execute("UPDATE `zimuzu`.`zmz_subtitle` SET `zmz_resourceid` = '" + sub_info["resourceid"] + "', `subtitle_cn_name` = '" + sql_check(sub_info["cnname"]) + "', `subtitle_en_name` = '" + sql_check(sub_info["enname"]) + "', `subtitle_segment` = '" + sql_check(sub_info["segment"]) + "', `subtitle_source` = '" + sql_check(sub_info["source"]) + "', `subtitle_category` = '" + sql_check(sub_info["category"]) + "', `subtitle_lang` = '" + sql_check(sub_info["lang"]) + "', `subtitle_format` = '" + sql_check(sub_info["format"]) + "', `subtitle_file_name` = '" + sql_check(sub_info["filename"]) + "', `subtitle_file_link` = '" + sql_check(sub_info["file"]) + "', `subtitle_release_time` = '" + datetime + "' WHERE `zmz_subtitle`.`zmz_subtitleid` = " + sub_id)
					conn.commit()
					log("Update " + sub_info["id"] + " success.")

	cur.close()
	conn.close()


if __name__ == "__main__":
	print "You can press Ctrl+c to close!"
	try:
		main()
	except KeyboardInterrupt:
		print "User press Ctrl+c, exit!"
		exit(0)

