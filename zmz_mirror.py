#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 

import sys
import urllib2, json, pymysql
import time, math, hashlib, types
import threading, Queue

reload(sys)
sys.setdefaultencoding('utf-8')

sql_user = ""
sql_pass = ""
sql_db = ""

log_pass = "/home/log/zimuzu_mirror_log/"

cid = ""
key = ""

fetch_sub_list_base_url = "http://api_server/sub/fetchlist?client=1&limit=20"
get_sub_info_base_url = "http://api_server/sub/getinfo?client=1"
fetch_res_list_base_url = "http://api_server/res/fetchlist?client=1&limit=20&sort=update"
get_res_info_base_url = "http://api_server/res/getinfo?client=1"
res_item_list_base_url = "http://api_server/res/itemlist?client=1&file=1"

fetch_sub_page_thread_count = 3
fetch_sub_info_thread_count = 10
fetch_res_page_thread_count = 3
fetch_res_info_thread_count = 10


def log(msg, num = 0, thread = ""):
	if num == 0:
		return None
	try:
		print time.asctime() + " thread:" + (threading.current_thread().getName() if thread == "" else " thread:" + thread) + " msg:" + msg
	except TypeError, e:
		print msg
	if num != 0:
		f = file((log_pass + "zmz_mirror_log_" + time.strftime("%Y-%m-%d",time.localtime()) + ".log"), 'a')
		lock = threading.Lock()
		lock.acquire()
		f.write(time.asctime() + " thread:" + (threading.current_thread().getName() if thread == "" else " thread:" + thread) + " msg: ")
		try:
			f.write(msg)
		except TypeError, e:
			pass
		f.write("\n")
		f.close()
		lock.release()


def access_key():
	timestamp = str(int(time.time()))
	md5 = hashlib.md5()
	md5.update(cid + "$$" + key + "&&" + timestamp)
	accesskey = md5.hexdigest()
	return "&cid=" + cid + "&timestamp=" + timestamp + "&accesskey=" + accesskey


def fetch(url):
	url = url + access_key()
	req = urllib2.Request(url)
	fails = 0
	while True:
		try:
			if fails >= 5:
				log("Error in fetch().", num = 1)
				log(url, num = 1)
				return None
			res_data = urllib2.urlopen(req, timeout = 5)
			res = res_data.read()
		except KeyboardInterrupt, e:
			raise e
		except Exception, e:
			log("Fetch timeout, URL: " + url)
			fails += 1
		else:
			if fails != 0:
				log("Fetch success, fail times: %d" % fails + ",URL: " + url)
			break

	try:
		json_data = json.loads(res)
	except ValueError, e:
		log("Error in fetch().", num = 1)
		log(url, num = 1)
		return None
	else:
		if json_data["status"] != 1:
			log("Fetch error!", num = 1)
			log(url, num = 1)
			log(json_data["status"], num = 1)
			log(json_data["info"], num = 1)
			exit(1)
		else :
			return json_data["data"]


def sql_check(str):
	if str == None:
		return ""
	if not isinstance(str, basestring):
		return ""
	new_str = str.replace("'", "\\'")
	return new_str


def fetch_subtitle_page():
	global main_tag
	global fetch_sub_list_tag
	global fetch_sub_page_tag
	global sub_page
	global sub_page_count
	global sub_id_que

	while main_tag > 0:
		lock = threading.Lock()
		lock.acquire()
		page = sub_page
		sub_page = sub_page + 1
		lock.release()
		if page > sub_page_count:
			fetch_sub_page_tag = fetch_sub_page_tag - 1
			fetch_sub_list_tag = fetch_sub_list_tag - 1
			return None
		
		fetch_sub_page_url = fetch_sub_list_base_url + "&page=" + "%d" % page
		sub_page_data = fetch(fetch_sub_page_url)
		if sub_page_data == None:
			log("Error in fetch_subtitle_page().", num = 1)
			log(fetch_sub_page_url, num = 1)
			continue
		
		log("Sub page = %d" % page)
		for sub_data in sub_page_data["list"]:
			sub_id = sub_data["id"]
			sub_update_time = int(sub_data["updatetime"])
			
			conn = pymysql.connect(host = 'localhost', user = sql_user, passwd = sql_pass, db = sql_db, charset = 'utf8')
			cur = conn.cursor()
			count = cur.execute("SELECT * FROM `zmz_subtitle` WHERE `zmz_subtitleid` = " + sub_id)
			if count != 0:
				res = cur.fetchall()[0]
			cur.close()
			conn.close()
			if count != 0:
				sub_indb_dateline = time.mktime(res[-1].timetuple())
				if sub_update_time <= sub_indb_dateline:
					continue
			lock = threading.Lock()
			lock.acquire()
			sub_id_que.put(sub_id)
			lock.release()


def fetch_subtitle_info():
	global main_tag
	global fetch_sub_list_tag
	global fetch_sub_page_tag
	global sub_id_que

	while main_tag > 0:
		lock = threading.Lock()
		lock.acquire()
		if sub_id_que.empty():
			lock.release()
			if fetch_sub_page_tag <= 0:
				fetch_sub_list_tag = fetch_sub_list_tag - 1
				return None
			else :
				time.sleep(1)
				continue
		else :
			sub_id = sub_id_que.get()
			lock.release()

		get_sub_info_url = get_sub_info_base_url + "&id=" + sub_id
		sub_info = fetch(get_sub_info_url)
		if sub_info == None:
			log("Error in fetch_subtitle_info().", num = 1)
			log(get_sub_info_url, num = 1)
			continue

		log("Sub ID = " + sub_id)
		conn = pymysql.connect(host = 'localhost', user = sql_user, passwd = sql_pass, db = sql_db, charset = 'utf8')
		cur = conn.cursor()
		count = cur.execute("SELECT * FROM `zmz_subtitle` WHERE `zmz_subtitleid` = " + sub_id)
		if count == 0:
			datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(sub_info["dateline"])))
			sql_code = "INSERT INTO `zmz_subtitle` (`zmz_subtitleid`, `zmz_resourceid`, `subtitle_cn_name`, `subtitle_en_name`, `subtitle_segment`, `subtitle_source`, `subtitle_category`, `subtitle_lang`, `subtitle_format`, `subtitle_file_name`, `subtitle_file_link`, `subtitle_release_time`) VALUES ('" + sub_id + "', '" + sub_info["resourceid"] + "', '" + sql_check(sub_info["cnname"]) + "', '" + sql_check(sub_info["enname"]) + "', '" + sql_check(sub_info["segment"]) + "', '" + sql_check(sub_info["source"]) + "', '" + sql_check(sub_info["category"]) + "', '" + sql_check(sub_info["lang"]) + "', '" + sql_check(sub_info["format"]) + "', '" + sql_check(sub_info["filename"]) + "', '" + sql_check(sub_info["file"]) + "', '" + datetime + "')"
		else :
			datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(sub_info["updatetime"])))
			sql_code = "UPDATE `zmz_subtitle` SET `zmz_resourceid` = '" + sub_info["resourceid"] + "', `subtitle_cn_name` = '" + sql_check(sub_info["cnname"]) + "', `subtitle_en_name` = '" + sql_check(sub_info["enname"]) + "', `subtitle_segment` = '" + sql_check(sub_info["segment"]) + "', `subtitle_source` = '" + sql_check(sub_info["source"]) + "', `subtitle_category` = '" + sql_check(sub_info["category"]) + "', `subtitle_lang` = '" + sql_check(sub_info["lang"]) + "', `subtitle_format` = '" + sql_check(sub_info["format"]) + "', `subtitle_file_name` = '" + sql_check(sub_info["filename"]) + "', `subtitle_file_link` = '" + sql_check(sub_info["file"]) + "', `subtitle_release_time` = '" + datetime + "' WHERE `zmz_subtitle`.`zmz_subtitleid` = " + sub_id
		try :
			cur.execute(sql_code)
			conn.commit()
		except pymysql.err.ProgrammingError, e :
			log("Execution SQL error, sub_id:" + sub_id, num = 1)
			log("SQL: " + sql_code, num = 1)
		log("Execution success, sub_id: " + sub_id)
		cur.close()
		conn.close()


fetch_sub_list_tag = 0
fetch_sub_page_tag = 0
sub_page = 1
sub_page_count = 0
sub_id_que = Queue.Queue(0)

def fetch_subtitle_list():
	global main_tag
	global fetch_sub_list_tag
	global fetch_sub_page_tag
	global sub_page
	global sub_page_count
	global sub_id_que

	if len(sys.argv) > 1:
		sub_page = int(sys.argv[1])
	else :
		sub_page = 1
	sub_id_que = Queue.Queue(0)
	fetch_sub_list_tag = 0
	fetch_sub_page_tag = 0

	list_data = fetch(fetch_sub_list_base_url + "&page=1")
	if list_data == None:
		log("Error in fetch_subtitle_list().", num = 1)
		return
	sub_page_count = int(math.ceil(float(int(list_data["count"])) / 20.0))

	fetch_sub_page_threads = []
	for i in xrange(1, fetch_sub_page_thread_count + 1):
		t = threading.Thread(target = fetch_subtitle_page)
		fetch_sub_page_threads.append(t)
		t.setDaemon(True)
		t.setName("fetch_sub_page_thread_%d" % i)
		fetch_sub_list_tag = fetch_sub_list_tag + 1
		fetch_sub_page_tag = fetch_sub_page_tag + 1
		t.start()
	log("fetch_sub_page_tag: %d" % fetch_sub_page_tag)
	fetch_sub_info_threads = []
	for i in xrange(1, fetch_sub_info_thread_count + 1):
		t = threading.Thread(target = fetch_subtitle_info)
		fetch_sub_info_threads.append(t)
		t.setDaemon(True)
		t.setName("fetch_sub_info_thread_%d" % i)
		fetch_sub_list_tag = fetch_sub_list_tag + 1
		t.start()
	log("fetch_sub_list_tag: %d" % fetch_sub_list_tag)

	while fetch_sub_list_tag > 0:
		time.sleep(1)
	main_tag = main_tag - 1


def fetch_resource_page():
	global main_tag
	global fetch_res_list_tag
	global fetch_res_page_tag
	global res_page
	global res_page_count
	global res_id_que

	while main_tag > 0:
		lock = threading.Lock()
		lock.acquire()
		page = res_page
		res_page = res_page + 1
		lock.release()
		if page > res_page_count:
			fetch_res_page_tag = fetch_res_page_tag - 1
			fetch_res_list_tag = fetch_res_list_tag - 1
			return None
		
		fetch_res_page_url = fetch_res_list_base_url + "&page=" + "%d" % page
		res_page_data = fetch(fetch_res_page_url)
		if res_page_data == None:
			log("Error in fetch_resource_page().", num = 1)
			log(fetch_res_page_url, num = 1)
			continue

		log("Res page = %d" % page)
		for res_data in res_page_data["list"]:
			res_id = res_data["id"]
			res_update_time = int(res_data["itemupdate"])
			res_lang = res_data["lang"]
			
			conn = pymysql.connect(host = 'localhost', user = sql_user, passwd = sql_pass, db = sql_db, charset = 'utf8')
			cur = conn.cursor()
			count = cur.execute("SELECT * FROM `zmz_resource` WHERE `zmz_resourceid` = " + res_id)
			if count != 0:
				res = cur.fetchall()[0]
			cur.close()
			conn.close()
			if count != 0:
				res_indb_dateline = time.mktime(res[-1].timetuple())
				if res_update_time <= res_indb_dateline:
					continue
			
			res_tuple = (res_id, res_update_time, res_lang)
			lock = threading.Lock()
			lock.acquire()
			res_id_que.put(res_tuple)
			lock.release()


def fetch_resource_info():
	global main_tag
	global fetch_res_list_tag
	global fetch_res_page_tag
	global res_id_que

	while main_tag > 0:
		lock = threading.Lock()
		lock.acquire()
		if res_id_que.empty():
			lock.release()
			if fetch_res_page_tag <= 0:
				fetch_res_list_tag = fetch_res_list_tag - 1
				return None
			else :
				time.sleep(1)
				continue
		else :
			res_tuple = res_id_que.get()
			lock.release()
			res_id = res_tuple[0]
			res_update_time = res_tuple[1]
			res_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(res_update_time))
			res_lang = res_tuple[2]

		get_res_info_url = get_res_info_base_url + "&id=" + res_id
		res_info = fetch(get_res_info_base_url + "&id=" + res_id)
		if res_info == None:
			log("Error in fetch_resource_info(), res_info.", num = 1)
			log(get_res_info_url, num = 1)
			continue

		log("Res ID = " + res_id)
		conn = pymysql.connect(host = 'localhost', user = sql_user, passwd = sql_pass, db = sql_db, charset = 'utf8')
		cur = conn.cursor()
		count = cur.execute("SELECT * FROM `zmz_resource` WHERE `zmz_resourceid` = " + res_id)
		if count == 0:
			sql_code = "INSERT INTO `zmz_resource` (`zmz_resourceid`, `resource_cn_name`, `resource_en_name`, `resource_remark`, `resource_content`, `resource_area`, `resource_category`, `resource_channel`, `resource_lang`, `resource_play_status`, `resource_item_update`) VALUES ('" + res_id + "', '" + sql_check(res_info["cnname"]) + "', '" + sql_check(res_info["enname"]) + "', '" + sql_check(res_info["remark"]) + "', '" + sql_check(res_info["content"]) + "', '" + sql_check(res_info["area"]) + "', '" + sql_check(res_info["category"]) + "', '" + sql_check(res_info["channel"]) + "', '" + sql_check(res_lang) + "', '" + sql_check(res_info["play_status"]) + "', '" + res_datetime + "')"
		else :
			sql_code = "UPDATE `zmz_resource` SET `resource_cn_name` = '" + sql_check(res_info["cnname"]) + "', `resource_en_name` = '" + sql_check(res_info["enname"]) + "', `resource_remark` = '" + sql_check(res_info["remark"]) + "', `resource_content` = '" + sql_check(res_info["content"]) + "', `resource_area` = '" + sql_check(res_info["area"]) + "', `resource_category` = '" + sql_check(res_info["category"]) + "', `resource_channel` = '" + sql_check(res_info["channel"]) + "', `resource_lang` = '" + sql_check(res_lang) + "', `resource_play_status` = '" + sql_check(res_info["play_status"]) + "', `resource_item_update` = '" + res_datetime + "' WHERE `zmz_resourceid` = " + res_id
		try :
			cur.execute(sql_code)
			conn.commit()
		except pymysql.err.ProgrammingError, e :
			log("Execution SQL error, res_id:" + res_id, num = 1)
			log("SQL: " + sql_code, num = 1)
		log("Execution success, res_id: " + res_id)
		cur.close()
		conn.close()

		item_list_url = res_item_list_base_url + "&id=" + res_id
		item_list = fetch(item_list_url)
		if item_list == None:
			log("Error in fetch_resource_info(), item_list.", num = 1)
			log(item_list_url, num = 1)
			continue
		
		for item_info in item_list:
			item_id = item_info["id"]
			dateline = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(item_info["dateline"])))
			if item_info["season"] != None:
				item_se = item_info["season"]
			else :
				item_se = "0"
			if item_info["episode"] != None:
				item_ep = item_info["episode"]
			else :
				item_ep = "0"
			if item_info["link"] != None:
				ed2k_link = "NULL"
				magnet_link = "NULL"
				for link in item_info["link"]:
					if link["way"] == "1" and link["address"] != None:
						ed2k_link = link["address"]
					if link["way"] == "2" and link["address"] != None:
						magnet_link = link["address"]

			log("Item ID = " + item_id)
			conn = pymysql.connect(host = 'localhost', user = sql_user, passwd = sql_pass, db = sql_db, charset = 'utf8')
			cur = conn.cursor()
			item_count = cur.execute("SELECT * FROM `zmz_resource_item` WHERE `item_id` = " + item_id)
			if item_count != 0:
				res = cur.fetchall()[0]
			cur.close()
			conn.close()
			if item_count != 0:
				old_date = time.mktime(res[-1].timetuple())
				if int(item_info["dateline"]) <= old_date:
					continue
				else :
					sql_code = "UPDATE `zimuzu`.`zmz_resource_item` SET `item_file_name` = '" + sql_check(item_info["name"]) + "', `item_format` = '" + sql_check(item_info["format"]) + "', `item_season` = '" + item_se + "', `item_episode` = '" + item_ep + "', `item_size` = '" + sql_check(item_info["size"]) + "', `item_ed2k_link` = '" + sql_check(ed2k_link) + "', `item_magnet_link` = '" + sql_check(magnet_link) + "', `item_dateline` = '" + dateline + "' WHERE `item_id` = " + item_id
			else :
				sql_code = "INSERT INTO `zimuzu`.`zmz_resource_item`(`zmz_resourceid`, `item_id`, `item_file_name`, `item_format`, `item_season`, `item_episode`, `item_size`, `item_dateline`, `item_ed2k_link`, `item_magnet_link`) VALUES ('" + res_id + "', '" + item_id + "', '" + sql_check(item_info["name"]) + "', '" + sql_check(item_info["format"]) + "', '" + item_se + "', '" + item_ep + "', '" + sql_check(item_info["size"]) + "', '" + dateline + "', '" + sql_check(ed2k_link) + "', '" + sql_check(magnet_link) + "')"
			conn = pymysql.connect(host = 'localhost', user = sql_user, passwd = sql_pass, db = sql_db, charset = 'utf8')
			cur = conn.cursor()
			try :
				cur.execute(sql_code)
				conn.commit()
			except pymysql.err.ProgrammingError, e :
				log("Execution SQL error, item_id:" + item_id, num = 1)
				log("SQL: " + sql_code, num = 1)
			log("Execution success, item_id: " + item_id)
			cur.close()
			conn.close()


fetch_res_list_tag = 0
fetch_res_page_tag = 0
res_page = 0
res_page_count = 0
res_id_que = Queue.Queue(0)

def fetch_resource_list():
	global main_tag
	global fetch_res_list_tag
	global fetch_res_page_tag
	global res_page
	global res_page_count
	global res_id_que

	if len(sys.argv) > 1:
		res_page = int(sys.argv[1])
	else :
		res_page = 1
	res_id_que = Queue.Queue(0)
	fetch_res_list_tag = 0
	fetch_res_page_tag = 0

	list_data = fetch(fetch_res_list_base_url + "&page=1")
	if list_data == None:
		log("Error in fetch_resource_list().", num = 1)
		return
	res_page_count = int(math.ceil(float(int(list_data["count"])) / 20.0))

	fetch_res_page_threads = []
	for i in xrange(1, fetch_res_page_thread_count + 1):
		t = threading.Thread(target = fetch_resource_page)
		fetch_res_page_threads.append(t)
		t.setDaemon(True)
		t.setName("fetch_res_page_thread_%d" % i)
		fetch_res_list_tag = fetch_res_list_tag + 1
		fetch_res_page_tag = fetch_res_page_tag + 1
		t.start()
	log("fetch_res_page_tag: %d" % fetch_res_page_tag)
	fetch_res_info_threads = []
	for i in xrange(1, fetch_res_info_thread_count + 1):
		t = threading.Thread(target = fetch_resource_info)
		fetch_res_page_threads.append(t)
		t.setDaemon(True)
		t.setName("fetch_res_info_thread_%d" % i)
		fetch_res_list_tag = fetch_res_list_tag + 1
		t.start()
	log("fetch_res_list_tag: %d" % fetch_res_list_tag)

	while fetch_res_list_tag > 0:
		time.sleep(1)
	main_tag = main_tag - 1


main_tag = 0

if __name__ == "__main__":
	print "You can press Ctrl+c to close!"
	log("Start", num = 1)
	try:
		main_tag = 0
		main_threads = []
		t1 = threading.Thread(target = fetch_subtitle_list)
		main_threads.append(t1)
		main_tag = main_tag + 1
		t2 = threading.Thread(target = fetch_resource_list)
		main_threads.append(t2)
		main_tag = main_tag + 1
		for t in main_threads:
			t.setDaemon(True)
			t.start()
		
		while main_tag > 0 :
			time.sleep(1)
		log("End", num = 1)

	except KeyboardInterrupt:
		print "User press Ctrl+c, exit!"
		exit(0)

