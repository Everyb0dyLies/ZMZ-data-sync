#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 

import sys
import urllib2
import json
import pymysql
import time
import math
import hashlib

reload(sys)
sys.setdefaultencoding('utf-8')

cid = ""
key = ""

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
	url = url + access_key()
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


def access_key():
	timestamp = str(int(time.time()))
	md5 = hashlib.md5()
	md5.update(cid + "$$" + key + "&&" + timestamp)
	accesskey = md5.hexdigest()
	return "&cid=" + cid + "&timestamp=" + timestamp + "&accesskey=" + accesskey


def fetch_subtitle_list():
	fetchlist_base_url = "http://api_server/subtitle/fetchlist?client=1&limit=20"
	getinfo_base_url = "http://api_server/subtitle/getinfo?client=1"
	if len(sys.argv) > 1:
		start_page = int(sys.argv[1])
	else :
		start_page = 1

	fetchlist_url = fetchlist_base_url + "&page=1"
	list_data = fetch(fetchlist_url)
	if list_data == None:
		log("Error in fetch_subtitle_list().")
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
				log("Insert sub " + sub_info["id"] + " success.")
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
					log("Update sub " + sub_info["id"] + " success.")

	cur.close()
	conn.close()


def fetch_resource_list():
	fetchlist_base_url = "http://api_server/resource/fetchlist?client=1&limit=20&sort=update"
	getinfo_base_url = "http://api_server/resource/getinfo?client=1"
	item_list_base_url = "http://api_server/resource/itemlist?client=1&file=1"

	if len(sys.argv) > 1:
		start_page = int(sys.argv[1])
	else :
		start_page = 1

	fetchlist_url = fetchlist_base_url + "&page=1"
	list_data = fetch(fetchlist_url)
	if list_data == None:
		log("Error in fetch_resource_list().")
		return
	res_count = int(list_data["count"])
	page_count = int(math.ceil(float(res_count) / 20.0))

	conn = pymysql.connect(host = 'localhost', user = sql_user, passwd = sql_pass, db = sql_db, charset = 'utf8')
	cur = conn.cursor()

	for page in xrange(start_page, page_count + 1):
		fetchlist_url = fetchlist_base_url + "&page=" + "%d" % page
		list_data = fetch(fetchlist_url)
		if list_data == None:
			continue
		
		log("Page = %d" % page)
		for res_data in list_data["list"]:
			res_id = res_data["id"]
			res_item_update_time = int(res_data["itemupdate"])
			getinfo_url = getinfo_base_url + "&id=" + res_id
			
			log("Res ID = " + res_id)
			count = cur.execute("SELECT * FROM `zmz_resource` WHERE `zmz_resourceid` = " + res_id)
			if count == 0:
				res_info = fetch(getinfo_url)
				if res_info == None:
					continue
				datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(res_item_update_time))
				try :
					cur.execute("INSERT INTO `zimuzu`.`zmz_resource` (`zmz_resourceid`, `resource_cn_name`, `resource_en_name`, `resource_remark`, `resource_content`, `resource_area`, `resource_category`, `resource_channel`, `resource_lang`, `resource_play_status`, `resource_item_update`) VALUES ('" + res_id + "', '" + sql_check(res_info["cnname"]) + "', '" + sql_check(res_info["enname"]) + "', '" + sql_check(res_info["remark"]) + "', '" + sql_check(res_info["content"]) + "', '" + sql_check(res_info["area"]) + "', '" + sql_check(res_info["category"]) + "', '" + sql_check(res_info["channel"]) + "', '" + sql_check(res_data["lang"]) + "', '" + sql_check(res_info["play_status"]) + "', '" + datetime + "')")
					conn.commit()
				except pymysql.err.ProgrammingError as e :
					log("Insert res " + res_id + " error.")
					log("INSERT INTO `zimuzu`.`zmz_resource` (`zmz_resourceid`, `resource_cn_name`, `resource_en_name`, `resource_remark`, `resource_content`, `resource_area`, `resource_category`, `resource_channel`, `resource_lang`, `resource_play_status`, `resource_item_update`) VALUES ('" + res_id + "', '" + sql_check(res_info["cnname"]) + "', '" + sql_check(res_info["enname"]) + "', '" + sql_check(res_info["remark"]) + "', '" + sql_check(res_info["content"]) + "', '" + sql_check(res_info["area"]) + "', '" + sql_check(res_info["category"]) + "', '" + sql_check(res_info["channel"]) + "', '" + sql_check(res_data["lang"]) + "', '" + sql_check(res_info["play_status"]) + "', '" + datetime + "')")
					log(e)
				log("Insert res " + res_id + " success.")
				
				item_list_url = item_list_base_url + "&id=" + res_id
				item_list = fetch(item_list_url)
				if item_list == None:
					continue
				for item_info in item_list:
					item_id = item_info["id"]
					log("Item ID = " + item_id)
					dateline = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(item_info["dateline"])))
					if item_info["season"] != None:
						item_se = item_info["season"]
					else :
						item_se = "NULL"
					if item_info["episode"] != None:
						item_ep = item_info["episode"]
					else :
						item_ep = "NULL"
					if item_info["link"] != None:
						ed2k_link = "NULL"
						magnet_link = "NULL"
						for link in item_info["link"]:
							if link["way"] == "1" and link["address"] != None:
								ed2k_link = link["address"]
							if link["way"] == "2" and link["address"] != None:
								magnet_link = link["address"]
					try :
						cur.execute("INSERT INTO `zimuzu`.`zmz_resource_item`(`zmz_resourceid`, `item_id`, `item_file_name`, `item_format`, `item_season`, `item_episode`, `item_size`, `item_dateline`, `item_ed2k_link`, `item_magnet_link`) VALUES ('" + res_id + "', '" + item_id + "', '" + sql_check(item_info["name"]) + "', '" + sql_check(item_info["format"]) + "', '" + item_se + "', '" + item_ep + "', '" + sql_check(item_info["size"]) + "', '" + dateline + "', '" + sql_check(ed2k_link) + "', '" + sql_check(magnet_link) + "')")
						conn.commit()
					except pymysql.err.ProgrammingError as e :
						log("Insert item " + item_id + " error.")
						log("INSERT INTO `zimuzu`.`zmz_resource_item`(`zmz_resourceid`, `item_id`, `item_file_name`, `item_format`, `item_season`, `item_episode`, `item_size`, `item_dateline`, `item_ed2k_link`, `item_magnet_link`) VALUES ('" + res_id + "', '" + item_id + "', '" + sql_check(item_info["name"]) + "', '" + sql_check(item_info["format"]) + "', '" + item_se + "', '" + item_ep + "', '" + sql_check(item_info["size"]) + "', '" + dateline + "', '" + sql_check(ed2k_link) + "', '" + sql_check(magnet_link) + "')")
						log(e)
					log("Insert item " + item_id + " success.")

			else :
				res = cur.fetchall()[0]
				res_item_indb_dateline = time.mktime(res[-1].timetuple())
				if res_item_update_time > res_item_indb_dateline:
					# 可能需要确认时间检测是否正确，可能需要添加剧信息更新代码
					item_list_url = item_list_base_url + "&id=" + res_id
					item_list = fetch(item_list_url)
					if item_list == None:
						continue
					for item_info in item_list:
						item_id = item_info["id"]
						dateline = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(item_info["dateline"])))
						if item_info["season"] != None:
							item_se = item_info["season"]
						else :
							item_se = "NULL"
						if item_info["episode"] != None:
							item_ep = item_info["episode"]
						else :
							item_ep = "NULL"
						if item_info["link"] != None:
							ed2k_link = "NULL"
							magnet_link = "NULL"
							for link in item_info["link"]:
								if link["way"] == "1" and link["address"] != None:
									ed2k_link = link["address"]
								if link["way"] == "2" and link["address"] != None:
									magnet_link = link["address"]

						log("Item ID = " + item_id)
						item_count = cur.execute("SELECT * FROM `zmz_resource_item` WHERE `item_id` = " + item_id)
						if item_count == 0:
							try :
								cur.execute("INSERT INTO `zimuzu`.`zmz_resource_item`(`zmz_resourceid`, `item_id`, `item_file_name`, `item_format`, `item_season`, `item_episode`, `item_size`, `item_dateline`, `item_ed2k_link`, `item_magnet_link`) VALUES ('" + res_id + "', '" + item_id + "', '" + sql_check(item_info["name"]) + "', '" + sql_check(item_info["format"]) + "', '" + item_se + "', '" + item_ep + "', '" + sql_check(item_info["size"]) + "', '" + dateline + "', '" + sql_check(ed2k_link) + "', '" + sql_check(magnet_link) + "')")
								conn.commit()
							except pymysql.err.ProgrammingError as e :
								log("Insert item " + item_id + " error.")
								log("INSERT INTO `zimuzu`.`zmz_resource_item`(`zmz_resourceid`, `item_id`, `item_file_name`, `item_format`, `item_season`, `item_episode`, `item_size`, `item_dateline`, `item_ed2k_link`, `item_magnet_link`) VALUES ('" + res_id + "', '" + item_id + "', '" + sql_check(item_info["name"]) + "', '" + sql_check(item_info["format"]) + "', '" + item_se + "', '" + item_ep + "', '" + sql_check(item_info["size"]) + "', '" + dateline + "', '" + sql_check(ed2k_link) + "', '" + sql_check(magnet_link) + "')")
								log(e)
							log("Insert item " + item_id + " success.")
						else :
							res = cur.fetchall()[0]
							old_date = time.mktime(res[-1].timetuple())
							if int(item_info["dateline"]) > old_date:
								try :
									cur.execute("UPDATE `zimuzu`.`zmz_resource_item` SET `item_file_name` = '" + sql_check(item_info["name"]) + "', `item_format` = '" + sql_check(item_info["format"]) + "', `item_season` = '" + item_se + "', `item_episode` = '" + item_ep + "', `item_size` = '" + sql_check(item_info["size"]) + "', `item_ed2k_link` = '" + sql_check(ed2k_link) + "', `item_magnet_link` = '" + sql_check(magnet_link) + "', `item_dateline` = '" + dateline + "' WHERE `item_id` = " + item_id)
									conn.commit()
								except pymysql.err.ProgrammingError as e :
									log("Update item " + item_id + " error.")
									log("UPDATE `zimuzu`.`zmz_resource_item` SET `item_file_name` = '" + sql_check(item_info["name"]) + "', `item_format` = '" + sql_check(item_info["format"]) + "', `item_season` = '" + item_se + "', `item_episode` = '" + item_ep + "', `item_size` = '" + sql_check(item_info["size"]) + "', `item_ed2k_link` = '" + sql_check(ed2k_link) + "', `item_magnet_link` = '" + sql_check(magnet_link) + "', `item_dateline` = '" + dateline + "' WHERE `item_id` = " + item_id)
									log(e)
								log("Update item " + item_id + " success.")

	cur.close()
	conn.close()


if __name__ == "__main__":
	print "You can press Ctrl+c to close!"
	try:
		fetch_subtitle_list()
		fetch_resource_list()

	except KeyboardInterrupt:
		print "User press Ctrl+c, exit!"
		exit(0)

