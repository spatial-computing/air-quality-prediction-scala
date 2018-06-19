import requests
import json
from pprint import pprint
import psycopg2


conn = psycopg2.connect(host="localhost",port="11223",database="prisms", user="jjow", password="m\\tC7;cc")
cursor = conn.cursor()

#Insert element into purpleAir.purpleAir_sensor_location
jsonData = "https://www.purpleair.com/json"

rawData = requests.get(jsonData)
rawData = json.loads(rawData.text)
pprint(len(rawData["results"]))
for result in rawData["results"]:
	pid = result["ID"]
	lat = result["Lat"]
	lon = result["Lon"]
	sid = result["THINGSPEAK_PRIMARY_ID"]
	skey = result["THINGSPEAK_PRIMARY_ID_READ_KEY"]
	parent = result["ParentID"]
	if(parent):
		if(lon and lat):
			print(pid)
			cursor.execute("INSERT INTO others.purpleair_a (primary_id,sensor_id,sensor_key,parent_ID,location) VALUES(%s,%s,%s,%s, ST_GeomFromText('POINT(%s %s)', 4326));",(pid,sid,skey,parent,lon,lat))
	else:
		if(lon and lat):
			print(pid)
			cursor.execute("INSERT INTO others.purpleair_b (primary_id,sensor_id,sensor_key,parent_ID,location) VALUES(%s,%s,%s,%s, ST_GeomFromText('POINT(%s %s)', 4326));",(pid,sid,skey,parent,lon,lat))
	conn.commit()
