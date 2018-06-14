import requests
import json
from pprint import pprint
import psycopg2


conn = psycopg2.connect(host="localhost",port="11223",database="prisms", user="jjow", password="m\\tC7;cc")
cursor = conn.cursor()

#Insert element into purpleAir.purpleAir_sensor_location
url = "https://thingspeak.com/channels/312808/feeds.json?api_key=V36QVAV602HLXD59"
jsonData = "https://www.purpleair.com/json"

rawData = requests.get(jsonData)
rawData = json.loads(rawData.text)
pprint(len(rawData["results"]))
for result in rawData["results"]:
	lat = result["Lat"]
	lon = result["Lon"]
	pid = result["THINGSPEAK_PRIMARY_ID"]
	pkey = result["THINGSPEAK_PRIMARY_ID_READ_KEY"]
	print(lat,lon,pid ,pkey)
	if(lon and lat):
		cursor.execute("INSERT INTO purpleAir.purpleAir_sensor_location (sensor_id,sensor_key,location) VALUES(%s,%s, ST_GeomFromText('POINT(%s %s)', 4326));",(pid,pkey,lon,lat))

conn.commit()
