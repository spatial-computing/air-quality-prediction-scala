from datetime import datetime
import requests
import psycopg2
import  pytz


def get_locations(aq_stations, conn):
    cur = conn.cursor()
    cur.execute('select station_id, st_astext(location) from {aq_stations}'.format(aq_stations=aq_stations))
    res = cur.fetchall()
    cur.close()
    locations = {}
    for each in res:
        idx1 = each[1].index('(')
        idx2 = each[1].index(')')
        if idx1 != -1 and idx2 != -1:
            lon = float(each[1][idx1+1:idx2].split(' ')[0])
            lat = float(each[1][idx1:idx2].split(' ')[1])
            locations[(lon, lat)] = each[0]
    return locations

def get_max_seq(seq_key, conn):
    cur = conn.cursor()
    cur.execute('select last_value from {seq_key}'.format(seq_key=seq_key))
    res = cur.fetchall()[0][0]
    cur.close()
    return res

def get_max_time(aq_table, conn):
    cur = conn.cursor()
    cur.execute('select max(date_observed) from {aq_table}'.format(aq_table=aq_table))
    max_time = cur.fetchall()[0][0]
    cur.close()
    if max_time is None:
        max_time = None
    else:
        tz = pytz.timezone("America/Los_Angeles")
        max_time = datetime(max_time.year, max_time.month, max_time.day, max_time.hour, 0, 0)
        max_time = tz.localize(max_time)
        max_time = max_time.astimezone(pytz.utc)

    return max_time

def main(aq_table, aq_seq_key, aq_stations, stations_seq_key, options, conn):

    # API request URL
    REQUEST_URL = '{url}?parameters={parameters}&&bbox={bbox}&datatype={datatype}&format={format}&api_key={api_key}' \
                  .format(url=options['url'], parameters=options['parameters'], bbox=options['bbox'],
                          datatype=options['data_type'], format=options['format'], api_key=options['api_key'])

    max_time = get_max_time(aq_table, conn)
    sensor_locations = get_locations(aq_stations, conn)

    try:
        raw_data = requests.get(REQUEST_URL)
        if raw_data.text == '':  # no data
            exit(1)

        for line in raw_data.text.split('\n'):
            """
                e.g., line = '"34.21","-118.8694","2018-06-14T16:00","PM2.5","20.3","UG/M3","68","2"'
            """

            if line == '':
                continue

            ele = line.split(',')
            tz = pytz.timezone('UTC')
            timestamp = datetime.strptime(ele[2][1:-1], '%Y-%m-%dT%H:%M')
            timestamp = tz.localize(timestamp)

            # Check if the request timestamp has existed in the database
            if timestamp == max_time:
                exit(0)

            lat = float(ele[0][1:-1])
            lon = float(ele[1][1:-1])

            if sensor_locations == {} or sensor_locations.get((lon, lat)) is None:
                cur = conn.cursor()
                cur.execute('insert into {aq_station} values(nextval(\'{seq_key}\'::regclass), '
                            'ST_GeomFromText(\'POINT({lon} {lat})\',4326));' \
                            .format(aq_station=aq_stations, seq_key=stations_seq_key, lon=lon, lat=lat))
                conn.commit()
                cur.close()
                station_id = get_max_seq(stations_seq_key, conn)
                sensor_locations[(lon, lat)] = station_id
            else:
                station_id = sensor_locations.get((lon, lat))

            parameters = ele[3][1:-1]
            concentration = ele[4][1:-1]
            unit = ele[5][1:-1]
            aqi = ele[6][1:-1]
            catergory = ele[7][1:-1]if ele[7] != "" else 'null'

            cur = conn.cursor()
            sql = 'insert into {aq_table} values ({uid}, {station_id}, \'{date_observed}\', ' \
                  '\'{parameter_name}\', {concentraion}, \'{unit}\', {aqi}, {category_number}, {location})'\
                  .format(aq_table=aq_table,
                          uid='nextval(\'{aq_seq_key}\')'.format(aq_seq_key=aq_seq_key),
                          station_id=station_id,
                          date_observed=timestamp.strftime('%Y-%m-%d %H:00:00+00:00'),
                          parameter_name=parameters,
                          concentraion=concentration,
                          unit=unit,
                          aqi=aqi,
                          category_number=catergory,
                          location='ST_GeomFromText(\'POINT({lon} {lat})\', 4326)'.format(lon=lon, lat=lat))
            # cur.execute(sql)
            # cur.close()

    except Exception as e:
        print('Current time: {time}. Error message: {msg}'.format(time=datetime.now(), msg=e))

if __name__ == "__main__":
    options = {}
    options["url"] = "https://airnowapi.org/aq/data/"
    options["parameters"] = 'O3,PM25,PM10,CO,NO2,SO2'
    options["bbox"] = '-119.017,33.704,-117.591,34.638'
    options["data_type"] = 'B'
    options["format"] = 'text/csv'
    options["api_key"] = "4E0C5E35-31BA-4366-8282-789ABA1B0F73"

    conn = psycopg2.connect(host='localhost', port='11223', user='yijun', password='m\\tC7;cc', database='prisms')
    aq_table = 'los_angeles_air_quality'
    aq_stations = 'los_angeles_aq_stations'
    aq_seq_key = 'los_angeles_air_quality_uid_seq'
    station_seq_key = 'los_angeles_aq_stations_uid_seq'

    main(aq_table, aq_seq_key, aq_stations, station_seq_key, options, conn)
    conn.commit()
    conn.close()