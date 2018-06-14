from datetime import datetime
import requests
import psycopg2


def main(conn):

    cur = conn.cursor()
    options = {}
    options["url"] = "https://airnowapi.org/aq/data/"
    options["parameters"] = 'O3,PM25,PM10,CO,NO2,SO2'
    options["bbox"] = '-119.017,33.704,-117.591,34.638'
    options["data_type"] = 'B'
    options["format"] = 'text/csv'
    options["api_key"] = "4E0C5E35-31BA-4366-8282-789ABA1B0F73"

    # API request URL
    REQUEST_URL = '{url}?parameters={parameters}&&bbox={bbox}&datatype={datatype}&format={format}&api_key={api_key}' \
                  .format(url=options['url'],
                          parameters=options['parameters'],
                          bbox=options['bbox'],
                          datatype=options['data_type'],
                          format=options['format'],
                          api_key=options['api_key'])

    '''
        Get current max time in the database
    '''
    sql = 'select max(date_observed) from los_angeles_air_quality'
    cur.execute(sql)
    dt = cur.fetchall()[0][0]
    if dt is None:
        max_timestamp = None
    else:
        max_timestamp = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)

    try:

        raw_data = requests.get(REQUEST_URL)
        if raw_data.text == '':
            exit(1)

        for each in raw_data.text.split('\n'):
            if each == '':
                continue

            each_split = each.split(',')
            lat = each_split[0][1:-1]
            lon = each_split[1][1:-1]
            timestamp = datetime.strptime(each_split[2][1:-1], '%Y-%m-%dT%H:%M')

            '''
                Check if the request timestamp has existed in the database
            '''
            if timestamp == max_timestamp:
                exit(0)
            parameters = each_split[3][1:-1]
            concentration = each_split[4][1:-1]
            unit = each_split[5][1:-1]
            aqi = each_split[6][1:-1]
            catergory = each_split[7][1:-1]if each_split[7] != "" else 'null'

            sql = 'insert into los_angeles_air_quality values ({uid}, \'{date_observed}\', \'{parameter_name}\', ' \
                  '{concentraion}, \'{unit}\', {aqi}, {category_number}, {location})'\
                  .format(uid='nextval(\'los_angeles_air_quality_uid_seq\')',
                          date_observed=timestamp.strftime('%Y-%m-%d %H:00:00+00:00'),
                          parameter_name=parameters,
                          concentraion=concentration,
                          unit=unit,
                          aqi=aqi,
                          category_number=catergory,
                          location='ST_GeomFromText(\'POINT({lon} {lat})\', 4326)'.format(lon=lon, lat=lat))
            cur.execute(sql)

    except Exception as e:
        print('Current time: {time}. Error message: {msg}'.format(time=datetime.now(), msg=e))

if __name__ == "__main__":
    conn = psycopg2.connect(host='localhost', port='5432', user='yijun', password='m\\tC7;cc', database='prisms')
    main(conn)
    conn.commit()