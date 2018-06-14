import psycopg2

def generate_buffers(schema, input_table, id, loc, new_table, conn, buffer_size=3000, ):
    """

    :param input_table: table name that includes locations
    :param id: column name of location id
    :param loc: column name of location
    :param new_table: table name of newly generated table (schema=geo_features)
    :param conn:
    :param buffer_size: default 3000
    :return:
    """

    cur = conn.cursor()
    seq_drop = 'drop sequence if exists public.{seq_key};'.format(seq_key=new_table+'_ogc_fid_seq')

    seq_create = 'create sequence public.{seq_key} ' \
                 'increment 1 minvalue 1 maxvalue 9223372036854775807 start 1 cache 1;' \
                 .format(seq_key=new_table+'_ogc_fid_seq')
    cur.execute(seq_drop)
    conn.commit()
    cur.execute(seq_create)
    conn.commit()

    # Generate a sql list for all buffers
    sql_list = []
    ogc_fid = 'nextval(\'{seq_key}\')'.format(seq_key=new_table+'_ogc_fid_seq')

    for i in range(100, buffer_size + 100, 100):
        sql_buffer = 'select {ogc_fid} as ogc_fid, a.{id} as id, a.{loc} as location, {buffer_size} as buffer_size, ' \
                     'geometry(st_buffer(geography(a.{loc}), {buffer_size}::double precision)) as buffer ' \
                     'from {schema}.{table} a '\
                     .format(ogc_fid = ogc_fid, schema=schema, table=input_table, id=id, loc=loc, buffer_size=i)
        sql_list.append(sql_buffer)

    sql = ' UNION '.join(sql_list)
    table_drop = 'drop table if exists {schema}.{table};'.format(schema='geo_features', table=new_table)
    table_create = 'create table {schema}.{table} as '.format(schema='geo_features', table=new_table) + sql + ';'
    cur.execute(table_drop)
    cur.execute(table_create)
    conn.commit()

    # Generate gist index and unique index
    gist_index = 'create index {gix} on {schema}.{table} using gist(buffer);' \
        .format(gix=new_table + '_gix', schema='geo_features', table=new_table)

    unique_index = 'create unique index {uix} on {schema}.{table} using btree({id});' \
        .format(uix=new_table + '_uix', schema='geo_features', table=new_table, id='ogc_fid')

    cur.execute(gist_index)
    cur.execute(unique_index)
    conn.commit()
    cur.close()
    return new_table


def get_tables(schema, conn):
    """

    :param schema: get tables in a given schema
    :param conn:
    :return:
    """
    cur = conn.cursor()
    cur.execute('select table_name from information_schema.tables where table_schema = \'{schema}\''
                .format(schema=schema))
    res = cur.fetchall()
    tables = [x[0] for x in res]
    cur.close()
    return tables


def generate_geo_features(buffer_table, region_name, b_point, osm_tables, conn):
    """

    :param buffer_table: a table stores all the buffers
    :param region_name: indicate the osm file from which regeion
    :param b_point: bounding box used for filtering
    :param osm_tables: all the openstreetmap tables
    :param conn:
    :return:
    """
    osm_tables = [table for table in osm_tables if table.startswith(region_name)]
    print(osm_tables)

    cur = conn.cursor()
    for osm_table in osm_tables:
        # Geographic feature
        geo_feature = '_'.join(osm_table.split('_')[2:])
        print(geo_feature)
        # Currently we won't include buildings
        if osm_table.find('buildings') != -1:
            continue

        table_name = '_'.join(buffer_table.split('_')[0:-2]) + '_geofeature_' + geo_feature
        print(table_name)
        cur.execute('drop table if exists geo_features.{table};'.format(table=table_name))
        cur.execute('drop table if exists temp;')

        # "temp" table is used for filter an area from osm data first
        # Currently we have california osm and china osm
        # Filter is based on bounding box vertexes (left top, right bottom)
        sql_temp = 'create table temp as select * from openstreetmap.{osm} ' \
                   'where st_intersects(wkb_geometry, ST_SetSRID(ST_MakeBox2D(ST_Point({lt}), ST_Point({rb})), 4326))'\
                   .format(lt=str(b_point[0][0]) + ',' + str(b_point[0][1]),
                           rb=str(b_point[1][0]) + ',' + str(b_point[1][1]), osm=osm_table)
        cur.execute(sql_temp)
        conn.commit()

        # For building is type
        feature_type = 'fclass'

        # Find the geo_type: [MULTIPOLYGON, MULTILINESTRING, POINT]
        cur.execute('select GeometryType(wkb_geometry) from openstreetmap.{table} limit 1'.format(table=osm_table))
        geo_type = cur.fetchall()[0][0]

        if geo_type == 'MULTIPOLYGON':
            sql = 'select t2.id, t2.feature_type, t2.buffer_size, sum(t2.value) as value, ' \
                  '\'{geo_feature}\'::text as geo_feature, \'area\'::text as measurement ' \
                  'from (select b.id, g.{feature_type} as feature_type, b.buffer_size, ' \
                  'st_area(st_intersection(b.buffer, g.wkb_geometry)) as value ' \
                  'from geo_features.{buffer_table} b, temp g ' \
                  'where g.{feature_type} is not NULL and st_intersects(b.buffer, g.wkb_geometry)) t2 ' \
                  'group by t2.id, t2.feature_type, t2.buffer_size'\
                  .format(buffer_table=buffer_table, feature_type=feature_type, geo_feature=geo_feature)

        elif geo_type == 'MULTILINESTRING':
            sql = 'select t2.id, t2.feature_type, t2.buffer_size, sum(t2.value) as value, ' \
                  '\'{geo_feature}\'::text as geo_type, \'length\'::text as measurement ' \
                  'from (select b.id, g.{feature_type} as feature_type, b.buffer_size, ' \
                  'st_length(geography(st_intersection(b.buffer, g.wkb_geometry))) as value ' \
                  'from geo_features.{buffer_table} b, temp g ' \
                  'where g.{feature_type} is not NULL and st_intersects(b.buffer, g.wkb_geometry)) t2 ' \
                  'group by t2.id, t2.feature_type, t2.buffer_size'\
                  .format(buffer_table=buffer_table, feature_type=feature_type, geo_feature=geo_feature)

        elif geo_type == 'POINT':
            sql = 'select b.id, g.{feature_type} as feature_type, b.buffer_size, count(*) as value, ' \
                  '\'{geo_feature}\'::text as geo_feature, \'count\'::text as measurement ' \
                  'from geo_features.{buffer_table} b, temp g ' \
                  'where g.{feature_type} is not NULL and st_contains(b.buffer, g.wkb_geometry) ' \
                  'group by b.id, b.buffer_size, g.{feature_type}'\
                  .format(buffer_table=buffer_table, feature_type=feature_type, geo_feature=geo_feature)

        sql = 'create table geo_features.{table} as '.format(table=table_name) + sql
        # print(sql)

        cur.execute(sql)
        cur.execute('drop table if exists temp;')
        conn.commit()


china = [(115.857, 40.545), (117.198, 39.431)]
cal = [(-119.857, 34.597), (-117.592, 33.659)]
conn = psycopg2.connect(host='localhost', port='5432', user='jjow', password='m\\tC7;cc', database='prisms')
#generate_buffers('public', 'airnow_reporting_area_location', 'reporting_area', 'location',
#                 'airnow_reporting_areas_3000m_buffer', conn)
#generate_buffers('purpleair', 'purplair_sensor_la', 'sensor_id', 'location',
#                 'purpleair_sensor_la_3000m_buffer', conn)
# generate_buffers('public', 'beijing_aq_stations', 'station_id', 'location',
#                  'beijing_aq_stations_3000m_buffer', conn)
osm_tables = get_tables('openstreetmap', conn)
#generate_geo_features('beijing_aq_stations_3000m_buffer', 'china', china, osm_tables, conn)
#generate_geo_features('airnow_reporting_areas_3000m_buffer', 'california', cal, osm_tables, conn)
generate_geo_features('purpleair_sensor_la_3000m_buffer', 'california', cal, osm_tables, conn)