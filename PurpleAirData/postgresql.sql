postgresql

#output POLYGON
insert into los_angeles.boundary
select ST_SetSRID(ST_ConvexHull(ST_Collect(los_angeles.los_angeles_fishnet.geom)),4326)
from los_angeles.los_angeles_fishnet

#create a table for purpleAir sensors within LA area
create table los_angeles.purplAir_sensor_LA(sensor_id varchar(20), sensor_key varchar(20), location GEOMETRY(Point,4326))


#find contain points
insert into los_angeles.purplAir_sensor_LA
select p.sensor_id, p.sensor_key, ST_ASTEXT(p.location)
from purpleAir.purpleair_sensor_location p ,los_angeles.boundary b
where ST_Contains(b.geom, p.location)