
#create table
create table purpleair.purpleair_sensor_la_hourly(
uid bigint NOT NULL DEFAULT nextval('purpleair.purpleair_sensor_la_hourly_seq'::regclass),
timestamp timestamp with time zone,id integer, aqi real, location GEOMETRY(Point,4326),
CONSTRAINT purpleair_sensor_la_hourly_pkey PRIMARY KEY (uid))

#create sequence
CREATE SEQUENCE purpleair.purpleair_sensor_la_hourly_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
#drop sequence
DROP SEQUENCE purpleair.purpleair_sensor_la_hourly_seq

