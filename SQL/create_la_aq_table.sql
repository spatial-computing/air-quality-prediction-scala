-- Sequence: public.airnow_reporting_area_uid_seq
-- DROP SEQUENCE public.airnow_reporting_area_uid_seq;
CREATE SEQUENCE public.airnow_reporting_area_uid_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
  
  
-- Table: public.airnow_reporting_area
-- DROP TABLE public.airnow_reporting_area;
CREATE TABLE public.airnow_reporting_area
(
  uid bigint NOT NULL DEFAULT nextval('airnow_reporting_area_uid_seq'::regclass),
  date_observed timestamp with time zone NOT NULL,
  reporting_area text NOT NULL,
  state_code character varying(5) NOT NULL,
  parameter_name character varying(10) NOT NULL,
  aqi real NOT NULL,
  category_name text,
  category_number integer NOT NULL,
  location geometry(Point,4326) NOT NULL,
  CONSTRAINT airnow_reporting_area_pkey PRIMARY KEY (uid)
)
WITH (
  OIDS=FALSE
);  


-- Sequence: public.los_angeles_air_quality_uid_seq
-- DROP SEQUENCE public.los_angeles_air_quality_uid_seq;
CREATE SEQUENCE public.los_angeles_air_quality_uid_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
  
-- Table: public.los_angeles_air_quality
-- DROP TABLE public.los_angeles_air_quality
CREATE TABLE public.los_angeles_air_quality
(
  uid bigint NOT NULL DEFAULT nextval('los_angeles_air_quality_uid_seq'::regclass),
  date_observed timestamp with time zone NOT NULL,
  parameter_name character varying(10) NOT NULL,
  concentraion real NOT NULL,
  unit text,
  aqi real NOT NULL,
  category_number integer,
  location geometry(Point,4326) NOT NULL,
  CONSTRAINT los_angeles_air_quality_pkey PRIMARY KEY (uid)
)
WITH (
  OIDS=FALSE
);  

