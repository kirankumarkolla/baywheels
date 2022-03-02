CREATE TABLE tripdata_stg
    (
        ride_id	text,
        rideable_type	text,
        started_at	text,
        ended_at	text,
        start_station_name	text,
        start_station_id	text,
        end_station_name	text,
        end_station_id	text,
        start_lat	text,
        start_lng	text,
        end_lat	text,
        end_lng	text,
        member_casual	text
    );

    CREATE TABLE load_status_ctl
    (
        id serial,
        batch_id integer,
        file_name text,
        load_date timestamp,
        load_status text

    );

    CREATE TABLE tripdata_cln
    (
      ride_id	text	,
      rideable_type	text	,
      started_at	timestamp	,
      ended_at	timestamp	,
      start_station_name	text	,
      start_station_id	text	,
      end_station_name	text	,
      end_station_id	text	,
      start_lat	float8	,
      start_lng	float8	,
      end_lat	float8	,
      end_lng	float8	,
      member_casual	text	,
      batch_id	text	,
      load_date	timestamp	
    );

    create table stations_dim
    (
      station_id SERIAL not null,
      station_name text,
      station_code text,
      longitude text,
      latitude text
    );

    create table rides_fact
    (
      ride_id text,
      rideable_type text,
      started_at timestamp,
      ended_at timestamp,
      start_station_id int,
      end_station_id int,
      member_casual text
    );