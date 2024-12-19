-- import stations.csv into the stations table
COPY stations(
  id,
  name,
  latitude,
  longitude
)
FROM '/python_cleanup_export/stations.csv'
DELIMITER ','
CSV HEADER;

-- import weather.csv into the weather table
COPY weather(
  date,
  awnd_mph,
  prcp,
  snow,
  snw_dpth,
  tavg,
  tmax,
  tmin,
  wdf2,
  wdf5,
  wsf2,
  wsf5
)
FROM '/python_cleanup_export/weather.csv'
DELIMITER ','
CSV HEADER;

-- import trip_data.csv into the trip_data table
COPY trip_data(
  trip_id,
  bike_id,
  start_station_id,
  start_time,
  end_station_id,
  stop_time,
  trip_duration,
  user_type,
  birth_year,
  gender
)
FROM '/python_cleanup_export/trip_data.csv'
DELIMITER ','
CSV HEADER;