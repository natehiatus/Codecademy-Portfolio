COPY (SELECT * FROM avg_rides_hourly) TO '/db_views_export/avg_rides_hourly.csv' DELIMITER ',' CSV HEADER;
COPY (SELECT * FROM daily_trips_weather) TO '/db_views_export/daily_trips_weather.csv' DELIMITER ',' CSV HEADER;
COPY (SELECT * FROM travel_directions) TO '/db_views_export/travel_directions.csv' DELIMITER ',' CSV HEADER;
COPY (SELECT * FROM travel_vs_wind) TO '/db_views_export/travel_vs_wind.csv' DELIMITER ',' CSV HEADER;
COPY (SELECT * FROM weather_trips_and_duration) TO '/db_views_export/weather_trips_and_duration.csv' DELIMITER ',' CSV HEADER;