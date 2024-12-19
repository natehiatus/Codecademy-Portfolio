import pandas as pd
import numpy as np
import glob

# use glob to collect csv's and concat them to a single csv
files = glob.glob("/bike-rental-starter-kit/data/JC-*-citibike-tripdata.csv")
df_list = []
for filename in files:
    data = pd.read_csv(filename)
    df_list.append(data)

bikedata = pd.concat(df_list)

weather = pd.read_csv("/bike-rental-starter-kit/data/newark_airport_2016.csv")

#adjust column names
bikedata.columns = bikedata.columns.str.lower().str.replace(' ','_')
weather.columns = weather.columns.str.lower()

#get rid of riders that are too old
bikedata = bikedata[bikedata.birth_year >= 1902]

#edit gender column: use strings 'M', 'F', and 'U' instead of int 1, 2, and 0
def gender(num):
    if num == 0:
        return 'U'
    elif num == 1:
        return 'M'
    else:
        return 'F'

bikedata.gender = bikedata.gender.astype(int).apply(gender)

# change birth year to int 
bikedata.birth_year = bikedata.birth_year.astype(int)

#create stations df 
bikedata = bikedata.rename(columns = {'start_station_latitude': 'start_latitude', 
                                      'start_station_longitude': 'start_longitude', 
                                      'end_station_latitude': 'end_latitude', 
                                      'end_station_longitude': 'end_longitude'})

start_stations = bikedata[['start_station_id', 'start_station_name', 'start_latitude','start_longitude']].drop_duplicates() # create start_stations df

start_stations.columns = start_stations.columns.str.replace('start_', '') # modify column names

end_stations = bikedata[['end_station_id', 'end_station_name', 'end_latitude','end_longitude']].drop_duplicates() # create end_stations df
end_stations.columns = end_stations.columns.str.replace('end_', '')

stations = pd.concat([start_stations, end_stations]).drop_duplicates().reset_index(drop=True) # use start_stations and end_stations to create stations df, making sure all stations in the data are included.
stations.columns = stations.columns.str.replace('station_', '') # modify column names

# create trip_data df 
trip_data = bikedata[['bike_id', 'start_station_id', 'start_time', 'end_station_id', 'stop_time', 'trip_duration', 'user_type', 'birth_year', 'gender']]

# clean weather table 
weather = weather.drop(columns = ['pgtm', 'tsun'])
weather.columns.str.lower()
weather = weather.rename(columns = {'awnd': 'awnd_mph', 'snwd': 'snw_dpth'})
weather = weather.drop(columns = ['station', 'name'])

# export data
trip_data.to_csv('/python_cleanup_export/trip_data.csv', index=False)
weather.to_csv('/python_cleanup_export/weather.csv', index=False)
stations.to_csv('/python_cleanup_export/stations.csv', index=False)