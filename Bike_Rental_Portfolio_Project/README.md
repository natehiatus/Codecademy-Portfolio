# CitiBike Bike Rentals Analysis

This analysis was conducted as a project from Codecademy’s Data Engineer Career
Path. The course provided the source data and the following prompt to start the project:

> A bike rental company has asked you to create a database to help their analysts understand the 
effects of weather on bike rentals. You’ve been given a year of bike rental data from the company 
and you’ll source weather data from the government. You’ll need to clean and validate both data 
sets, design a relational PostgreSQL database to store the data, and develop views for the 
database to assist the analytics team.

## Project Features

- project_starter_kit
  - contains the data and data dictionaries for the project
- BikeRentals_script.py
  - this script extracts and transforms the data from the project_starter_kit folder <br>
  and places them in the python_cleanup_export folder
- python_cleanup_export
    - contains the transformed data
- CitiBike_DB_Schema.pdf
  - a schema of the relational database to be created to house the transformed data
- BikeRentals_import_csv.sql
  - a SQL script for importing the csv's from python_cleanup_export into my db
- views
  - a folder containing SQL scripts to create the following views:
    - avg_rides_hourly
    - daily_trips_weather
    - travel_directions
    - travel_vs_wind
- BikeRentals_export_views
  - a SQL script for exporting the views from the db into csv's in a folder db_views_export
- CitBike_Rentals_Analysis.twb
  - a Tableau dashboard visualizing data from the views we created
- PortfolioProject_BikeRentals.ipynb
  - an overview of how the project was completed

## Tools
- VSCode
- PostgreSQL
- Tableau
