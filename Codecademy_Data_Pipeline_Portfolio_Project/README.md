# Codecademy Data Pipeline Portfolio Project

## Overview
This project is part of the 'Data Engineer' career path hosted by Codecademy. The career path covers advanced Python and SQL skills and offers multiple projects in which to put these skills to practice.

## Objective
Provided with a synthetic dataset of users on the platform, the objective of the project is to extract data from the development database, conduct the necessary transformations, and then load it to a new build database -  as well as prepare data for reporting in a separate csv.

## Workflow

1. **Setup logging, context manager, and error handling**
<br>
The first section of the script sets up logging, a context manager, and a custom exception. For logging, I set outputs to both the console and a log file. A custom class paired with a context manager decorator allowed me to setup a context manager easily for a sqlite connection.

2. **Define ETL Functions**
<br>
***The extract() function*** first checks if there is an existing build db. If so, it collects the existing id’s for rows in the db and then will only extract subsequent rows from the development db. These new rows of data from each table are saved to their respective dataframes. 
<br>
<br>
***The transform() function*** has two nested functions: clean_students_df() and extract_contact_info(). The clean_students_df() function transforms the data by managing data types and missing values. The extract_contact_info() function takes data from the contact_info column of the students table and extracts 4 separate rows from a dictionary that was in json format. Finally if the tables that were extracted in the extract() function were not empty (i.e. there were new rows of data to load) all of these transformations are completed and saved as the following four dataframes: students_df, contact_info, careers_df, and jobs_df.
<br>
<br>
***The load() function*** has two operations. The first is to load the transformed dataframes into the build database. Next, it uses pandas to join dataframes in a format useful for reporting and appends that data to the analytics csv.
<br>

3. **Execute the Pipeline**
<br>
I found that setting up a class (ETL) to handle the dataframes created in the ETL functions was a good way to make the data indirectly accessible should the ETL script be imported into another. The execute_etl() function initializes that class and executes the load() function from it.

## Running the Script
On the first run, an output similar to the following is generated:

```
INFO: Extracting data...
INFO: cademycode_students: 5000 new rows
INFO: cademycode_courses: 10 new rows
INFO: cademycode_student_jobs: 13 new rows
INFO: Transforming data...
INFO: students table updated.
INFO: contact_info table created.
INFO: Loading data...
INFO: students: 5000 rows added
INFO: contact_info: 5000 rows added
INFO: careers: 10 rows added
INFO: jobs: 10 rows added
INFO: analytics_csv created:
INFO: 5000 rows added to analytics_csv
```
If no more data is added to the dev database, running the script will produce the following output:

```
INFO: Extracting data...
INFO: cademycode_students: 0 new rows
INFO: cademycode_courses: 0 new rows
INFO: cademycode_student_jobs: 0 new rows
INFO: Transforming data...
INFO: Loading data...
INFO: students: 0 rows added
INFO: contact_info: 0 rows added
INFO: careers: 0 rows added
INFO: jobs: 0 rows added
INFO: No new rows loaded to the analytics csv.
```

After adding new data to the dev database, running the script will extract and transform the data before appending it to the build database and analytics csv. Refer to the log folder for more detailed outputs.

