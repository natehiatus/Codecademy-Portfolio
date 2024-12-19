import pandas as pd
import sqlite3
import json
from contextlib import contextmanager
import os
import logging
from datetime import datetime


# -------------------------------------------------------------------------------------------
# SETUP LOGGING, CONTEXT MANAGER, AND ERROR HANDLING
# -------------------------------------------------------------------------------------------


# define paths to dev and build db's
dev_path = 'dev/cademycode.db'
build_path = 'build/cademycode_build.db'

# setup logging
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
log_filename = f'log/log_{timestamp}.log'

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False # console would repeat outputs before adding this line

if logger.handlers:
    logger.handlers.clear() # and this

file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(levelname)s: %(message)s')
console_handler.setFormatter(console_formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# a db class that can hold the connection as an attribute, so it can be referenced from inside the context mgr
class DatabaseConnection:
    def __init__(self, con):
        self.con = con

# yield the DatabaseConnection class
@contextmanager
def database_connection(db_name):
    con = sqlite3.connect(db_name)
    try:
        yield DatabaseConnection(con)
        logger.debug(f'{db_name} connected successfully')
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
    finally:
        logger.debug('Closing database connection')
        con.close()


# TransformError checks if the shape of the df has changed unintentionally during the load
class TransformError(Exception):
    def __init__(self, start_len, end_len):
        self.start_len = start_len
        self.end_len = end_len

    def __str__(self):
        if self.end_len > self.start_len:
            return f'Transformed dataframe returned {self.end_len - self.start_len} more rows than the input dataframe.'
        elif self.end_len < self.start_len:
            return f'Transformed dataframe returned {self.start_len - self.end_len} less rows than the input dataframe.'


# -------------------------------------------------------------------------------------------
# DEFINE ETL FUNCTIONS
# -------------------------------------------------------------------------------------------


def extract(): # gets the existing rows from the build db if exists, and only collects subsequent rows from the dev db

    def log_table_status(table_name, df): # for outputing how many rows were extracted
        logger.info(f'{table_name}: {len(df)} new rows')

    if os.path.exists(build_path):
        logger.debug('Build database exists. Checking for new data:')
        existing_id_dict = { # define an empty dictionary to assign existing id's to table names
            'students': '',
            'careers': '',
            'jobs': ''
        }

        with database_connection(build_path) as db:
            for table in existing_id_dict:
                id_list = list(pd.read_sql_query(f'SELECT id FROM {table};', db.con)['id']) # must reference ['id'] at the end to get the column instead of column name
                id_string = ', '.join(map(str, id_list)) # convert the list into str and join into a single string
                existing_id_dict[table] = id_string # add to dict
            logger.debug('Extracting existing rows in the build database')
        
        with database_connection(dev_path) as db: # only select rows that have id's not already in the build db
            students_df = pd.read_sql_query(f'SELECT * FROM cademycode_students WHERE uuid NOT IN ({existing_id_dict['students']});', db.con)
            careers_df = pd.read_sql_query(f'SELECT * FROM cademycode_courses WHERE career_path_id NOT IN ({existing_id_dict['careers']});', db.con)
            jobs_df = pd.read_sql_query(f'SELECT * FROM cademycode_student_jobs WHERE job_id NOT IN ({existing_id_dict['jobs']});', db.con)
            logger.debug('Extracting new data from the dev database')

    else: # if build db doesn't exist, get all data from dev db
        logger.debug('Build database does not exist. Getting all data from dev:')
        with database_connection(dev_path) as db:
            students_df = pd.read_sql_query('SELECT * FROM cademycode_students;', db.con)
            careers_df = pd.read_sql_query('SELECT * FROM cademycode_courses;', db.con)
            jobs_df = pd.read_sql_query('SELECT * FROM cademycode_student_jobs;', db.con)
        
    log_table_status('cademycode_students', students_df)
    log_table_status('cademycode_courses', careers_df)
    log_table_status('cademycode_student_jobs', jobs_df)

    return students_df, careers_df, jobs_df


def transform(students_df, careers_df, jobs_df):
    
    logger.info('Transforming data...')
    
    def clean_students_df(input_df):
        output_df = input_df.copy()  # start with a copy of all data
        
        output_df[['job_id', 'current_career_path_id']] = output_df[['job_id', 'current_career_path_id']].fillna(999).astype(float) 
        
        output_df = output_df.astype({
            'uuid': int,
            'name': str,
            'sex': str,
            'job_id': int,
            'num_course_taken': float,
            'current_career_path_id': int,
            'time_spent_hrs': float
        })
        
        output_df.dob = pd.to_datetime(output_df.dob)
        output_df[['job_id', 'current_career_path_id']] = output_df[['job_id', 'current_career_path_id']].replace(999, pd.NA)
        
        # fillna num_course_taken with 0
        output_df.num_course_taken = output_df.num_course_taken.fillna(0)
        
        # convert the contact info column into dictionaries so we can extract the data from them in the next function
        output_df.contact_info = output_df.contact_info.apply(json.loads)
        
        logger.info('students table updated.')

        return output_df
    
    def extract_contact_info(input_df):
        # create a new df from selected columns in the input df. the mailing address and email are extracted into separate columns with lambda expressions
        output_df = pd.DataFrame({
            'id': input_df['id'],
            'mailing_address': input_df['contact_info'].apply(lambda x: x['mailing_address']),
            'email': input_df['contact_info'].apply(lambda x: x['email'])
        })

        output_df[['address', 'city', 'state', 'zip']] = output_df.mailing_address.str.split(', ', expand=True) # here the mailing address column is split into 4 additional columns
        output_df.drop(columns='mailing_address', inplace=True) # and the original mailing address column is dropped

        input_df.drop(columns='contact_info', inplace=True)

        logger.info('contact_info table created.')

        return input_df, output_df

    students_contact_info = pd.DataFrame() # initialize the new df

    if not students_df.empty: # perform the transformations only if the df's that were extracted aren't empty (no new data)
        students_df = clean_students_df(students_df).rename(columns={'uuid': 'id'})

        students_df, students_contact_info = extract_contact_info(students_df)

    if not jobs_df.empty:
        jobs_df = jobs_df.drop_duplicates()
    jobs_df = jobs_df.rename(columns={'job_id': 'id'})

    careers_df = careers_df.rename(columns={'career_path_id': 'id'})

    return students_df, students_contact_info, careers_df, jobs_df


def load(load_dict):

    with database_connection(build_path) as db:

        logger.info('Loading data...')
        for table_name, df in load_dict.items(): # load df's to the build db with a loop
            df.to_sql(table_name, db.con, if_exists='append', index=False)
            logger.info(f'{table_name}: {len(df)} rows added')

        students_df = load_dict['students']
        contact_info = load_dict['contact_info']
        careers_df = load_dict['careers']
        jobs_df = load_dict['jobs']

        analytics_csv_file_path = 'build/cademycode_build.csv'
        
        if not students_df.empty: # only attempt to load to csv if there is new data in the students table

            try:
                analytics_csv_df = (
                    students_df
                    .merge(
                        careers_df[['id', 'career_path_name', 'hours_to_complete']], 
                        left_on='current_career_path_id', 
                        right_on='id', 
                        how='left',
                        suffixes=('', '_career')
                    )
                    .merge(
                        jobs_df[['id', 'job_category']], 
                        left_on='job_id', 
                        right_on='id', 
                        how='left',
                        suffixes=('', '_job')
                    )
                    .merge(
                        contact_info[['id', 'city', 'state']], 
                        on='id',
                        how='left'
                    )
                )

                if len(analytics_csv_df) != len(students_df):
                    raise TransformError(len(students_df), len(analytics_csv_df))
                
            except TransformError(len(students_df), len(analytics_csv_df)) as t:
                logger.error(t)
                return
            
            # calculate percentage complete
            analytics_csv_df['prcnt_complete'] = analytics_csv_df['time_spent_hrs'] / analytics_csv_df['hours_to_complete']
            
            # reorder columns
            final_columns = [
                'id', 'name', 'sex', 'dob', 'num_course_taken',
                'career_path_name', 'hours_to_complete', 'time_spent_hrs',
                'prcnt_complete', 'job_category', 'city', 'state'
            ]
            
            analytics_csv_df = analytics_csv_df[final_columns].sort_values('id') # make sure rows are in order

            

            if os.path.exists(analytics_csv_file_path): # if this isnt the first go, dont include the headers
                header = False
                logger.debug('analytics_csv exists:')
            else:
                header = True
                logger.info('analytics_csv created:')

            
            analytics_csv_df.to_csv(analytics_csv_file_path, mode='a', header=header, index=False)
            logger.info(f'{len(analytics_csv_df)} rows added to analytics_csv')

        else:
            logger.info('No new rows loaded to the analytics csv.')

# -------------------------------------------------------------------------------------------
# EXECUTING THE PIPELINE
# -------------------------------------------------------------------------------------------

class ETL:

    def __init__(self):
        logger.info('Extracting data...')
        self.students_df, self.careers_df, self.jobs_df = extract()
        self.students_df, self.students_contact_info, self.careers_df, self.jobs_df = transform(self.students_df, self.careers_df, self.jobs_df)
        
    def load(self):
        load_dict = {
            'students': self.students_df,
            'contact_info': self.students_contact_info,
            'careers': self.careers_df,
            'jobs': self.jobs_df
        }
        
        load(load_dict)

def execute_etl():
    cademycode_etl = ETL()
    cademycode_etl.load()


if __name__ == "__main__":
    execute_etl()