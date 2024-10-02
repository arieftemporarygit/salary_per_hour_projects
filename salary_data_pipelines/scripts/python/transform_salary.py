import os
import pandas as pd
from sqlalchemy import create_engine, text

def salary_per_hour_calculation(employees, timesheets):
    ### employees ###
        
    # rename employe_id to employee_id
    employees.rename(columns={'employe_id': 'employee_id'}, inplace=True)
    
    # Calculate row number based on employee_id, ordered by join_date and salary
    employees['row_num'] = employees.sort_values(['join_date', 'salary'], ascending=[False, False]) \
                      .groupby('employee_id') \
                      .cumcount() + 1
    
    # Select relevant columns
    employees_remove_duplicates = employees[['employee_id', 'branch_id', 'salary', 'join_date', 'resign_date', 'row_num']]
    
    # Filter for rows where row_num is 1
    clean_employees = employees_remove_duplicates[employees_remove_duplicates['row_num'] == 1][['employee_id', 'branch_id', 'salary', 'join_date', 'resign_date']]

    ### timesheets ###
        
    # Define a function to apply the CASE logic for checkin_new
    def get_checkin_new(row):
        if pd.isnull(row['checkin']):
            if pd.to_datetime(row['checkout']).time() > pd.to_datetime('12:00:00').time() and pd.to_datetime(row['checkout']).time() < pd.to_datetime('00:00:00').time():
                return '08:00:00'
            elif pd.to_datetime(row['checkout']).time() > pd.to_datetime('00:00:00').time() and pd.to_datetime(row['checkout']).time() < pd.to_datetime('12:00:00').time():
                return '17:00:00'
        return row['checkin']
    
    # Define a function to apply the CASE logic for checkout_new
    def get_checkout_new(row):
        if pd.isnull(row['checkout']):
            if pd.to_datetime(row['checkin']).time() > pd.to_datetime('00:00:00').time() and pd.to_datetime(row['checkin']).time() < pd.to_datetime('12:00:00').time():
                return '17:00:00'
            elif pd.to_datetime(row['checkin']).time() > pd.to_datetime('12:00:00').time() and pd.to_datetime(row['checkin']).time() < pd.to_datetime('00:00:00').time():
                return '08:00:00'
        return row['checkout']
    
    # Apply the functions to create new columns
    timesheets['checkin_new'] = timesheets.apply(get_checkin_new, axis=1)
    timesheets['checkout_new'] = timesheets.apply(get_checkout_new, axis=1)
    
    # Select relevant columns
    timesheets_modify = timesheets[['timesheet_id', 'employee_id', 'date', 'checkin', 'checkout', 'checkin_new', 'checkout_new']]
    
    def calculate_total_hours(row):
        # Handle NaN values
        checkin_new = pd.to_datetime(row['checkin_new'], format='%H:%M:%S', errors='coerce')
        checkout_new = pd.to_datetime(row['checkout_new'], format='%H:%M:%S', errors='coerce')
        
        # If either checkin_new or checkout_new is NaT, return 9.0
        if pd.isnull(checkin_new) or pd.isnull(checkout_new):
            return 9.0
    
        # Convert times to seconds for easier calculations
        checkin_seconds = (checkin_new.hour * 3600) + (checkin_new.minute * 60) + checkin_new.second
        checkout_seconds = (checkout_new.hour * 3600) + (checkout_new.minute * 60) + checkout_new.second
    
        # Calculate total hours based on conditions
        if checkin_seconds > 0 and checkin_seconds < (12 * 3600):
            return (checkout_seconds - checkin_seconds) / 3600.0
        elif checkin_seconds >= (12 * 3600) and checkin_seconds < (24 * 3600):
            return (checkin_seconds - checkout_seconds) / 3600.0
        elif checkout_seconds > (12 * 3600) and checkout_seconds < (24 * 3600):
            return (checkout_seconds - checkin_seconds) / 3600.0
        elif checkout_seconds > 0 and checkout_seconds < (12 * 3600):
            return (checkin_seconds - checkout_seconds) / 3600.0
        else:
            return 9.0
    
    # Apply the function to create the total_hours column
    timesheets_modify['total_hours'] = timesheets_modify.apply(calculate_total_hours, axis=1)
    
    # Select relevant columns
    timesheets_duration = timesheets_modify[['timesheet_id', 'employee_id', 'date', 'checkin', 'checkout', 'checkin_new', 'checkout_new', 'total_hours']]


    ### join ###

    # Step 1: Perform the left join
    merged_df = pd.merge(timesheets_duration, clean_employees, on='employee_id', how='left')
    
    # Step 2: Filter where salary is not null or not equal to zero
    filtered_df = merged_df[(merged_df['salary'].notnull()) & (merged_df['salary'] != 0)]
    
    filtered_df['date'] = pd.to_datetime(filtered_df['date'])
    # Step 3: Extract year and month from the date
    filtered_df['year'] = filtered_df['date'].dt.year
    filtered_df['month'] = filtered_df['date'].dt.month
    
    # Step 4: Group by employee_id, branch_id, year, and month
    gross_total_hours = (filtered_df.groupby(['employee_id', 'branch_id', 'year', 'month'])
                  .agg(total_day=('employee_id', 'count'),
                       total_hours=('total_hours', 'sum'),
                       salary=('salary', 'min'))
                  .reset_index())
    
    gross_total_hours['prorated_salary'] = gross_total_hours.apply(lambda row: row['salary'] if row['total_day'] > 22 else round((row['total_day'] / 22.0), 2) * row['salary'], axis=1)
    
    # Select relevant columns
    prorated_salary = gross_total_hours[['total_day', 'employee_id', 'branch_id', 'total_hours', 'salary', 'year', 'month', 'prorated_salary']]
    
    # Calculate salary per hour
    prorated_salary['salary_per_hour'] = prorated_salary['prorated_salary'] / prorated_salary['total_hours']
    
    # Select relevant columns
    salary_per_hour = prorated_salary[['total_day', 'employee_id', 'branch_id', 'total_hours', 'salary', 'year', 'month', 'salary_per_hour']]
    
    # Step 1: Group by branch_id, year, and month
    # Step 2: Calculate average salary_per_hour
    branch_salary_per_hour = salary_per_hour.groupby(['branch_id', 'year', 'month'], as_index=False)['salary_per_hour'].mean()
    
    return branch_salary_per_hour



if __name__ == "__main__":

    # Database connection parameters for PostgreSQL
    DB_USER = 'airflow'
    DB_PASSWORD = 'airflow'
    DB_HOST = 'postgres'
    DB_PORT = '5432'
    DB_NAME = 'airflow'

    # Connect to PostgreSQL
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Define configurations for the tables to be processed
    transform_dir = '/opt/airflow/data/transform/python'

    # employees = pd.read_csv('/opt/airflow/data/employees.csv')
    # timesheets = pd.read_csv('/opt/airflow/data/timesheets.csv')
    employees = pd.read_sql_table("employees_python", con=engine)
    timesheets = pd.read_sql_table("timesheets_python", con=engine)

    df = salary_per_hour_calculation(employees,timesheets)

        # Check if the directory exists; if not, create it
    if not os.path.exists(transform_dir):
        os.makedirs(transform_dir)
    
    # Save the DataFrame to a CSV file in the directory
    csv_file_path = os.path.join(transform_dir, 'salary_per_hours.csv')
    df.to_csv(csv_file_path, index=False)
