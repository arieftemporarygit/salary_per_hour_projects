import os
import psycopg2
import pandas as pd

def connect_to_db():
    """Connect to the PostgreSQL database."""
    try:
        conn = psycopg2.connect("dbname='airflow' user='airflow' password='airflow' host='postgres'")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

# Function to fetch data as DataFrame
def fetch_query(query, conn):
    return pd.read_sql_query(query, conn)

def transform_salary(transform_dir, query):
    """Main function to load data from a CSV file into the database."""
    conn = connect_to_db()
    if conn is None:
        return  # Exit if connection fails

    cursor = conn.cursor()
    
    try:
        df = fetch_query(query, conn)
        # Commit the changes
        conn.commit()

    except Exception as e:
        print(f"An error occurred during the database operation: {e}")
        conn.rollback()  # Rollback if there's an error
    finally:
        # Close the connection
        cursor.close()
        conn.close()

    # Check if the directory exists; if not, create it
    if not os.path.exists(transform_dir):
        os.makedirs(transform_dir)
    
    # Save the DataFrame to a CSV file in the directory
    csv_file_path = os.path.join(transform_dir, 'salary_per_hours.csv')
    df.to_csv(csv_file_path, index=False)


if __name__ == "__main__":
    # Define configurations for the tables to be processed
    tables_config = {'folder_path': '/opt/airflow/data/transform/sql',
                     'query' : """ WITH
                                    timesheets_modify AS (
                                        SELECT 
                                        timesheet_id,
                                        employee_id,
                                        date,
                                        checkin,
                                        checkout,
                                        CASE WHEN checkin is null AND checkout >'08:00:00' THEN '08:00:00'::TIME
                                            WHEN checkin is null AND checkout <'08:00:00' THEN '17:00:00'::TIME
                                            ELSE checkin END AS checkin_new,
                                        CASE WHEN checkout is null AND checkin >'08:00:00' THEN '17:00:00'::TIME
                                            WHEN checkout is null AND checkin <'08:00:00' THEN '08:00:00'::TIME
                                            ELSE checkout END AS checkout_new
                                        FROM timesheets
                                        ),
                                    timesheets_duration AS (
                                        SELECT 
                                            timesheet_id,
                                            employee_id,
                                            date,
                                            CASE
                                                WHEN checkin_new < '17:00:00' THEN EXTRACT(EPOCH FROM (checkout_new - checkin_new))/3600
                                                WHEN checkin_new > '17:00:00' THEN EXTRACT(EPOCH FROM (checkin_new - checkout_new))/3600
                                                WHEN checkout_new < '17:00:00' THEN EXTRACT(EPOCH FROM (checkin_new - checkout_new))/3600
                                                WHEN checkout_new > '17:00:00' THEN EXTRACT(EPOCH FROM (checkout_new - checkin_new))/3600
                                                ELSE 9.0 END AS total_hours 
                                        FROM timesheets_modify),
                                    gross_total_hours AS (
                                        SELECT 
                                            count(t1.employee_id) as total_day,
                                            t1.employee_id,
                                            t2.branch_id,
                                            SUM(t1.total_hours) AS total_hours,
                                            min(t2.salary) as salary,
                                            EXTRACT(YEAR FROM date) AS year,
                                            EXTRACT(MONTH FROM date) AS month
                                            FROM timesheets_duration t1
                                            LEFT JOIN employees t2
                                            ON t1.employee_id = t2.employee_id 
                                        WHERE salary is not null 
                                        GROUP BY t1.employee_id,t2.branch_id ,EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)
                                        ),
                                    prorated_salary AS (
                                        SELECT 
                                            total_day,
                                            employee_id,
                                            branch_id,
                                            total_hours,
                                            salary,
                                            year,
                                            month,
                                            CASE 
                                                WHEN total_day > 22 THEN salary
                                                ELSE ROUND((total_day/22.0),2) * salary
                                                END AS prorated_salary
                                        FROM gross_total_hours
                                        ),
                                        salary_per_hour_calculation AS ( 
                                        SELECT 
                                            total_day,
                                            employee_id,
                                            branch_id,
                                            total_hours,
                                            salary,
                                            year,
                                            month,
                                            prorated_salary/total_hours AS salary_per_hour
                                        FROM prorated_salary),
                                        branch_salary_per_hour AS (
                                            SELECT 
                                                branch_id,
                                                year,
                                                month,
                                                AVG(salary_per_hour) AS salary_per_hour
                                            FROM salary_per_hour_calculation
                                            GROUP BY branch_id, year, month)
                                        SELECT 
                                            branch_id,
                                            year::INT,
                                            month::INT,
                                            ROUND(salary_per_hour::NUMERIC,2) AS salary_per_hour
                                        FROM branch_salary_per_hour
                                        ORDER BY branch_id ASC
                    """}

    transform_salary(
        transform_dir=tables_config['folder_path'],
        query=tables_config['query']
        )