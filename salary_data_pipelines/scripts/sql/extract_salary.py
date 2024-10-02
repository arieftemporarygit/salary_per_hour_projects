import psycopg2

def connect_to_db():
    """Connect to the PostgreSQL database."""
    try:
        conn = psycopg2.connect("dbname='airflow' user='airflow' password='airflow' host='postgres'")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def create_table(cursor, table_name, columns):
    """Create a table if it doesn't exist."""
    column_definitions = ", ".join(f"{col} {dtype}" for col, dtype in columns.items())
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                        {column_definitions}
                        )""")

def create_staging_table(cursor, staging_table_name, columns):
    """Create a staging table if it doesn't exist."""
    column_definitions = ", ".join(f"{col} {dtype}" for col, dtype in columns.items())
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {staging_table_name} (
                        {column_definitions}
                        )""")

def load_data_into_staging(cursor, csv_file_path, staging_table_name):
    """Load data from the CSV file into the staging table."""
    with open(csv_file_path, 'r') as f:
        next(f)  # Skip the header if present
        cursor.copy_expert(f"COPY {staging_table_name} FROM STDIN WITH CSV", f)

def insert_data_from_staging(cursor, staging_table_name, table_name, columns):
    """Move data from staging table to the main table."""
    column_list = ", ".join(columns)
    cursor.execute(f"""
    INSERT INTO {table_name} ({column_list})
    SELECT {column_list} FROM {staging_table_name};
    """)

def truncate_table(cursor, table_name):
    """Truncate table."""
    cursor.execute(f"TRUNCATE TABLE {table_name};")

def drop_staging_table(cursor, staging_table_name):
    """Drop the staging table."""
    cursor.execute(f"DROP TABLE IF EXISTS {staging_table_name};")

def extract_salary(csv_file_path, staging_table_name, main_table_name, columns):
    """Main function to load data from a CSV file into the database."""
    conn = connect_to_db()
    if conn is None:
        return  # Exit if connection fails

    cursor = conn.cursor()
    
    try:
        create_table(cursor, main_table_name, columns)
        create_staging_table(cursor, staging_table_name, columns)
        load_data_into_staging(cursor, csv_file_path, staging_table_name)
        
        # Truncate the main table before inserting data
        truncate_table(cursor, main_table_name)
        
        insert_data_from_staging(cursor, staging_table_name, main_table_name, columns)
        drop_staging_table(cursor, staging_table_name)

        # Commit the changes
        conn.commit()

    except Exception as e:
        print(f"An error occurred during the database operation: {e}")
        conn.rollback()  # Rollback if there's an error
    finally:
        # Close the connection
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Define configurations for the tables to be processed
    tables_config = [
        {
            'csv_file_path': '/opt/airflow/data/timesheets.csv',
            'staging_table_name': 'timesheets_staging',
            'main_table_name': 'timesheets',
            'columns': {
                'timesheet_id': 'INT',
                'employee_id': 'INT',
                'date': 'DATE',
                'checkin': 'TIME',
                'checkout': 'TIME'
            }
        },
        {
            'csv_file_path': '/opt/airflow/data/employees.csv',
            'staging_table_name': 'employees_staging',
            'main_table_name': 'employees',
            'columns': {
                'employee_id': 'INT',
                'branch_id': 'INT',
                'salary': 'DECIMAL(10, 2)',
                'join_date': 'DATE',
                'resign_date': 'DATE'
            }
        }
    ]

    # Process each table configuration
    for config in tables_config:
        extract_salary(
            csv_file_path=config['csv_file_path'],
            staging_table_name=config['staging_table_name'],
            main_table_name=config['main_table_name'],
            columns=config['columns']
        )