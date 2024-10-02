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

def load_data(cursor, csv_file_path, table_name):
    """Load data from the CSV file into the table."""
    with open(csv_file_path, 'r') as f:
        next(f)  # Skip the header if present
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", f)

def transfer_data(cursor, staging_table_name, main_table_name, columns):
    """Transfer data from staging table to main table dynamically based on column names."""
    column_names = ", ".join(columns.keys())  # Get the column names as a string
    placeholders = ", ".join([f"{col}" for col in columns.keys()])  # Generate the placeholders dynamically
    
    query = f"""
        INSERT INTO {main_table_name} ({column_names})
        SELECT {placeholders} FROM {staging_table_name};"""
    cursor.execute(query)

def truncate_table(cursor, table_name):
    """Truncate table."""
    cursor.execute(f"TRUNCATE TABLE {table_name};")

def load_salary(csv_file_path, main_table_name, staging_table_name, columns):
    """Main function to load data from a CSV file into the database."""
    conn = connect_to_db()
    if conn is None:
        return  # Exit if connection fails

    cursor = conn.cursor()
    
    try:
        create_table(cursor, staging_table_name, columns)  # Create staging table
        create_table(cursor, main_table_name, columns)      # Create main table
        
        load_data(cursor, csv_file_path, staging_table_name)  # Load data into staging table
        truncate_table(cursor, main_table_name)
        transfer_data(cursor, staging_table_name, main_table_name, columns)  # Transfer data to main table
        
        # Optionally, drop the staging table if no longer needed
        cursor.execute(f"DROP TABLE IF EXISTS {staging_table_name};")

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
            'csv_file_path': '/opt/airflow/data/transform/sql/salary_per_hours.csv',
            'main_table_name': 'salary_per_hours_sql',
            'staging_table_name': 'staging_salary_per_hours',
            'columns': {
                'branch_id': 'INT',
                'year': 'INT',
                'month': 'INT',
                'salary_per_hour': 'DECIMAL(10,2)'
            }
        }
    ]

    # Process each table configuration
    for config in tables_config:
        load_salary(
            csv_file_path=config['csv_file_path'],
            main_table_name=config['main_table_name'],
            staging_table_name=config['staging_table_name'],
            columns=config['columns']
        )
