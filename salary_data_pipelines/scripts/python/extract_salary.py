import pandas as pd
from sqlalchemy import create_engine, text

# Function to check if the table exists in the database
def check_table_exists(engine, table_name):
    query = text(f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = '{table_name}'
    );
    """)
    with engine.connect() as conn:
        result = conn.execute(query).scalar()
    return result

# Function to load CSV data
def load_csv_data(csv_file_path):
    new_data = pd.read_csv(csv_file_path)

    # Handling specific cases (rename column as needed for the employee dataset)
    if csv_file_path == 'data/employees.csv':
        new_data.rename(columns={'employe_id': 'employee_id'}, inplace=True)

    return new_data

# Main function for incremental update
def incremental_update(engine, csv_file_path, table_name, update_keys):
    # Ensure update_keys is a list
    if not isinstance(update_keys, list):
        update_keys = [update_keys]

    # Load the new data from the CSV file
    new_data = load_csv_data(csv_file_path)

    # First time: create the table and insert all data if the table doesn't exist
    if not check_table_exists(engine, table_name):
        print(f"Table '{table_name}' does not exist. Creating it now and uploading the data...")

        # Create the table with the schema based on the CSV data
        new_data.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"Table '{table_name}' created and data uploaded.")

        # Since this is the first run, the total incremental updates are all the rows in the new data
        total_updates = len(new_data)
        print(f"Total incremental updates (first run): {total_updates} records inserted.")
    else:
        print(f"Table '{table_name}' exists. Performing incremental update...")

        # Load the existing data from the database
        existing_data = pd.read_sql_table(table_name, con=engine)

        # Ensure all update_keys exist in both datasets
        for key in update_keys:
            if key not in new_data.columns or key not in existing_data.columns:
                raise KeyError(f"Both the new data and existing data must have the '{key}' column for the incremental update.")

        # Merge the existing data with the new data based on the update_keys
        # We update rows with the same update_keys and append new rows that don't exist
        merged_data = pd.concat([existing_data, new_data]).drop_duplicates(subset=update_keys, keep='last')
        # Count the number of new or updated records
        updated_or_new_data = merged_data[~merged_data[update_keys].apply(tuple, 1).isin(existing_data[update_keys].apply(tuple, 1))]
        total_updates = len(updated_or_new_data)

        # Write the merged DataFrame back to the database (replace existing data)
        updated_or_new_data.to_sql(table_name, con=engine, if_exists='append', index=False)

        print(f"Incremental update completed successfully.")
        print(f"Total incremental updates: {total_updates} records inserted or updated.")

if __name__ == "__main__":
        # Database connection parameters for PostgreSQL
    DB_USER = 'airflow'
    DB_PASSWORD = 'airflow'
    DB_HOST = 'postgres'
    DB_PORT = '5432'
    DB_NAME = 'airflow'

    # Connect to PostgreSQL
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')


    csv_files = [
        {'path': 'data/employees.csv', 'table': 'employees_python', 'keys': ['employee_id','branch_id']},
        {'path': 'data/timesheets.csv', 'table': 'timesheets_python', 'keys': ['timesheet_id']}
    ]

    for file in csv_files:
        incremental_update(engine, file['path'], file['table'], file['keys'])
