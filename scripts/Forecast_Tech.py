from urllib.parse import quote_plus
from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path

# Credentials and connection details
username = 'YOUR_USERNAME'
password = quote_plus('YOUR_PASSWORD') # URL encode the password
hostname = 'your-server-name.database.windows.net'
database_name = 'Finance'
driver = quote_plus('ODBC Driver 17 for SQL Server') # URL encode the driver name

# Construct the connection string with URL encoding
connection_string = f"mssql+pyodbc://{username}:{password}@{hostname}/{database_name}?driver={driver}"

def find_specific_excel_file(base_dir, file_name):
    """Find a specific Excel file in a directory."""
    full_path = Path(base_dir)
    excel_files = [file for file in full_path.rglob(file_name)]
    print(f"Looking in: {full_path}") # Diagnostic print
    print(f"Files found: {excel_files}") # Diagnostic print
    return excel_files

def export_to_sql(df, table_name, sql_connection_string):
    """Export DataFrame to a SQL table."""
    engine = create_engine(sql_connection_string)
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print("Data has been exported to the SQL server.")

# Configuration
root_directory = r'C:\Users\AnonymizedPath\Forecast Modelling\Tech'

# Find the specific Excel file
supplier_file_name = "Tech - Forecast File.xlsx"
excel_files = find_specific_excel_file(root_directory, supplier_file_name)

if not excel_files:
    print("No Excel files found.")
else:
    try:
        # Load the first found Excel file into a DataFrame
        combined_df = pd.read_excel(excel_files[0], sheet_name='Tech Spend Consolidated')

        # Update the column headers format
        def format_header(header):
            try:
                return pd.to_datetime(header).strftime('%d/%m/%Y')
            except ValueError:
                return header

        combined_df.columns = [format_header(col) for col in combined_df.columns]

        # Round values in specific columns (P to AM) to two decimal places
        columns_to_round = combined_df.columns[15:39] # Adjust range as per actual positions
        combined_df[columns_to_round] = combined_df[columns_to_round].round(2)

        # Export the DataFrame to SQL
        export_to_sql(combined_df, 'Forecast_Tech', connection_string)
        print("Exported final merged data to SQL.")

    except Exception as e:
        print(f"An error occurred: {e}")