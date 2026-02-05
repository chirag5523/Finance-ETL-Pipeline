from urllib.parse import quote_plus
from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path

# ==========================================
# 1. Credentials and connection details
# ==========================================
username = 'YOUR_USERNAME'
password = quote_plus('YOUR_PASSWORD') # URL encode the password
hostname = 'your-server-name.database.windows.net' #
database_name = 'Finance' #
driver = quote_plus('ODBC Driver 17 for SQL Server') # URL encode the driver name

# Construct the connection string with URL encoding
connection_string = f"mssql+pyodbc://{username}:{password}@{hostname}/{database_name}?driver={driver}"

# ==========================================
# 2. Helper Functions
# ==========================================
def find_specific_excel_file(base_dir, file_name):
    """Find a specific Excel file in a directory."""
    full_path = Path(base_dir) #
    excel_files = [file for file in full_path.rglob(file_name)] #
    print(f"Looking in: {full_path}") # Diagnostic print
    print(f"Files found: {excel_files}") # Diagnostic print
    return excel_files

def export_to_sql(df, table_name, sql_connection_string):
    """Export DataFrame to a SQL table."""
    engine = create_engine(sql_connection_string) #
    df.to_sql(table_name, con=engine, if_exists='replace', index=False) #
    print(f"Data has been exported to the SQL server table: {table_name}")

# ==========================================
# 3. Configuration
# ==========================================
# Anonymized directory for Marketing Forecasts
root_directory = r'C:\Users\YourUser\Path\To\Forecast Modelling\Marketing' #

# Specific Excel file for Marketing
supplier_file_name = "Marketing - Forecast File.xlsx" #

# ==========================================
# 4. Processing Logic
# ==========================================
excel_files = find_specific_excel_file(root_directory, supplier_file_name) #

if not excel_files:
    print("No Excel files found.") #
else:
    try:
        # Load the Excel file specifically from the Marketing Spend sheet
        # Update sheet_name if the Marketing sheet is named differently
        combined_df = pd.read_excel(excel_files[0], sheet_name='Marketing Spend Consolidated') #

        # Update the column headers format (Standardizes date headers)
        def format_header(header):
            try:
                # Converts date headers to dd/mm/yyyy format
                return pd.to_datetime(header).strftime('%d/%m/%Y')
            except ValueError:
                return header #

        combined_df.columns = [format_header(col) for col in combined_df.columns] #

        # Round values to two decimal places
        # Note: Index range [15:39] corresponds to columns P through AM
        columns_to_round = combined_df.columns[15:39] 
        combined_df[columns_to_round] = combined_df[columns_to_round].round(2) #

        # Export the DataFrame to SQL
        export_to_sql(combined_df, 'Forecast_Marketing', connection_string) #
        print("Exported final merged Marketing data to SQL.")

    except Exception as e:
        print(f"An error occurred: {e}") #