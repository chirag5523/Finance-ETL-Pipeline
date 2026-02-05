from sqlalchemy import create_engine
import pandas as pd
import re
from pathlib import Path
from urllib.parse import quote_plus

# ==========================================
# 1. Credentials and connection details
# ==========================================
username = 'YOUR_USERNAME'
password = quote_plus('YOUR_PASSWORD')  # URL encode the password
hostname = 'your-server-name.database.windows.net'
database_name = 'YourDatabaseName'
driver = quote_plus('ODBC Driver 17 for SQL Server') 

# Construct the connection string
connection_string = f"mssql+pyodbc://{username}:{password}@{hostname}/{database_name}?driver={driver}"

# ==========================================
# 2. Helper Functions
# ==========================================
def find_specific_excel_file(base_dir, file_name):
    """Find a specific Excel file in a designated directory."""
    full_path = Path(base_dir)
    excel_files = [file for file in full_path.rglob(file_name)]
    print(f"Looking in: {full_path}") 
    print(f"Files found: {excel_files}")
    return excel_files

def export_to_sql(df, table_name, sql_connection_string, if_exists='replace'):
    """Export DataFrame to a SQL table."""
    engine = create_engine(sql_connection_string)
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
    print(f"Data has been exported to the SQL table: {table_name}")

def read_excel_data(file_path, sheet_name, start_row, columns):
    """Read specific data from an Excel file."""
    df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=start_row-1)
    
    # Select only the required columns that actually exist in the file
    existing_cols = [c for c in columns if c in df.columns]
    df = df[existing_cols]
    
    # Format the date column if it exists
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%d/%m/%Y')
        
    return df

# ==========================================
# 3. Configuration & Paths
# ==========================================
raw_data_directory = r'C:\Users\YourUser\Path\To\Raw_Data\AX'
supplier_mapping_directory = r'C:\Users\YourUser\Path\To\Supplier_Mapping'

transactions_files = [
    'January 2024 - AX.xlsx', 'February 2024 - AX.xlsx', 'March 2024 - AX.xlsx',
    'April 2024 - AX.xlsx', 'May 2024 - AX.xlsx', 'June 2024 - AX.xlsx',
    'July 2024 - AX.xlsx', 'August 2024 - AX.xlsx', 'September 2024 - AX.xlsx',
    'October 2024 - AX.xlsx', 'November 2024 - AX.xlsx', 'December 2024 - AX.xlsx',
    '13.January 2025 - AX.xlsx'
]

mapping_file_name = 'Mapping_Consolidated.xlsx'
mapping_supplier_file_name = 'Mapping_AX.xlsx'

# ==========================================
# 4. Main Processing Logic
# ==========================================

# Find the files
transactions_excel_files = []
for f_name in transactions_files:
    transactions_excel_files += find_specific_excel_file(raw_data_directory, f_name)

mapping_excel_files = find_specific_excel_file(supplier_mapping_directory, mapping_file_name)
mapping_supplier_excel_files = find_specific_excel_file(supplier_mapping_directory, mapping_supplier_file_name)

if transactions_excel_files and mapping_excel_files and mapping_supplier_excel_files:
    # Setup column requirements
    transactions_columns = [
        'Journal number', 'Voucher', 'Date', 'Year closed', 'Ledger account',
        'Account name', 'Description', 'Currency', 'Amount in transaction currency',
        'Amount', 'Amount in reporting currency', 'Posting type', 'Posting layer',
        'Supplier Name AX', 'Supplier Account AX', 'MainAccount', 'Supplier Required', 'Ledger Code'
    ]
    
    mapping_columns = ['MainAccount', 'Company', 'FS type', 'Level 1', 'Level 2', 'Level 3', 'Level 4']
    mapping_supplier_columns = ['Supplier Name AX', 'Department', 'Cost Center']

    # Load and concatenate Transactions
    transactions_dfs = []
    for path in transactions_excel_files:
        df = read_excel_data(path, 'Sheet1', 1, transactions_columns)
        # Filter: Supplier Required is True and Ledger Code > 5
        df = df[(df['Supplier Required'] == True) & (df['Ledger Code'] > 5)]
        transactions_dfs.append(df)
        
    transactions_df = pd.concat(transactions_dfs, ignore_index=True)
    
    # Load Mapping Files
    mapping_df = read_excel_data(mapping_excel_files[0], 'Sheet1', 1, mapping_columns)
    mapping_supplier_df = read_excel_data(mapping_supplier_excel_files[0], 'Sheet1', 1, mapping_supplier_columns)
    
    # ---------------------------------------------------------
    # IMPROVED JOIN LOGIC
    # ---------------------------------------------------------
    # 1. Join with Main Mapping (on MainAccount)
    merged_df = pd.merge(transactions_df, mapping_df, on='MainAccount', how='left')
    
    # 2. Join with Supplier Mapping (on Supplier Name AX)
    # Ensure no duplicates in lookup to avoid row explosion
    mapping_supplier_lookup = mapping_supplier_df.drop_duplicates('Supplier Name AX')
    merged_df = pd.merge(merged_df, mapping_supplier_lookup, on='Supplier Name AX', how='left')
    
    # Clean up formatting
    merged_df['Description'] = merged_df['Description'].astype(str)
    
    # Format Cost Center to remove .0 decimals
    def clean_cost_center(x):
        try:
            if pd.notnull(x) and x != '':
                return str(int(float(x)))
            return x
        except:
            return str(x)

    if 'Cost Center' in merged_df.columns:
        merged_df['Cost Center'] = merged_df['Cost Center'].apply(clean_cost_center)
    
    # ---------------------------------------------------------
    # 5. Export to SQL
    # ---------------------------------------------------------
    export_to_sql(merged_df, 'Transactions_AX', connection_string)
    print("Process complete!")

else:
    print("Error: One or more required Excel files were not found.")