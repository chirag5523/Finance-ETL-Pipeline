from urllib.parse import quote_plus
from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path
from datetime import datetime

# ==========================================
# 1. Credentials and connection details
# ==========================================
# Anonymized credentials
username = 'YOUR_USERNAME'
password = quote_plus('YOUR_PASSWORD') # URL encode the password
hostname = 'your-server-name.database.windows.net'
database_name = 'Finance'
driver = quote_plus('ODBC Driver 17 for SQL Server') # URL encode the driver name

# Construct the connection string with URL encoding
connection_string = f"mssql+pyodbc://{username}:{password}@{hostname}/{database_name}?driver={driver}"

# ==========================================
# 2. Helper Functions
# ==========================================
def export_to_sql(df, table_name, sql_connection_string, if_exists='replace'):
    """Export DataFrame to a SQL table."""
    engine = create_engine(sql_connection_string)
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
    print(f"Data has been exported to the SQL table {table_name}.")

def read_sql_table(sql_connection_string, table_name, columns):
    """Read specific columns from a SQL table."""
    engine = create_engine(sql_connection_string)
    # Escaping column names with square brackets for SQL Server compatibility
    columns_escaped = ", ".join([f"[{column}]" for column in columns])
    query = f"SELECT {columns_escaped} FROM [{table_name}]"
    df = pd.read_sql(query, con=engine)
    return df

# ==========================================
# 3. Process Transactions_AX
# ==========================================
# Read data from 'Transactions_AX' table
transactions_ax_columns = [
    'Company', 'Date', 'Supplier Account AX', 'Amount in reporting currency', 
    'Supplier Name AX', 'Account name', 'Level 1', 'Level 2', 'Level 3', 
    'Level 4', 'MainAccount', 'Department', 'Cost Center', 'Posting type'
]
transactions_ax_df = read_sql_table(connection_string, 'Transactions_AX', transactions_ax_columns)

# Rename columns to match the final table schema
transactions_ax_df.rename(columns={
    'Supplier Account AX': 'Supplier Account',
    'Amount in reporting currency': 'Amount',
    'Supplier Name AX': 'Supplier Name',
    'Account name': 'Account Name',
    'MainAccount': 'Main Account'
}, inplace=True)

# ==========================================
# 4. Process Transactions_Sage
# ==========================================
# Read data from 'Transactions_Sage' table
transactions_sage_columns = [
    'Company', 'Date', 'Account', 'Total', 'Supplier Name',
    'Name', 'Level 1', 'Level 2', 'Level 3', 'Level 4', 'N/C:', 
    'Department', 'Cost Center', 'Type'
]
transactions_sage_df = read_sql_table(connection_string, 'Transactions_Sage', transactions_sage_columns)

# Rename columns to match the final table schema
transactions_sage_df.rename(columns={
    'Account': 'Supplier Account',
    'Total': 'Amount',
    'Name': 'Account Name',
    'N/C:': 'Main Account',
    'Type': 'Posting type'
}, inplace=True)

# ==========================================
# 5. Combine and Finalize
# ==========================================
# Combine data from both tables
transactions_final_df = pd.concat([transactions_ax_df, transactions_sage_df], ignore_index=True)

# Fill None values in 'Cost Center' with 0 and then convert to integer
transactions_final_df['Cost Center'] = transactions_final_df['Cost Center'].fillna(0).astype(int)

# Update 'Amount' to have 2 decimal places
transactions_final_df['Amount'] = transactions_final_df['Amount'].round(2)

# Add the updated_timestamp column
transactions_final_df['Updated_Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Export the combined data to 'Transactions_Final' table
final_table_name = 'Transactions_Final'
export_to_sql(transactions_final_df, final_table_name, connection_string, if_exists='replace')

print("Data has been exported to 'Transactions_Final' successfully.")