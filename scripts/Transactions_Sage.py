from urllib.parse import quote_plus
from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path

# ==========================================
# 1. Credentials and connection details
# ==========================================
username = 'YOUR_USERNAME'
password = quote_plus('YOUR_PASSWORD')  # URL encode the password
hostname = 'your-server-name.database.windows.net'
database_name = 'YourDatabaseName'
driver = quote_plus('ODBC Driver 17 for SQL Server') 

# Construct the connection string with URL encoding
connection_string = f"mssql+pyodbc://{username}:{password}@{hostname}/{database_name}?driver={driver}"

# ==========================================
# 2. Configuration & File Setup
# ==========================================
# Anonymized folder paths
root_directory = r'C:\Users\YourUser\Path\To\Raw_Data\SAGE'
mapping_file_path = Path(r'C:\Users\YourUser\Path\To\Supplier_Mapping\Mapping_Sage.xlsx')

transactions_file_names = [
    '1.Strike FS Jan 2024 Nominal Activity.xlsx',
    '2.Strike FS Feb 2024 Nominal Activity.xlsx',
    '3.Strike FS Mar 2024 Nominal Activity.xlsx',
    '4.Strike FS Apr 2024 Nominal Activity.xlsx',
    '5.Strike FS May 2024 Nominal Activity.xlsx',
    '6.Strike FS June 2024 Nominal Activity.xlsx',
    '7.Strike FS July 2024 Nominal Activity.xlsx',
    '8.Strike FS August 2024 Nominal Activity.xlsx',
    '9.Strike FS September 2024 Nominal Activity.xlsx',
    '10.Strike FS October 2024 Nominal Activity.xlsx',
    '11.Strike FS November 2024 Nominal Activity.xlsx',
    '12.Strike FS December 2024 Nominal Activity.xlsx',
    '13.Strike FS January 2025 Nominal Activity.xlsx',
    '14.Strike FS February 2025 Nominal Activity.xlsx',
    '1.Strike Jan 2024 Nominal Activity.xlsx',
    '2.Strike Feb 2024 Nominal Activity.xlsx',
    '3.Strike Mar 2024 Nominal Activity.xlsx',
    '4.Strike Apr 2024 Nominal Activity.xlsx',
    '5.Strike May 2024 Nominal Activity.xlsx',
    '6.Strike June 2024 Nominal Activity.xlsx',
    '7.Strike July 2024 Nominal Activity.xlsx',
    '8.Strike August 2024 Nominal Activity.xlsx',
    '9.Strike September 2024 Nominal Activity.xlsx',
    '10.Strike October 2024 Nominal Activity.xlsx',
    '11.Strike November 2024 Nominal Activity.xlsx',
    '12.Strike December 2024 Nominal Activity.xlsx',
    '13.Strike January 2025 Nominal Activity.xlsx',
    '14.Strike February 2025 Nominal Activity.xlsx'
]

sheet_name = 'Nominal Activity - Excluding N'

# ==========================================
# 3. Data Loading & Initial Cleaning
# ==========================================
# Read mapping data
mapping_data = pd.read_excel(mapping_file_path, sheet_name='Sheet1')

# Ensure 'Account' column is a string and trim spaces
mapping_data['Account '] = mapping_data['Account '].astype(str).str.strip()
mapping_data.drop(columns=['Account '], inplace=True)

# Read data from both Excel files and concatenate them
data_frames = []
for file_name in transactions_file_names:
    excel_file_path = Path(root_directory) / file_name
    # Header starts at row 9 (header=8 in zero-indexed pandas)
    data = pd.read_excel(excel_file_path, sheet_name=sheet_name, header=8)
    
    # Assign company name based on filename
    company_name = 'Financial Services' if 'FS' in file_name else 'Strike'
    data['Company'] = company_name
    data_frames.append(data)

combined_data = pd.concat(data_frames, ignore_index=True)

# Filter out rows where the 'N/C:' column is NULL
filtered_data = combined_data.dropna(subset=['N/C:'])

# Select only the required columns
required_columns = [
    'Company', 'N/C:', 'No', 'Type', 'Date', 'Account ', 
    'Ref', 'Details', 'T/C', 'Total', 'Supplier Name', 'Company/Account'
]
filtered_data = filtered_data[required_columns]

# Format the Date column
if 'Date' in filtered_data.columns:
    filtered_data['Date'] = pd.to_datetime(filtered_data['Date']).dt.strftime('%d/%m/%Y')

# Rename 'Account ' for merging and clean strings
filtered_data.rename(columns={'Account ': 'Account'}, inplace=True)
filtered_data['Account'] = filtered_data['Account'].astype(str).str.strip()
filtered_data['Company/Account'] = filtered_data['Company/Account'].astype(str).str.strip()

# ==========================================
# 4. Merging & Special Mappings
# ==========================================
mapping_columns = ['Company/Account', 'Name', 'Level 1', 'Level 2', 'Level 3', 'Level 4', 'Cost Center', 'Department']
merged_data = pd.merge(filtered_data, mapping_data[mapping_columns], on='Company/Account', how='left')

# Specific mappings to resolve NULL 'Department' entries based on Account name
additional_mappings = {
    'ADP': {'Cost Center': 170, 'Department': 'Finance'},
    'BIRKETTS': {'Cost Center': 170, 'Department': 'Finance'},
    'ENJOY': {'Cost Center': 180, 'Department': 'People'},
    'GIRAFFE': {'Cost Center': 250, 'Department': 'Partnerships'},
    'GOTO': {'Cost Center': 250, 'Department': 'Partnerships'},
    'HOUSE': {'Cost Center': 130, 'Department': 'Head Office (utilities & other)'},
    'MAB': {'Cost Center': 130, 'Department': 'Head Office (utilities & other)'},
    'OPTAMOR': {'Cost Center': 180, 'Department': 'People'},
    'PPL': {'Cost Center': 130, 'Department': 'Head Office (utilities & other)'},
    'PRATT': {'Cost Center': 180, 'Department': 'People'},
    'RIGHT': {'Cost Center': 250, 'Department': 'Partnerships'},
    'ZOHO': {'Cost Center': 202, 'Department': 'IT Ops'},
}

# Apply additional mappings only where Department is still null
for account, update_values in additional_mappings.items():
    condition = (merged_data['Account'] == account) & (merged_data['Department'].isnull())
    merged_data.loc[condition, 'Cost Center'] = update_values['Cost Center']
    merged_data.loc[condition, 'Department'] = update_values['Department']

# ==========================================
# 5. SQL Export
# ==========================================
engine = create_engine(connection_string)
table_name = 'Transactions_Sage'
merged_data.to_sql(table_name, con=engine, if_exists='replace', index=False)

print("Data has been successfully imported.")