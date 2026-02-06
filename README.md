💹 Finance Data Pipeline: Actuals vs. Forecasts

![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Pandas](https://img.shields.io/badge/data-pandas-orange)

This repository contains a modular Python-based ETL (Extract, Transform, Load) pipeline. It automates the collection of financial data from Sage and Microsoft Dynamics AX, merges them into a unified transactions table, and processes Marketing/Tech forecasts for high-level variance analysis in Power BI.
## Project Overview

The goal of this project is to eliminate manual data consolidation. By standardizing diverse Excel exports into a centralized Azure SQL Database, we create a "Single Source of Truth." This allows Power BI to generate real-time dashboards comparing actual spend against forecasted budgets without manual intervention.
## Script Explanations

The project is divided into two logical branches: Actuals (Transactions) and Forecasts.
### 1. Actuals Branch (The Transaction Engine)

These scripts handle the historical spending data.

    Transactions_AX.py:

        Function: Extracts data from multiple monthly AX Excel exports.

        Logic: Filters for specific ledger codes (greater than 5) and required suppliers. It performs a left-join with a master mapping file to categorize data into Level 1 through Level 4 financial hierarchies.

        Output: Loads cleaned AX data into the Transactions_AX SQL table.

    Transactions_Sage.py:

        Function: Processes "Nominal Activity" exports from the Sage accounting system.

        Logic: It identifies the entity (e.g., "Strike" vs "Financial Services") based on the filename. It includes a hardcoded mapping dictionary to resolve missing departmental data for specific vendors like ADP, Zoho, or Rightmove.

        Output: Loads data into the Transactions_Sage SQL table.

    Transactions_Final.py:

        Function: The "Silver Layer" consolidator.

        Logic: It reads the previously uploaded AX and Sage tables from SQL. It standardizes column names (e.g., mapping 'Total' from Sage and 'Amount in reporting currency' from AX into a single 'Amount' column) and appends a Updated_Timestamp to track data freshness.

        Output: Creates the final master table: Transactions_Final.

### 2. Forecast Branch (The Budgetary Layer)

These scripts process the forward-looking financial plans.

    Forecast_Marketing.py & Forecast_Tech.py:

        Function: Ingests departmental forecast models.

        Logic: These scripts handle the "horizontal" nature of forecast files where dates are often column headers. They use a format_header helper function to convert Excel date objects into standardized dd/mm/yyyy strings and round financial values to two decimal places for precision.

        Output: Updates the Forecast_Marketing and Forecast_Tech tables in the database.

## Technical Implementation Details
### Data Transformation Workflow

    URL Encoding: Passwords and drivers are encoded using quote_plus to ensure the SQLAlchemy connection string handles special characters securely.

    Path Management: The pathlib library is used for "globbing" (searching) through directories to find the most recent Excel files automatically.

    SQL Integration: Data is pushed to SQL using the to_sql method with if_exists='replace', ensuring the tables are refreshed with the latest data every time the script runs.

### Requirements

To run this project, you will need:

    Python 3.8+

    Microsoft ODBC Driver 17 for SQL Server

    Libraries: pandas, sqlalchemy, pyodbc, openpyxl
