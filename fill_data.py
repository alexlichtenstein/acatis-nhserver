import pandas as pd
import pyodbc

# Load the data from Excel file
file_path = 'input_data.xlsx'  # Replace with the path to your .xlsx file
data = pd.read_excel(file_path)

# Database connection parameters
server = 'aogency-acatis.database.windows.net'  # Replace with your SQL server address
database = 'acatis_msci'  # Replace with your database name
username = 'aogency@aogency-acatis'  # Replace with your username
password = 'Acatis2023!'  # Replace with your password
cnxn_string = f'Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:aogency-acatis.database.windows.net,1433;Database=acatis_msci;Uid=aogency;Pwd=Acatis2023!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

# Establish a connection to the database
cnxn = pyodbc.connect(cnxn_string)
cursor = cnxn.cursor()

# SQL query to insert data
insert_query = '''
    INSERT INTO company (ISSUER_NAME, ISSUERID, ISSUER_TICKER, ISSUER_CUSIP, ISSUER_SEDOL, ISSUER_ISIN, ISSUER_CNTRY_DOMICILE)
    VALUES (?, ?, ?, ?, ?, ?, ?)
'''

# Insert data into the database
for index, row in data.iterrows():
    print(index)
    cursor.execute(insert_query,
                   str(row['ISSUER_NAME']),
                   str(row['ISSUERID']),
                   str(row['ISSUER_TICKER']),
                   str(row['ISSUER_CUSIP']),
                   str(row['ISSUER_SEDOL']),
                   str(row['ISSUER_ISIN']),
                   str(row['ISSUER_CNTRY_DOMICILE']))

# Commit the transactions
cnxn.commit()

# Close the cursor and connection
cursor.close()
cnxn.close()

# Output a confirmation message
print(f"Data from {file_path} has been inserted into the 'company' table in the database.")