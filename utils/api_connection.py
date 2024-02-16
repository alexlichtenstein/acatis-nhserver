import http.client
import json
import ssl
import pyodbc
import pandas as pd
from datetime import datetime
import numpy
import warnings

# Function to create an unverified SSL context
def create_unverified_ssl_context():
    return ssl._create_unverified_context()

# Function to generate token
def generate_token(client_id, client_secret):
    context = create_unverified_ssl_context()
    conn = http.client.HTTPSConnection("accounts.msci.com", context=context)
    payload = json.dumps({
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": "https://esg/data"
    })
    headers = {'Content-Type': 'application/json'}
    conn.request("POST", "/oauth/token/", payload, headers)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data.decode("utf-8"))["access_token"]

# Function to make API request
def api_request(token, isin_ids, date, factors):
    context = create_unverified_ssl_context()
    conn = http.client.HTTPSConnection("api.msci.com", context=context)
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    payload = json.dumps({
        "issuer_identifier_list": isin_ids,
        "factor_name_list": factors
    })
    conn.request("POST", "/esg/data/v2.0/issuers", payload, headers)
    res = conn.getresponse()
    data = res.read()
    return data.decode("utf-8")

def healthcheck(token):
    context = create_unverified_ssl_context()
    conn = http.client.HTTPSConnection("api.msci.com", context=context)
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    payload = json.dumps({
        # "issuer_identifier_list": isin_ids,
        # "factor_name_list": factors
    })
    conn.request("GET", "/esg/data/v2.0/healthcheck", payload, headers)
    res = conn.getresponse()
    data = res.read()
    return data.decode("utf-8")

def coverages(token):
    context = create_unverified_ssl_context()
    conn = http.client.HTTPSConnection("api.msci.com", context=context)
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    payload = json.dumps({
        # "issuer_identifier_list": isin_ids,
        # "factor_name_list": factors
    })
    conn.request("GET", "/esg/data/v2.0/parameterValues/coverages", payload, headers)
    res = conn.getresponse()
    data = res.read()
    data_decoded = data.decode("utf-8")

    # Parse the JSON response
    json_response = json.loads(data_decoded)

    # Extract the coverages list
    coverages_list = json_response.get("result", {}).get("coverages", [])

    return coverages_list


def issuers(token, coverage, factor_name_list):
    print(f"Starting collecting data from {coverage}")
    context = create_unverified_ssl_context()
    conn = http.client.HTTPSConnection("api.msci.com", context=context)
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json', 'Accept': '*/*'}

    all_data = []
    offset = 0
    limit = 10000
    total_count = None
    messages = []

    while total_count is None or offset < total_count:
        payload = json.dumps({
            "factor_name_list": factor_name_list,
            "limit": limit,
            "offset": offset,
            "format": "json",
            "coverage": coverage
        })

        conn.request("POST", "/esg/data/v2.0/issuers", payload, headers)
        res = conn.getresponse()

        if res.status != 200:
            print(f"Error: {res.status}, {res.reason}")
            return None

        data = res.read()
        response_json = json.loads(data.decode("utf-8"))
        if "messages" in response_json:
            messages = response_json["messages"]
        if "result" in response_json and "issuers" in response_json["result"]:
            all_data.extend(response_json["result"]["issuers"])

        if "paging" in response_json and "total_count" in response_json["paging"]:
            total_count = response_json["paging"]["total_count"]

        offset += limit
    print(f"{total_count} numbers of data points been successfully fetched")
    fetch_problems = "; ".join(messages)
    print(f"Following problems have occured: {fetch_problems}")
    return all_data, messages

def get_column_names():
    # Connection string
    cnxn_string = (
        'Driver={ODBC Driver 18 for SQL Server};'
        'Server=tcp:aogency-acatis.database.windows.net,1433;'
        'Database=acatis_msci;'
        'Uid=aogency;'
        'Pwd=Acatis2023!;'
        'Encrypt=yes;'
        'TrustServerCertificate=no;'
        'Connection Timeout=30;'
    )

    # Connect to the database
    cnxn = pyodbc.connect(cnxn_string)
    cursor = cnxn.cursor()

    # Query to fetch column names from the table
    query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'company_data'"

    # Execute the query
    cursor.execute(query)

    # Fetch all column names and exclude specific columns
    excluded_columns = {'DataID', 'CompanyID', 'DataDate'}
    columns = [row.COLUMN_NAME for row in cursor.fetchall() if row.COLUMN_NAME not in excluded_columns]

    # Close the connection
    cursor.close()
    cnxn.close()

    return columns

def sync_issuers_with_database(issuer_response):
    # Connection string
    cnxn_string = (
        'Driver={ODBC Driver 18 for SQL Server};'
        'Server=tcp:aogency-acatis.database.windows.net,1433;'
        'Database=acatis_msci;'
        'Uid=aogency;'
        'Pwd=Acatis2023!;'
        'Encrypt=yes;'
        'TrustServerCertificate=no;'
        'Connection Timeout=30;'
    )

    # Connect to the database
    cnxn = pyodbc.connect(cnxn_string)
    cursor = cnxn.cursor()

    def fetch_api_data():
        # Assuming issuer_response is a list of dictionaries
        # Filter the required data and convert it to a DataFrame
        filtered_data = [{'ISSUER_NAME': item['ISSUER_NAME'],
                          'ISSUERID': item['ISSUERID'],
                          'ISSUER_ISIN': item['ISSUER_ISIN']} for item in issuer_response]
        return pd.DataFrame(filtered_data)

    def fetch_database_data():
        # Database connection code
        query = "SELECT ISSUER_NAME, ISSUERID, ISSUER_ISIN FROM company"
        # Assuming cnxn is your database connection
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            return pd.read_sql(query, cnxn)

    def insert_new_records(new_records):
        print("Check if new ISINs should be added to the company table.")
        # Database connection code
        inserted_count = 0
        for _, row in new_records.iterrows():
            cursor.execute("INSERT INTO company (ISSUER_NAME, ISSUERID, ISSUER_ISIN) VALUES (?, ?, ?)",
                           row['ISSUER_NAME'], row['ISSUERID'], row['ISSUER_ISIN'])
            inserted_count += 1
        cnxn.commit()
        return inserted_count

    # Fetch and filter API data
    df_api = fetch_api_data()

    # Fetch data from database
    database_data = fetch_database_data()

    # Compare and find new records
    new_records = df_api[~df_api['ISSUER_ISIN'].isin(database_data['ISSUER_ISIN'])]

    # Insert new records into the database and print logs
    if not new_records.empty:
        inserted_count = insert_new_records(new_records)
        print(f"{inserted_count} new ISINs have been added to the database.")
    else:
        print("No new ISINs to add to the companies(ISINS) table.")

    # Close the connection
    cursor.close()
    cnxn.close()

def insert_issuer_data(issuer_response):
    # Connection string
    cnxn_string = (
        'Driver={ODBC Driver 18 for SQL Server};'
        'Server=tcp:aogency-acatis.database.windows.net,1433;'
        'Database=acatis_msci;'
        'Uid=aogency;'
        'Pwd=Acatis2023!;'
        'Encrypt=yes;'
        'TrustServerCertificate=no;'
        'Connection Timeout=30;'
    )

    # Connect to the database
    cnxn = pyodbc.connect(cnxn_string)
    cursor = cnxn.cursor()

    def fetch_company_ids():
        query = "SELECT CompanyID, ISSUER_ISIN FROM company"
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            return pd.read_sql(query, cnxn)

    def fetch_table_columns():
        query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'company_data'"
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            return pd.read_sql(query, cnxn)['COLUMN_NAME'].tolist()

    def insert_data(filtered_data):
        inserted_count = 0
        for data in filtered_data:
            data_columns = [col for col in data if col != 'DataID']
            columns = ', '.join(data_columns)
            placeholders = ', '.join(['?'] * len(data_columns))
            sql = f"INSERT INTO company_data ({columns}) VALUES ({placeholders})"

            # Convert values to appropriate types
            values = []
            for col in data_columns:
                value = data[col]
                if col == 'CompanyID' and isinstance(value, numpy.generic):
                    # Convert numpy types to native Python integer
                    values.append(int(value))
                elif col == 'DataDate':
                    # Keep DataDate as date
                    values.append(value)
                elif isinstance(value, numpy.generic):
                    # Convert other numpy types to native Python types
                    values.append(value.item())
                else:
                    # Convert other types to string
                    values.append(str(value))

            cursor.execute(sql, values)
            inserted_count += 1
        cnxn.commit()
        return inserted_count

    # Fetch CompanyID and ISSUER_ISIN mappings
    company_ids_df = fetch_company_ids()

    # Fetch column names from the database
    table_columns = fetch_table_columns()

    # Filter API response to match table columns, add DataDate, and map CompanyID
    current_time = datetime.now()
    filtered_api_data = []
    for item in issuer_response:
        filtered_item = {key: item[key] for key in item if key in table_columns and key != 'DataID'}
        filtered_item['DataDate'] = current_time

        # Map CompanyID using ISSUER_ISIN
        issuer_isin = item.get('ISSUER_ISIN')
        company_id = company_ids_df.loc[company_ids_df['ISSUER_ISIN'] == issuer_isin, 'CompanyID'].values
        if company_id.size > 0:
            filtered_item['CompanyID'] = company_id[0]
            filtered_api_data.append(filtered_item)

    # Insert the data into the database
    if filtered_api_data:
        inserted_count = insert_data(filtered_api_data)
        print(f"{inserted_count} new records have been added to the company_data table.")
    else:
        print("No new records to insert.")

    # Close the connection
    cursor.close()
    cnxn.close()


client_id = "a568Wa48TM3xzfeOT8xxe3V5VJzo4Mfb"
client_secret = "S1HM7CrxsnbUMRTUkn8o8-t-_OEYnSfXLyaze0IpgX1vPDweBW35wHzmidyvWxd6"
# Generate token
token = generate_token(client_id, client_secret)
print(f"Token for API been successfully generated.")
# print('123')
covs = coverages(token)
cov_string = ", ".join(covs)
print(f"Following coverages available:{cov_string}")
# print(covs)
#
# # Make API request using the generated token
for c in covs:
    try:
        response, messages = issuers(token,c,get_column_names())
        sync_issuers_with_database(response)
        insert_issuer_data(response)
    except:
        continue

print("Data been successfully fetched and stored to the internal database.")
# response = issuers(token)
# print(response)