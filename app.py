import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pyodbc
import pandas as pd
from dash import dash_table
from dash.exceptions import PreventUpdate
import io
import sys
from contextlib import redirect_stdout
from dash import callback_context
import subprocess
import os
from dash.long_callback import DiskcacheLongCallbackManager
import diskcache

import dash
from dash import Dash, dcc, html, dash_table
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import json
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
import threading

import dash_bootstrap_components as dbc
import datetime

from azure.storage.blob import BlobServiceClient
import pdfplumber
import re
from datetime import datetime
import base64
from azure.storage.blob import BlobServiceClient
import pdfplumber

dropdown_options = [{'label': 'Aktiv', 'value': 'Aktiv'}, {'label': 'Inaktiv', 'value': 'Inaktiv'}]

# Connection string for SQL Server
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

# Azure Blob Storage connection string
blob_connection_string = "DefaultEndpointsProtocol=https;AccountName=aogencyupload;AccountKey=cBSpeBIFwgoAsCrMRvuu+6Vqn9Ev00rJuK/83RsacxkWifMBG05WGLu4Bt/a+bGrk2SmIrn7J9v6+AStl2M4pw==;EndpointSuffix=core.windows.net"
blob_container_name = "lists"

import pandas as pd
import re

# Function to fetch all lists data from the database
def fetch_all_lists_data():
    conn = pyodbc.connect(cnxn_string)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM lists')
    rows = cursor.fetchall()
    conn.close()
    return rows

def fetch_all_lists_names():
    conn = pyodbc.connect(cnxn_string)
    cursor = conn.cursor()
    cursor.execute('SELECT name FROM lists GROUP BY name')
    rows = cursor.fetchall()
    conn.close()
    return [{'label': row[0], 'value': row[0]} for row in rows]

# Function to fetch ISINs for a specific list from the database
def fetch_isins_for_list(list_id):
    conn = pyodbc.connect(cnxn_string)
    cursor = conn.cursor()
    cursor.execute('SELECT isin FROM lists_data WHERE list_id = ?', (list_id,))
    rows = cursor.fetchall()
    conn.close()
    return [row[0] for row in rows]
# Function to upload file to Azure Blob Storage
def upload_file_to_blob(file, filename):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        blob_client = blob_service_client.get_blob_client(container=blob_container_name, blob=filename)
        blob_client.upload_blob(file)
        return True
    except Exception as e:
        print("Error uploading file to Azure Blob Storage:", e)
        raise
        # return False

# Function to extract date and type from filename
def extract_info_from_filename(filename):
    date_formats = ['%d-%m-%y', '%Y%m%d', '%d.%m.%Y', '%d_%m_%Y', 'Q%q_%Y']
    for date_format in date_formats:
        try:
            date_str = re.search(r'\b\d{2}[-._]\d{2}[-._]\d{2,4}\b', filename).group()
            date = datetime.strptime(date_str, date_format)
            if 'Q' in filename:
                quarter = int(re.search(r'Q(\d)', filename).group(1))
                if quarter == 1:
                    date = datetime(date.year, 3, 31)
                elif quarter == 2:
                    date = datetime(date.year, 6, 30)
                elif quarter == 3:
                    date = datetime(date.year, 9, 30)
                elif quarter == 4:
                    date = datetime(date.year, 12, 31)
            type_from_filename = 'Positiv' if 'positiv' in filename.lower() else 'Negativ' if 'negativ' in filename.lower() else None
            return type_from_filename, date.strftime('%Y-%m-%d')
        except:
            continue
    return None, None

def add_list_record(name, comment, type, date, filename):
    try:
        cnxn = pyodbc.connect(cnxn_string)
        cursor = cnxn.cursor()

        # Insert record into lists table
        cursor.execute("INSERT INTO lists (name, comment, type, date, filename, status) OUTPUT INSERTED.id VALUES (?, ?, ?, ?, ?, 1)",
                       (name, comment, type, date, filename))
        id_of_new_record = cursor.fetchone()[0]
        cnxn.commit()
        print(f"Record added successfully with ID: {id_of_new_record}")
        return id_of_new_record
    except Exception as e:
        print("Error:", e)
    finally:
        if cnxn:
            cnxn.close()

def add_isins_to_list(isin_list, list_id):
    try:
        cnxn = pyodbc.connect(cnxn_string)
        cursor = cnxn.cursor()

        # Prepare the values to be inserted
        # This creates a list of tuples, each containing (isin, list_id)
        values_to_insert = [(isin, list_id) for isin in isin_list]

        # Build the INSERT statement dynamically based on the number of ISINs
        # The "?" placeholders match the number of values for each row
        insert_query = f"INSERT INTO lists_data (isin, list_id) VALUES " + \
                       ", ".join(["(?, ?)"] * len(isin_list))

        # Flatten the list of tuples for the execute() method
        flattened_values = [item for sublist in values_to_insert for item in sublist]

        # Execute the query with the flattened list of values
        cursor.execute(insert_query, flattened_values)
        cnxn.commit()
        print(f"All ISINs added successfully to list ID: {list_id}")
    except Exception as e:
        print("Error:", e)
    finally:
        if cnxn:
            cnxn.close()
def retrieve_all_lists():
    try:
        cnxn = pyodbc.connect(cnxn_string)
        cursor = cnxn.cursor()

        # Retrieve all records from lists table
        cursor.execute("SELECT * FROM lists")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print("Error:", e)
    finally:
        cnxn.close()

def retrieve_all_lists_data():
    try:
        cnxn = pyodbc.connect(cnxn_string)
        cursor = cnxn.cursor()

        # Retrieve all records from lists_data table
        cursor.execute("SELECT * FROM lists_data")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print("Error:", e)
    finally:
        cnxn.close()


def retrieve_filtered_data(filters):
    try:
        cnxn = pyodbc.connect(cnxn_string)
        cursor = cnxn.cursor()

        # Building the SQL query dynamically based on the filters provided
        query = "SELECT * FROM lists WHERE "
        conditions = []
        params = []

        for filter_type, filter_value in filters.items():
            if filter_type.lower() == 'name':
                conditions.append("name LIKE ?")
            elif filter_type.lower() == 'type':
                conditions.append("type LIKE ?")
            elif filter_type.lower() == 'date':
                conditions.append("date LIKE ?")
            params.append('%' + filter_value + '%')

        query += " AND ".join(conditions)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print("Error:", e)
    finally:
        cnxn.close()
def parse_excel(file):
    try:
        # Load the Excel file
        xls = pd.ExcelFile(file)
        records = []

        # Iterate through all sheets
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name)

            # Iterate through all values in the DataFrame
            for value in df.values.flatten():
                # Make sure to convert non-string cells to strings
                value_str = str(value)
                value_str = value_str.strip()
                # Search for records matching the new pattern "XXYYYYYY"
                # matches = re.findall(r'\b[A-Z]{2}[A-Z0-9]*\d[A-Z0-9]*\b', value_str)
                potential_matches = re.findall(r'\b[A-Z]{2}[A-Z0-9]{10}\b', value_str)
                for match in potential_matches:
                    # Count the digits in the match
                    digit_count = sum(char.isdigit() for char in match)
                    # Ensure the match contains at least 3 to 4 digits
                    if digit_count >= 2:
                        records.append(match)

        return records
    except Exception as e:
        print("Error parsing Excel file:", e)
        return None

# Function to parse PDF file
def parse_pdf(file):
    try:
        with pdfplumber.open(file) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text()
            records = []
            potential_matches = re.findall(r'\b[A-Z]{2}[A-Z0-9]{10}\b', text)
            for match in potential_matches:
                # Count the digits in the match
                digit_count = sum(char.isdigit() for char in match)
                # Ensure the match contains at least 3 to 4 digits
                if digit_count >= 2:
                    records.append(match)
        return records
    except Exception as e:
        print("Error parsing PDF file:", e)
        return None

# Function to search for records in parsed content
def search_records(content):
    records = []
    # Search for records matching the pattern "USXXXXXX"
    matches = re.findall(r'\bUS[A-Z0-9]{8,10}\b', content)
    records.extend(matches)
    return records


# Function to insert data into the database
def insert_data_to_database(df, records, filename):
    try:
        cnxn = pyodbc.connect(cnxn_string)
        cursor = cnxn.cursor()

        # Insert records from DataFrame to lists table
        if df is not None:
            for index, row in df.iterrows():
                cursor.execute("INSERT INTO lists (name, description, type, date, filename) VALUES (?, ?, ?, ?, ?)",
                               (row['name'], row['description'], row['type'], row['date'], filename))
                cnxn.commit()

        # Insert found records to lists_data table
        if records:
            for record in records:
                cursor.execute("INSERT INTO lists_data (isin, list_id) SELECT ?, id FROM lists WHERE filename = ?",
                               (record, filename))
                cnxn.commit()

        print("Data inserted into database successfully.")
    except Exception as e:
        print("Error inserting data into database:", e)
    finally:
        cnxn.close()

def get_unique_isins():
    try:
        with pyodbc.connect(cnxn_string) as conn:
            df = pd.read_sql_query("SELECT DISTINCT ISSUER_ISIN FROM company", conn)
        return [{'label': str(isin), 'value': str(isin)} for isin in df['ISSUER_ISIN'].tolist()]
    except Exception as e:
        print(e)
        return []

def get_factor_names():
    # Connect to the database
    cnxn = pyodbc.connect(cnxn_string)
    cursor = cnxn.cursor()

    # Query to fetch column names from the table
    query = "SELECT * FROM factors WHERE Status = 1"

    # Execute the query
    cursor.execute(query)

    # Fetch all column names and exclude specific columns
    excluded_columns = {}
    columns = [row.Mapping for row in cursor.fetchall() if row.Mapping not in excluded_columns]

    # Close the connection
    cursor.close()
    cnxn.close()

    return columns

def get_list_names():
    # Connect to the database
    cnxn = pyodbc.connect(cnxn_string)
    cursor = cnxn.cursor()

    # Query to fetch column names from the table
    query = """SELECT 
    l.name,
    l.Date,
    l.Status,
    l.Type
FROM 
    lists l
JOIN 
    (SELECT 
         name, 
         MAX(Date) AS MaxDate
     FROM 
         lists
     WHERE 
         Status = 1
     GROUP BY 
         name) AS latest
ON 
    l.name = latest.name
    AND l.Date = latest.MaxDate;
    """

    # Execute the query
    cursor.execute(query)

    # Fetch all column names and exclude specific columns
    excluded_columns = {}
    columns = [row.name for row in cursor.fetchall() if row.name not in excluded_columns]

    # Close the connection
    cursor.close()
    cnxn.close()

    return columns

def get_list_names_with_dates():
    # Connect to the database
    cnxn = pyodbc.connect(cnxn_string)
    cursor = cnxn.cursor()

    # Query to fetch column names from the table
    query = """SELECT 
    l.name,
    l.Date,
    l.Status,
    l.Type,
    latest.MaxDate
FROM 
    lists l
JOIN 
    (SELECT 
         name, 
         MAX(Date) AS MaxDate
     FROM 
         lists
     WHERE 
         Status = 1
     GROUP BY 
         name) AS latest
ON 
    l.name = latest.name
    AND l.Date = latest.MaxDate;
    """

    # Execute the query
    cursor.execute(query)

    # Fetch all column names and exclude specific columns
    excluded_columns = {}
    # columns = [{'name': row.name+'_'+str(row.MaxDate) for row in cursor.fetchall() if row.name not in excluded_columns]
    columns = [{'label': col.name + '_' + str(col.MaxDate), 'value': col.name}
                    for col in cursor.fetchall()]
    # Close the connection
    cursor.close()
    cnxn.close()

    return columns
def get_column_names():
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

def get_unique_dates():
    try:
        with pyodbc.connect(cnxn_string) as conn:
            # Fetch the unique dates
            df = pd.read_sql_query("SELECT DISTINCT DataDate FROM company_data ORDER BY DataDate DESC", conn)

        # Convert the 'DataDate' column to datetime type
        df['DataDate'] = pd.to_datetime(df['DataDate'])

        # Format the dates and prepare them for the dropdown options
        dates = df['DataDate'].dt.strftime('%Y-%m-%d').tolist()
        return [{'label': date, 'value': date} for date in dates]
    except Exception as e:
        print(e)
        return []

cache = diskcache.Cache("./cache")
long_callback_manager = DiskcacheLongCallbackManager(cache)

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP,'styles.css'], long_callback_manager=long_callback_manager)
output_file = "output.txt"
open(output_file, 'w').close()

# Set up HTTP Basic Authentication
auth = HTTPBasicAuth()

# User data
users = {
    "acatis": generate_password_hash("Acatis2024!")
}

# Initialize current_output as an empty string
current_output = ""

@auth.verify_password
def verify_password(username, password):
    if username in users and \
            check_password_hash(users.get(username), password):
        return username

# Protect the entire Dash app
@app.server.before_request
@auth.login_required
def restrict_access():
    pass

column_options = [{'label': col, 'value': col} for col in get_column_names()]
factor_options = [{'label': col, 'value': col} for col in get_factor_names()]
list_options = [{'label': col, 'value': col} for col in get_list_names()]
list_options_with_dates = get_list_names_with_dates()

app.title = "Nachhaltigkeitsserver"
# Define the layout of the app
app.layout = html.Div([
    html.Div([
        html.Img(src="/assets/logo.png", style={'height': '43px'}),
        html.H1("Nachhaltigkeitsserver", style={'margin': '0', 'padding-right': '10px', 'margin-left': 'auto'}),
    ], style={'display': 'flex'}),

    dcc.Tabs([
        # Tab 1: ISINS
        dcc.Tab(label='Unternehmensübersicht', children=[
            html.Div([
                html.Div([
                    html.H6("In der aktuellen Ansicht sehen Sie alle Unternehmen, die auf dem NH-Server verfügbar sind. Sie können das Eingabefeld für eine Suche verwenden.",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
                ]),
                dcc.Input(
                    id='add-company-input',
                    type='text',
                    placeholder='ISIN-Suche',
                    style={'width': '20%'}
                ),
                html.Br(),
                html.Div(id='add-company-output'),
                html.Div(id='dummy-div', style={'display': 'none'}),
                dcc.Loading(
                            id="loading-company-table",
                            type="default",
                            children=[
                dash_table.DataTable(
                    id='company-table',
                    columns=[
                        {'name': 'ISIN', 'id': 'ISSUER_ISIN'},
                        {'name': 'Issuer Name', 'id': 'ISSUER_NAME'},
                    ],
                    data=[],
                    style_table={'maxHeight': '80vh', 'overflowY': 'auto'},
                    style_cell_conditional=[
                            {'if': {'column_id': 'ISSUER_ISIN'}, 'width': '200px'},  # Set specific width for the ISIN column
                        ],
                ),
                                ]
                )
            ], style={'margin': '0 5%'}),
        ]),

        # Tab 2: Data Tab
        dcc.Tab(label='ESG Daten', children=[
            html.Div([
                html.Div([
                    html.H6("In der aktuellen Ansicht können Sie alle im NH Server verfügbaren Daten sehen. Sie können die Daten nach dem Datum, der Faktorliste und der Liste der ISINs filtern.",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
                    html.H6("Zum Starten wählen Sie bitte das Datum aus.",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
                ]),
                html.Div([
                    html.Label("Datum:", style={'margin-top': '10px', 'margin-bottom': '10px'}),  # Add a label for the filters
                ]),

                dcc.Dropdown(
                    id='date-dropdown',
                    options=get_unique_dates(),
                    placeholder="Wählen Sie ein Datum aus",
                    style={'width': '300px'}  # Adjust the style as needed
                ),
                html.Div([
                    html.Label("Faktorliste:", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                ]),
                dcc.Dropdown(
                        id='column-select-dropdown',
                        options=factor_options,
                        multi=True,
                        placeholder='Spalten filtern'
                    ),
                html.Div([
                    html.Label("Listen:", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                ]),
                dcc.Dropdown(
                        id='lists-select-dropdown',
                        options=list_options_with_dates,
                        multi=True,
                        placeholder='Listen filtern'
                    ),
                html.Div([
                    html.Label("ISINs Liste:", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                ]),
                dcc.Dropdown(
                    id='isins-dropdown',
                    options=get_unique_isins(),
                    multi=True,
                    placeholder='ISINs wählen',
                    style={'width': '100%', 'margin-bottom': '10px'}
                ),
                html.Button("Export", id="export-data-button", style={'margin-top': '10px'}),
                dcc.Download(id="download-dataframe-csv"),
                dcc.Loading(
                            id="loading-data-table",
                            type="default",  # You can choose from 'graph', 'cube', 'circle', 'dot', or 'default'
                            children=[
                    dash_table.DataTable(id='data-table',style_cell_conditional=[
                                {'if': {'column_id': 'ISSUER_ISIN'}, 'width': '200px'},  # Set specific width for the ISIN column
                            ],
                            fixed_columns={'headers': True, 'data': 1},
                            style_table={'overflowX': 'auto', 'width':'100%', 'minWidth': '100%'},
                            )
                        ]
                    )

            ], style={'margin': '0 5%'}),
        ]),

        dcc.Tab(label='Manueller Datenimport', children=[
            html.Div([
                html.H6("Die Daten werden regelmäßig einmal am Tag heruntergeladen. Wenn Sie den Datenabruf jetzt starten wollen, drücken Sie auf die Starttaste. Der Abruf wird sofort gestartet und ist in etwa 20 Minuten fertig.",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
                dcc.Interval(id='interval-component', interval=1 * 1000, n_intervals=0),
                html.Button("Abruf starten", id="run-script-button"),
                html.Pre(id="output"),
            ], style={'margin': '0 5%'}),
        ]),

        dcc.Tab(label='Faktoren bearbeiten', children=[
            html.Div([
                html.Div([
                    html.H6("In der aktuellen Ansicht sehen Sie alle Faktoren, die von der MSCI API abgerufen werden. Sie können hier zusätzliche Faktoren hinzufügen oder bestehende deaktivieren.",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
                ]),
                dcc.Input(
                        id='new-factor-name-input',
                        type='text',
                        placeholder='Faktor Name eingeben',style={'margin-top': '20px', 'margin-bottom': '20px'}
                    ),
                html.Button('Neuen Faktor hinzufügen', id='add-factor-button'),
                html.Div(id='add-factor-output', style={'margin-top': '20px', 'margin-bottom': '10px'}),
                html.Div([
                    html.Label("Faktor-Suche:", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                ]),
                dcc.Dropdown(
                        id='factor-select-dropdown',
                        options=factor_options,
                        multi=True,
                        placeholder='Faktore filtern'
                    ),
                html.Br(),
                html.Div(id='add-factor-output'),
                html.Div(id='dummy-div', style={'display': 'none'}),
                # html.Button('Show Company Data', id='show-company-data-button'),
                dcc.Loading(
                            id="loading-factor-table",
                            type="default",  # You can choose from 'graph', 'cube', 'circle', 'dot', or 'default'
                            children=[
                dash_table.DataTable(
                    id='factor-table',
                    css=[{"selector":".dropdown", "rule": "position: static",}],
                    columns=[
                        {'name': 'Name', 'id': 'Name'},
                        {'name': 'Mapping', 'id': 'Mapping'},
                        {'name': 'Status', 'id': 'Status', 'editable': True,
                         'presentation': 'dropdown'},
                        # Add more columns as needed
                    ],
                    data=[],
                    editable=True,
                    dropdown={
                        'Status': {
                            'options': [
                                {'label': 'Aktiv', 'value': 'Aktiv'},
                                {'label': 'Inaktiv', 'value': 'Inaktiv'}
                            ]
                        }
                    },
                    # style_table={'maxHeight': '80vh', 'overflowY': 'auto'},
                    style_cell_conditional=[
                            {'if': {'column_id': 'Status'}, 'width': '200px'},  # Set specific width for the ISIN column
                        ],
                    style_data_conditional=[
                        {
                            'if': {
                                'filter_query': '{Status} = "Aktiv"',
                                'column_id': 'Status'
                            },
                            'backgroundColor': 'lightgreen',
                            'color': 'black'
                        },
                        {
                            'if': {
                                'filter_query': '{Status} = "Inaktiv"',
                                'column_id': 'Status'
                            },
                            'backgroundColor': 'tomato',
                            # 'color': 'white'
                        }
                    ]
                ),
                                ]
                )
            ], style={'margin': '0 5%'}),
        ]),

        dcc.Tab(label='Listen Übersicht', children=[
            html.Div([
                html.Div([
                    html.H6("In der aktuellen Ansicht sehen Sie alle Listen.",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
                ]),
                html.Div([
                    html.Label("Typ:", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                ]),
                dcc.Dropdown(
                        id='list-typ-dropdown',
                        options=['Positiv','Negativ'],
                        multi=True,
                        placeholder='Typ filtern'
                    ),
                html.Div([
                    html.Label("Status:", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                ]),
                dcc.Dropdown(
                        id='list-status-dropdown',
                        options=['Aktiv','Inaktiv'],
                        multi=True,
                        placeholder='Status filtern'
                    ),
                html.Div([
                    html.Label("Listen-Suche:", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                ]),
                dcc.Dropdown(
                        id='list-select-dropdown',
                        options=list_options,
                        multi=True,
                        placeholder='Listen filtern'
                    ),
                html.Br(),
                html.Div(id='add-list-output'),
                html.Div(id='dummy-div', style={'display': 'none'}),
                # html.Button('Show Company Data', id='show-company-data-button'),

                dash_table.DataTable(
                    id='list-table',
                    css=[{"selector":".dropdown", "rule": "position: static",}],
                    columns=[
                        {'name': 'Name', 'id': 'Name'},
                        {'name': 'Kommentar', 'id': 'Comment'},
                        {'name': 'Leztes Update Datum', 'id': 'Date'},
                        {'name': 'Type', 'id': 'Type'},
                        {'name': 'Anzahl Uploads', 'id': 'Anzahl'},
                        {'name': 'Status', 'id': 'Status', 'editable': True,
                         'presentation': 'dropdown'},

                        # Add more columns as needed
                    ],
                    data=[],
                    editable=True,
                    dropdown={
                        'Status': {
                            'options': [
                                {'label': 'Aktiv', 'value': 'Aktiv'},
                                {'label': 'Inaktiv', 'value': 'Inaktiv'}
                            ]
                        },
                        'Type': {
                            'options': [
                                {'label': 'Positiv', 'value': 'Positiv'},
                                {'label': 'Negativ', 'value': 'Negativ'}
                            ]
                        }
                    },
                    # style_table={'maxHeight': '80vh', 'overflowY': 'auto'},
                    style_cell_conditional=[
                            {'if': {'column_id': 'Status'}, 'width': '200px'},  # Set specific width for the ISIN column
                        ],
                    style_data_conditional=[
                        {
                            'if': {
                                'filter_query': '{Status} = "Aktiv"',
                                'column_id': 'Status'
                            },
                            'backgroundColor': 'lightgreen',
                            'color': 'black'
                        },
                        {
                            'if': {
                                'filter_query': '{Status} = "Inaktiv"',
                                'column_id': 'Status'
                            },
                            'backgroundColor': 'tomato',
                            # 'color': 'white'
                        }
                    ]
                ),

            ], style={'margin': '0 5%'}),
        ]),

        ##LISTS TABS
        # dcc.Tab(label='Listen Übersicht', children=[
        #     html.Div([
        #         html.Table(id='all-lists-table'),
        #         # html.Table(id='isins-table')
        #     ], style={'margin': '0 5%'})
        # ]),
        # dcc.Tab(label='Listen Übersicht', children=[
        #     html.Div([
        #         html.Div([
        #             html.H6("In der aktuellen Ansicht sehen Sie alle Listen, die auf dem NH-Server verfügbar sind. Sie können das Eingabefeld für eine Suche verwenden.",
        #                     style={'margin-top': '20px', 'margin-bottom': '20px'}),
        #         ]),
        #         dcc.Input(
        #             id='add-list-input',
        #             type='text',
        #             placeholder='Listen-Suche',
        #             style={'width': '20%'}
        #         ),
        #         html.Br(),
        #         html.Div(id='add-list-output'),
        #         html.Div(id='dummy-div', style={'display': 'none'}),
        #         dcc.Loading(
        #                     id="loading-list-table",
        #                     type="default",
        #                     children=[
        #         dash_table.DataTable(
        #             id='list-table',
        #             # columns=[
        #             #     {'name': 'ISIN', 'id': 'ISSUER_ISIN'},
        #             #     {'name': 'Issuer Name', 'id': 'ISSUER_NAME'},
        #             # ],
        #             data=[],
        #             style_table={'maxHeight': '80vh', 'overflowY': 'auto'},
        #             # style_cell_conditional=[
        #             #         {'if': {'column_id': 'ISSUER_ISIN'}, 'width': '200px'},  # Set specific width for the ISIN column
        #             #     ],
        #         ),
        #                         ]
        #         )
        #     ], style={'margin': '0 5%'}),
        # ]),
        dcc.Tab(label='Neue Liste hochladen', children=[
            html.Div([
            html.Div([
                    html.H6("In der aktuellen Ansicht kannst du die neue Liste auf den Server hochladen. Das System akzeptiert Excel- und Pdf-Dokumente. Du kannst die Liste unter dem bestehenden Namen anordnen, oder einen neuen Namen eingeben. Wenn der neue Name angegeben wird, wird die Auswahl der Liste vernachlässigt.",
                            style={'margin-top': '20px', 'margin-bottom': '20px', }),
                ]),
            dcc.Upload(
                id='upload-data',
                children=html.Div([
                    'Drag and Drop oder ',
                    html.A('File wählen')
                ]),
                style={
                    'width': '100%',
                    'height': '60px',
                    'lineHeight': '60px',
                    'borderWidth': '1px',
                    'borderStyle': 'dashed',
                    'borderRadius': '5px',
                    'textAlign': 'center',
                    'margin': '10px',
                    'margin-left': '0px',
                    'cursor': 'pointer'
                },
                multiple=False
            ),
            html.Div(id='output-file-name',style={'margin-top': '20px', 'margin-bottom': '20px'}),
            html.Div(children=[
            dcc.Dropdown(id='input-name-dropdown', options=fetch_all_lists_names(), placeholder='Name wählen',
                         ),
            html.H6("Oder geben Sie einen neuen Namen ein:",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
            dcc.Input(id='input-name', type='text', placeholder='Name', ),
                ], style={'margin': '10px 10px 10px 0px','width':'300px'}),

            dcc.Textarea(id='input-description', placeholder='Kommentar', style={'margin': '0 10px 0 0px'}),
            dcc.Dropdown(
                id='input-type',
                options=[
                    {'label': 'Positiv', 'value': 'Positiv'},
                    {'label': 'Negativ', 'value': 'Negativ'}
                ],
                placeholder='Typ wählen',
                style={'margin': '10px 10px 10px 0px','width':'300px'}
            ),
            dcc.DatePickerSingle(
                id='input-date',
                placeholder='Datum wählen',
                clearable=True,
                style={'margin': '10px 10px 10px 0px', 'width':'150px'}
            ),
            html.Div(children=[
                html.Button('Hochladen', id='upload-button', style={'margin': '10px 10px 10px 0px'}),
            ]),
            html.Div(id='output-upload', style={'margin': '10px 10px 10px 10px'}),
            ],style={'margin': '0 5%'}),
        ]),
        ##LISTS TABS END

    ]),
])



output_file = "output.txt"

def run_script():
    with open(output_file, "w") as file:
        process = subprocess.Popen(['python', 'utils/api_connection.py'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        for line in iter(process.stdout.readline, ''):
            file.write(line)
            file.flush()


# Callback to handle file upload
@app.callback(Output('output-file-name', 'children'),
              [Input('upload-data', 'filename')])
def update_file_name(filename):
    if filename:
        return html.Div([html.B('Selected File: '), filename])
    else:
        return html.Div()
# Callback to update input fields based on filename
@app.callback([Output('input-type', 'value'),
               Output('input-date', 'date')],
              [Input('upload-data', 'filename')])
def update_fields(filename):
    type_from_filename = None
    date_from_filename = None
    if filename:
        try:
            date_str = re.search(r'(\d{2}[-._]\d{2}[-._]20\d{2})|(\d{6})|Q(\d)_20\d{2}', filename).group()
            if 'Q' in date_str:
                year = int(re.search(r'20\d{2}', filename).group())
                quarter = int(date_str[1])
                if quarter == 1:
                    date = datetime(year, 3, 31)
                elif quarter == 2:
                    date = datetime(year, 6, 30)
                elif quarter == 3:
                    date = datetime(year, 9, 30)
                elif quarter == 4:
                    date = datetime(year, 12, 31)
            elif len(date_str) == 6:
                date = datetime.strptime(date_str, '%y%m%d')
            else:
                date_formats = ['%d-%m-%y', '%Y%m%d', '%d.%m.%Y', '%d_%m_%Y']
                for date_format in date_formats:
                    try:
                        date = datetime.strptime(date_str, date_format)
                        break
                    except:
                        continue
            date_from_filename = date.strftime('%Y-%m-%d')
        except:
            pass

        type_from_filename = 'Positiv' if 'positiv' in filename.lower() else 'Negativ' if 'negativ' in filename.lower() else None

    return type_from_filename, date_from_filename

# Callback to handle file upload
@app.callback(Output('output-upload', 'children'),
              [Input('upload-button', 'n_clicks'),
               Input('input-name-dropdown', 'value'),
               Input('input-name', 'value'),
               Input('input-description', 'value'),
               Input('input-type', 'value'),
               Input('input-date', 'date')],
              [State('upload-data', 'filename'),
               State('upload-data', 'contents')])
def upload_file(n_clicks, name_dropdown, name, description, type, date, filename, contents):
    if n_clicks:
        if name:
            name = name
        elif name_dropdown:
            name = name_dropdown
        else:
            return html.Div(f'Bitte Name eingeben')
        if date:
            date = str(date)
        else:
            return html.Div(f'Bitte Datum eingeben')
        if description:
            description = description
        else:
            description = ""
        if type:
            type = type
        else:
            return html.Div(f'Bitte Typ wählen')
        if filename:
            try:
                upload_file_to_blob(contents, filename)
            except Exception as e:
                if "BlobAlreadyExists" in str(e):
                    return html.Div(f'Error uploading file, this file already exists')
                else:
                    return html.Div(f'Error uploading file: {e}')
            try:
                isins = []
                # Decode the base64 file content
                content_type, content_string = contents.split(',')
                decoded = base64.b64decode(content_string)
                with open(filename, 'wb') as f:
                    f.write(decoded)

                if filename.endswith('.xlsx'):
                    df = parse_excel(filename)
                    if df is not None:
                        isins = df
                if filename.endswith('.pdf'):
                    df = parse_pdf(filename)
                    if df is not None:
                        isins = df

                print("ISINS")
                print(isins)
                new_record_id = add_list_record(name, description, type, date,
                                                filename)
                print(f"New record ID: {new_record_id}")

                add_isins_to_list(isins, new_record_id)


                return html.Div('File uploaded successfully!')
            except Exception as e:
                return html.Div(f'Error processing file: {e}')
        else:
            return html.Div('No file selected.')
    return html.Div()

@app.callback(
    Output('output', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_output(n):
    if os.path.exists(output_file):
        with open(output_file, "r") as file:
            return file.read()
    return "No output yet."

@app.callback(
    Output('interval-component', 'disabled'),
    Input('run-script-button', 'n_clicks'),
    prevent_initial_call=True
)
def start_script(n_clicks):
    thread = threading.Thread(target=run_script, daemon=True)
    thread.start()
    return False

@app.callback(
    Output('company-table', 'data'),
    [Input('add-company-input', 'value')]
)
def display_filtered_company_data(isin_input):
    if not isin_input:
        # If no ISIN is provided, display default or all data
        query = "SELECT TOP 1000 * FROM company"
    else:
        # Filter by ISIN when input is provided
        query = f"SELECT * FROM company WHERE ISSUER_ISIN LIKE '%{isin_input}%' OR ISSUER_NAME LIKE '%{isin_input}%'"
    try:
        with pyodbc.connect(cnxn_string) as conn:
            df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    except Exception as e:
        print(e)  # It's a good practice to log or print errors
        return []

# @app.callback(
#     Output('list-table', 'data'),
#     [Input('add-list-input', 'value')]
# )
# def display_filtered_list_data(list_input):
#     if not list_input:
#         # If no ISIN is provided, display default or all data
#         query = "SELECT TOP 1000 * FROM lists"
#     else:
#         # Filter by ISIN when input is provided
#         query = f"SELECT * FROM lists WHERE name LIKE '%{list_input}%' OR filename LIKE '%{list_input}%'"
#     try:
#         with pyodbc.connect(cnxn_string) as conn:
#             df = pd.read_sql_query(query, conn)
#         return df.to_dict('records')
#     except Exception as e:
#         print(e)  # It's a good practice to log or print errors
#         return []

@app.callback(
    [Output('factor-table', 'data'),
     Output('add-factor-output', 'children')],
    [
     Input('factor-table', 'data_previous'), Input('factor-select-dropdown', 'value'),Input('add-factor-button', 'n_clicks') ],
    [State('new-factor-name-input', 'value'), State('factor-table', 'data')]
)
def update_table( previous_data, factor_input_dropdown, n_clicks, new_factor_name, data_current):
    ctx = dash.callback_context
    triggered_component = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else None
    if triggered_component == 'dummy-div.children' or triggered_component == 'add-factor-button.n_clicks':
        try:
            with pyodbc.connect(cnxn_string) as conn:
                query = "SELECT * FROM factors ORDER BY ID DESC"

        except Exception as e:
            print(str(e))
    if triggered_component == 'add-factor-button':
        if not new_factor_name:
            error_message = "Bitte geben Sie einen Faktornamen ein."
        else:
            try:
                with pyodbc.connect(cnxn_string) as conn:
                    cursor = conn.cursor()
                    # Add new factor to company_data table
                    alter_query = f"ALTER TABLE company_data ADD [{new_factor_name}] nvarchar(50)"
                    cursor.execute(alter_query)
                    # Insert new factor into factors table
                    insert_query = f"INSERT INTO factors (Name, Status, Mapping) VALUES ('{new_factor_name}', 1, '{new_factor_name}')"
                    cursor.execute(insert_query)
                    conn.commit()
            except Exception as e:
                error_message = f"An error occurred: {str(e)}"

    if not factor_input_dropdown:
        factor_input_dropdown = get_factor_names()  # Default to all columns if none are selected

    if triggered_component == 'factor-table':
        if previous_data is not None:
            # Find changes between current_data and previous_data
            for i, (current, previous) in enumerate(zip(data_current, previous_data)):
                if current != previous:
                    factor_id = current['Id']
                    # Generate SQL query to update changed values
                    update_queries = []
                    column_name_change_query = None
                    old_name = None

                    for key, value in current.items():
                        if current[key] != previous[key]:
                            if key == 'Status':
                                if value == 'Aktiv':
                                    update_query = f"UPDATE factors SET {key} = 1 WHERE Id = {factor_id}"
                                    update_queries.append(update_query)
                                else:
                                    update_query = f"UPDATE factors SET {key} = 0 WHERE Id = {factor_id}"
                                    update_queries.append(update_query)
                            elif key == 'Name':
                                old_name = previous[key]
                                new_name = value
                                update_query = f"UPDATE factors SET {key} = '{value}' WHERE Id = {factor_id}"
                                update_queries.append(update_query)
                                column_name_change_query = f"EXEC sp_rename 'company_data.[{old_name}]', '{new_name}', 'COLUMN';"
                                update_queries.append(column_name_change_query)
                            else:
                                update_query = f"UPDATE factors SET {key} = '{value}' WHERE Id = {factor_id}"
                                update_queries.append(update_query)
                    # Execute SQL update queries
                    try:
                        with pyodbc.connect(cnxn_string) as conn:
                            cursor = conn.cursor()
                            for query in update_queries:
                                cursor.execute(query)
                            conn.commit()
                    except Exception as e:
                        error_message = f"An error occurred while updating data: {str(e)}"

        return data_current, ""
    else:
        conditions = " OR ".join([f"Mapping LIKE '{factor}'" for factor in factor_input_dropdown])
        query = f"SELECT * FROM factors WHERE {conditions} ORDER BY ID DESC "
        with pyodbc.connect(cnxn_string) as conn:
            df = pd.read_sql_query(query, conn)
            df['Status'] = df['Status'].apply(lambda x: 'Aktiv' if x == 1 else 'Inaktiv')
            dropdown_options = [{'label': 'Aktiv', 'value': 'Aktiv'}, {'label': 'Inaktiv', 'value': 'Inaktiv'}]
            return df.to_dict('records'), ""

##### LIST UBERSICHT
@app.callback(
    [Output('list-table', 'data'), Output('list-table', 'columns')],
    [
     Input('list-table', 'data_previous'), Input('list-select-dropdown', 'value'), Input('list-status-dropdown', 'value'), Input('list-typ-dropdown', 'value') ],
    [State('list-table', 'data')]
)
def update_table( previous_data, list_input_dropdown, list_status_dropdown, list_typ_dropdown, data_current):
    ctx = dash.callback_context
    triggered_component = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else None

    if not list_status_dropdown:
        list_status_dropdown = ['Aktiv', 'Inaktiv']

    if not list_typ_dropdown:
        list_typ_dropdown = ['Positiv','Negativ']

    columns = [
        {'name': 'Name', 'id': 'Name'},
        {'name': 'Leztes Update Datum', 'id': 'Date'},
        {'name': 'Type', 'id': 'Type', 'editable': True, 'presentation': 'dropdown'},
        {'name': 'Anzahl Uploads', 'id': 'Anzahl'},
        {'name': 'Status', 'id': 'Status', 'editable': True,
         'presentation': 'dropdown'}
    ]

    if triggered_component == 'list-table':
        if previous_data is not None:
            print('WEAREHERE')
            # Find changes between current_data and previous_data
            for i, (current, previous) in enumerate(zip(data_current, previous_data)):
                if current != previous:
                    print(current)
                    id = current['id']
                    list_id = current['Name']
                    date = current['Date']
                    # Generate SQL query to update changed values
                    update_queries = []
                    column_name_change_query = None
                    old_name = None

                    for key, value in current.items():
                        if current[key] != previous[key]:
                            if key == 'Status':
                                if value == 'Aktiv':
                                    print('update1')
                                    update_query = f"UPDATE lists SET status = 1 WHERE name = '{list_id}'"
                                    update_queries.append(update_query)
                                else:
                                    print('update2')
                                    update_query = f"UPDATE lists SET status = 0 WHERE name = '{list_id}'"
                                    update_queries.append(update_query)
                                    print(update_query)
                            if key == 'Comment':
                                update_query = f"UPDATE lists SET comment = '{value}' WHERE id = '{id}'"
                                update_queries.append(update_query)
                            if key == 'Type':
                                update_query = f"UPDATE lists SET type = '{value}' WHERE name = '{list_id}'"
                                update_queries.append(update_query)

                    # Execute SQL update queries
                    try:
                        with pyodbc.connect(cnxn_string) as conn:
                            cursor = conn.cursor()
                            for query in update_queries:
                                cursor.execute(query)
                            conn.commit()
                    except Exception as e:
                        error_message = f"An error occurred while updating data: {str(e)}"

        if not list_input_dropdown:
            list_input_dropdown = get_list_names()  # Default to all columns if none are selected

            conditions = " OR ".join([f"name = '{list}'" for list in list_input_dropdown])
            query = """
                SELECT
                    MAX(l.id) as id,
                    l.Name,
                    MAX(l.Date) AS Date,
                    COUNT(*) AS Anzahl,
                    l.Status, l.Type
                FROM
                    lists l
                WHERE {}
                GROUP BY
                    l.name, l.status, l.type 

            """.format(conditions)
            with pyodbc.connect(cnxn_string) as conn:
                df = pd.read_sql_query(query, conn)
                df['Status'] = df['Status'].apply(lambda x: 'Aktiv' if x == 1 else 'Inaktiv')
                dropdown_options = [{'label': 'Aktiv', 'value': 'Aktiv'}, {'label': 'Inaktiv', 'value': 'Inaktiv'}]
                filtered_df = df[df['Status'].isin(list_status_dropdown) & df['Type'].isin(list_typ_dropdown)]
                print(df.info())
                return [filtered_df.to_dict('records'), columns]
        else:
            columns = [
                {'name': 'Name', 'id': 'Name', 'editable': True},
                {'name': 'Kommentar', 'id': 'Comment', 'editable': True},
                {'name': 'Anzahl ISINs', 'id': 'AnzahlIsin'},
                {'name': 'Leztes Update Datum', 'id': 'Date'},
                {'name': 'Type', 'id': 'Type', 'editable': True, 'presentation': 'dropdown'},
                {'name': 'Status', 'id': 'Status', 'editable': True,
                 'presentation': 'dropdown'}
            ]
            conditions = " OR ".join([f"l.Name = '{list}'" for list in list_input_dropdown])
            query = """
                            SELECT
                            l.id,
                            MAX(l.Name) AS Name,
                            MAX(l.Comment) AS Comment,
                            MAX(l.Date) AS Date,
                            COUNT(DISTINCT ld.isin) AS AnzahlIsin, -- Counts distinct ISINs per list
                            MAX(CONVERT(INT, l.Status)) AS Status, -- Convert bit to int for aggregation
                            MAX(l.Type) AS Type
                        FROM
                            lists l
                        LEFT JOIN
                            lists_data ld ON l.id = ld.list_id
                        WHERE {}
                        GROUP BY
                            l.id
                        ORDER BY
                            MAX(l.Name) DESC, -- Then sorting by Name in descending order within each Date
                            MAX(l.Date) DESC; -- Sorting by Date in ascending order

                        """.format(conditions)
            with pyodbc.connect(cnxn_string) as conn:
                df = pd.read_sql_query(query, conn)
                df['Status'] = df['Status'].apply(lambda x: 'Aktiv' if x == 1 else 'Inaktiv')
                filtered_df = df[df['Status'].isin(list_status_dropdown) & df['Type'].isin(list_typ_dropdown)]
                dropdown_options = [{'label': 'Aktiv', 'value': 'Aktiv'}, {'label': 'Inaktiv', 'value': 'Inaktiv'}]
                print(df.info())
                return [filtered_df.to_dict('records'), columns]

        # return [data_current, columns]
    else:
        if not list_input_dropdown:
            list_input_dropdown = get_list_names()  # Default to all columns if none are selected

            conditions = " OR ".join([f"name = '{list}'" for list in list_input_dropdown])
            query = """
                SELECT
                    MAX(l.id) as id,
                    l.Name,
                    MAX(l.Date) AS Date,
                    COUNT(*) AS Anzahl,
                    l.Status, l.Type
                FROM
                    lists l
                WHERE {}
                GROUP BY
                    l.name, l.status, l.type 
    
            """.format(conditions)
            with pyodbc.connect(cnxn_string) as conn:
                df = pd.read_sql_query(query, conn)
                df['Status'] = df['Status'].apply(lambda x: 'Aktiv' if x == 1 else 'Inaktiv')
                dropdown_options = [{'label': 'Aktiv', 'value': 'Aktiv'}, {'label': 'Inaktiv', 'value': 'Inaktiv'}]
                filtered_df = df[df['Status'].isin(list_status_dropdown) & df['Type'].isin(list_typ_dropdown)]
                print(df.info())
                return [filtered_df.to_dict('records'), columns]
        else:
            columns = [
                {'name': 'Name', 'id': 'Name', 'editable': True},
                {'name': 'Kommentar', 'id': 'Comment', 'editable': True},
                {'name': 'Leztes Update Datum', 'id': 'Date'},
                {'name': 'Anzahl Isins', 'id': 'AnzahlIsin'},
                {'name': 'Type', 'id': 'Type', 'editable': True, 'presentation': 'dropdown'},
                {'name': 'Status', 'id': 'Status', 'editable': True,
                 'presentation': 'dropdown'}
            ]
            conditions = " OR ".join([f"l.Name = '{list}'" for list in list_input_dropdown])
            query = """
                            SELECT
                            l.id,
                            MAX(l.Name) AS Name,
                            MAX(l.Comment) AS Comment,
                            MAX(l.Date) AS Date,
                            COUNT(DISTINCT ld.isin) AS AnzahlIsin, -- Counts distinct ISINs per list
                            MAX(CONVERT(INT, l.Status)) AS Status, -- Convert bit to int for aggregation
                            MAX(l.Type) AS Type
                        FROM
                            lists l
                        LEFT JOIN
                            lists_data ld ON l.id = ld.list_id
                        WHERE {}
                        GROUP BY
                            l.id
                        ORDER BY
                            MAX(l.Name) DESC, -- Then sorting by Name in descending order within each Date
                            MAX(l.Date) DESC; -- Sorting by Date in ascending order

                        """.format(conditions)
            with pyodbc.connect(cnxn_string) as conn:
                df = pd.read_sql_query(query, conn)
                df['Status'] = df['Status'].apply(lambda x: 'Aktiv' if x == 1 else 'Inaktiv')
                filtered_df = df[df['Status'].isin(list_status_dropdown) & df['Type'].isin(list_typ_dropdown)]
                dropdown_options = [{'label': 'Aktiv', 'value': 'Aktiv'}, {'label': 'Inaktiv', 'value': 'Inaktiv'}]
                print(df.info())
                return [filtered_df.to_dict('records'), columns]



##### LISTEN UBERSICHT END

@app.callback(
    Output("download-dataframe-csv", "data"),
    Input("export-data-button", "n_clicks"),
    State('data-table', 'data'),
    prevent_initial_call=True
)
def generate_csv(n_clicks, data):
    if n_clicks is None:
        raise PreventUpdate
    df = pd.DataFrame(data)
    return dcc.send_data_frame(df.to_csv, filename=f"exported_data_{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.csv")

@app.callback(
    Output('data-table', 'data'),
    Output('data-table', 'columns'),
    Input('date-dropdown', 'value'),
    Input('isins-dropdown', 'value'),
    Input('column-select-dropdown', 'value'),
    Input('lists-select-dropdown', 'value')
)
def update_data_table(selected_date, selected_isins, selected_columns, selected_lists):
    try:
        if not selected_date:
            # Handle the case where no date is selected
            return [], []

        # Replace with your SQL query to fetch data from the SQL Server database
        # Establish a connection to the SQL Server database
        conn = pyodbc.connect(cnxn_string)

        # Fetch the mapping from the factors table where Status is 1 (enabled)
        mapping_query = "SELECT Name, Mapping FROM factors WHERE Status = 1"
        mapping_df = pd.read_sql_query(mapping_query, conn)
        column_mapping = dict(zip(mapping_df['Name'], mapping_df['Mapping']))
        reverse_mapping = {v: k for k, v in column_mapping.items()}
        # Update the columns of the data table based on selected_columns
        if not selected_columns:
            selected_columns = mapping_df['Mapping'].tolist()
        else:
            # Ensure only enabled and selected columns are included
            selected_columns = [col for col in selected_columns if col in mapping_df['Mapping'].tolist()]

        lists_query = "SELECT name FROM lists WHERE status = 1 GROUP BY name"
        list_df = pd.read_sql_query(lists_query, conn)
        if not selected_lists:
            selected_lists = list_df['name'].tolist()
        else:
            print(selected_lists)
            # selected_lists = [col for col in selected_lists if col in list_df['name']]
            # print(selected_lists)
        # Build the SQL query based on selected_date and isins
        query = f"SELECT TOP 1000 * FROM company RIGHT JOIN company_data ON company.CompanyID = company_data.CompanyID WHERE DataDate = '{selected_date}'"
        if selected_isins:
            selected_isins_str = ','.join(f"'{isin}'" for isin in selected_isins)
            query += f" AND ISSUER_ISIN IN ({selected_isins_str})"

        # Fetch and filter data from the database
        df = pd.read_sql_query(query, conn)
        df.rename(columns=column_mapping, inplace=True)

        # Close the database connection
        conn.close()
        issuer_isin_column = {'name': 'ISSUER_ISIN', 'id': 'ISSUER_ISIN'}
        issuer_name_column = {'name': 'ISSUER_NAME', 'id': 'ISSUER_NAME'}
        columns = [{'name': col, 'id': col} for col in df.columns if col in selected_columns]
        for col in selected_lists:
            print(col)
            columns.append({'name': col, 'id': col})
        # columns.append([{'name': col, 'id': col} for col in selected_lists])
        columns.insert(0, issuer_name_column)
        columns.insert(0, issuer_isin_column)

        return df.to_dict('records'), columns
    except Exception as e:
        return str(e)

server = app.server

if __name__ == '__main__':
    if os.path.exists("selected_date.txt"):
        os.remove("selected_date.txt")
    # app.run_server(debug=True, host='0.0.0.0', port=8050) #To run on server
    app.run_server(debug=True) #To run locally
