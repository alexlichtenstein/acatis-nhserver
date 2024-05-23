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
import pandas as pd
import re

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

# Function to fetch all lists data from the database
def fetch_all_lists_data():
    conn = pyodbc.connect(cnxn_string)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM lists')
    rows = cursor.fetchall()
    conn.close()
    return rows

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
        blob_client.upload_blob(file.read())
        return True
    except Exception as e:
        print("Error uploading file to Azure Blob Storage:", e)
        return False

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

def add_list_record(name, description, type, date):
    try:
        cnxn = pyodbc.connect(cnxn_string)
        cursor = cnxn.cursor()

        # Insert record into lists table
        cursor.execute("INSERT INTO lists (name, description, type, date) VALUES (?, ?, ?, ?)",
                       (name, description, type, date))
        cnxn.commit()
        print("Record added successfully.")
    except Exception as e:
        print("Error:", e)
    finally:
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
                # Search for records matching the new pattern "XXYYYYYY"
                matches = re.findall(r'\b[A-Z]{2}[A-Z0-9]{6,8}\b', value_str)
                records.extend(matches)

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
        return text
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
        ##LISTS TABS
        dcc.Tab(label='Lists', children=[
            html.Div([
                html.Table(id='all-lists-table'),
                # html.Table(id='isins-table')
            ], style={'margin': '0 5%'})
        ]),
        dcc.Tab(label='Upload', children=[
            dcc.Upload(
                id='upload-data',
                children=html.Div([
                    'Drag and Drop or ',
                    html.A('Select Files')
                ]),
                style={
                    'width': '100%',
                    'height': '60px',
                    'lineHeight': '60px',
                    'borderWidth': '1px',
                    'borderStyle': 'dashed',
                    'borderRadius': '5px',
                    'textAlign': 'center',
                    'margin': '10px'
                },
                multiple=False
            ),
            html.Div(id='output-file-name'),
            dcc.Input(id='input-name', type='text', placeholder='Enter Name'),
            dcc.Textarea(id='input-description', placeholder='Enter Description'),
            dcc.Dropdown(
                id='input-type',
                options=[
                    {'label': 'Positiv', 'value': 'Positiv'},
                    {'label': 'Negativ', 'value': 'Negativ'}
                ],
                placeholder='Select Type'
            ),
            dcc.DatePickerSingle(
                id='input-date',
                placeholder='Select Date',
                clearable=True
            ),
            html.Button('Upload', id='upload-button'),
            html.Div(id='output-upload'),
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
              [Input('upload-button', 'n_clicks')],
              [State('upload-data', 'filename'),
               State('upload-data', 'contents')])
def upload_file(n_clicks, filename, contents):
    if n_clicks:
        if filename:
            try:
                if filename.endswith('.xlsx'):
                    df = parse_excel(contents)
                # Add conditions for other file types like PDF, Word, etc. if required
                if df is not None:
                    # Process DataFrame (example: insert into database)
                    pass
                elif records:
                    # Process found records (example: insert into database)
                    pass
                return html.Div('File uploaded successfully!')
            except Exception as e:
                return html.Div(f'Error uploading file: {e}')
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
    return dcc.send_data_frame(df.to_csv, filename=f"exported_data_{datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.csv")

@app.callback(
    Output('data-table', 'data'),
    Output('data-table', 'columns'),
    Input('date-dropdown', 'value'),
    Input('isins-dropdown', 'value'),
    Input('column-select-dropdown', 'value')
)
def update_data_table(selected_date, selected_isins, selected_columns):
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
