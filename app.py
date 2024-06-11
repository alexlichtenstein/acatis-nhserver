import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
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
from flask_caching import Cache
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
import threading

import dash_bootstrap_components as dbc
import datetime

from azure.storage.blob import BlobServiceClient
import pdfplumber
import re
import base64
import openpyxl
import dash_ag_grid as dag

# Set up Diskcache for long callbacks
cache = diskcache.Cache("./cache")
long_callback_manager = DiskcacheLongCallbackManager(cache)

# Initialize Dash app with Bootstrap theme and long callback manager
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, 'styles.css'], long_callback_manager=long_callback_manager)
app.title = "Nachhaltigkeitsserver"

cache = Cache(app.server, config={
    'CACHE_TYPE': 'filesystem',  # You can choose 'SimpleCache', 'FileSystemCache', etc.
    'CACHE_DIR': 'cache-directory',  # Directory where cached data will be stored
    'CACHE_DEFAULT_TIMEOUT': 60  # Default timeout in seconds for cached data
})
cache.clear()

output_file = "output.txt"
output_file_history = "output_history.txt"
open(output_file, 'w').close()

# Initialize HTTP Basic Authentication
auth = HTTPBasicAuth()
users = {
    "acatis": generate_password_hash("Acatis2024!")
}

# Initialize current_output as an empty string
current_output = ""

german_locale_text = {
    # General
    "loadingOoo": "Lädt...",
    "noRowsToShow": "Keine Zeilen zum Anzeigen",
    # Filter
    "filterOoo": "Filter...",
    "applyFilter": "Filter anwenden",
    "equals": "Gleich",
    "notEqual": "Nicht gleich",
    "lessThan": "Weniger als",
    "greaterThan": "Größer als",
    "lessThanOrEqual": "Kleiner oder gleich",
    "greaterThanOrEqual": "Größer oder gleich",
    "inRange": "Im Bereich",
    "contains": "Enthält",
    "notContains": "Nicht enthält",
    "startsWith": "Beginnt mit",
    "endsWith": "Endet mit",
    "blank": "Leer",
    "notBlank": "Nicht leer",
    # Column Menu
    "pinColumn": "Spalte anheften",
    "valueAggregation": "Wertaggregation",
    "autosizeThiscolumn": "Diese Spalte automatisch anpassen",
    "autosizeAllColumns": "Alle Spalten automatisch anpassen",
    "groupBy": "Gruppieren nach",
    "ungroupBy": "Gruppierung aufheben nach",
    # Side Bar
    "columns": "Spalten",
    "filters": "Filter",
    # Other
    "export": "Exportieren",
    "csvExport": "CSV Export",
    "excelExport": "Excel Export",
    "andCondition": "und",
    "orCondition": "oder"
}


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

# Connection strings
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

blob_connection_string = "DefaultEndpointsProtocol=https;AccountName=acatis;AccountKey=CXqkeM7gU/X9HSF4cDftHfYPcqLAhpE3OeA3BLt9zVgazx6jhPAh16T9oitRn+dOE2SByAyaE46P+AStzTy+ug==;EndpointSuffix=core.windows.net"
blob_container_name = "listsupload"

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

# Function to extract date and type from filename
def extract_info_from_filename(filename):
    date_formats = ['%d-%m-%y', '%Y%m%d', '%d.%m.%Y', '%d_%m_%Y']
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
            return type_from_filename, date.strftime('%d-%m-%Y')
        except:
            continue
    return None, None

# Function to add a list record to the database
def add_list_record(name, comment, type, date, filename):
    try:
        cnxn = pyodbc.connect(cnxn_string)
        cursor = cnxn.cursor()
        cursor.execute("INSERT INTO lists (name, comment, type, date, filename, status) OUTPUT INSERTED.id VALUES (?, ?, ?, ?, ?, 1)",
                       (name, comment, type, date, filename))
        id_of_new_record = cursor.fetchone()[0]
        cnxn.commit()
        return id_of_new_record
    except Exception as e:
        print("Error:", e)
    finally:
        if cnxn:
            cnxn.close()

# Function to add ISINs to a list
def add_isins_to_list(isin_list, list_id):
    try:
        cnxn = pyodbc.connect(cnxn_string)
        cursor = cnxn.cursor()
        chunk_size = 1000  # Max number of rows per INSERT statement
        for i in range(0, len(isin_list), chunk_size):
            chunk = isin_list[i:i + chunk_size]
            values_to_insert = [(isin, list_id) for isin in chunk]
            insert_query = f"INSERT INTO lists_data (isin, list_id) VALUES " + ", ".join(["(?, ?)"] * len(chunk))
            flattened_values = [item for sublist in values_to_insert for item in sublist]
            cursor.execute(insert_query, flattened_values)
        cnxn.commit()
    except Exception as e:
        print("Error:", e)
    finally:
        if cnxn:
            cnxn.close()

# Fetch column names from the database
def get_column_names():
    cnxn = pyodbc.connect(cnxn_string)
    cursor = cnxn.cursor()
    query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'company_data'"
    cursor.execute(query)
    excluded_columns = {'DataID', 'CompanyID', 'DataDate'}
    columns = [row.COLUMN_NAME for row in cursor.fetchall() if row.COLUMN_NAME not in excluded_columns]
    cursor.close()
    cnxn.close()
    return columns

# Fetch unique dates from the database
def get_unique_dates():
    try:
        with pyodbc.connect(cnxn_string) as conn:
            df = pd.read_sql_query("SELECT DISTINCT DataDate FROM company_data ORDER BY DataDate DESC", conn)
        df['DataDate'] = pd.to_datetime(df['DataDate'])
        labels = df['DataDate'].dt.strftime('%d.%m.%Y').tolist()
        values = df['DataDate'].dt.strftime('%Y-%m-%d').tolist()
        dropdown_options = [{'label': label, 'value': value} for label, value in zip(labels, values)]
        return dropdown_options
    except Exception as e:
        print(e)
        return []

# Fetch unique ISINs for a given date
def get_unique_isins(selected_date):
    try:
        with pyodbc.connect(cnxn_string) as conn:
            query = f"""
                SELECT DISTINCT company.ISSUER_ISIN
                FROM company
                JOIN company_data ON company.CompanyID = company_data.CompanyID
                WHERE company_data.DataDate = '{selected_date}'
            """
            df = pd.read_sql_query(query, conn)
        return [{'label': str(isin), 'value': str(isin)} for isin in df['ISSUER_ISIN'].tolist()]
    except Exception as e:
        print(e)
        return []

# Get factor names from the database
def get_factor_names():
    cnxn = pyodbc.connect(cnxn_string)
    cursor = cnxn.cursor()
    query = "SELECT * FROM factors"
    cursor.execute(query)
    excluded_columns = {}
    columns = [row.Mapping for row in cursor.fetchall() if row.Mapping not in excluded_columns]
    cursor.close()
    cnxn.close()
    return columns

# Get list names from the database
def get_list_names():
    cnxn = pyodbc.connect(cnxn_string)
    cursor = cnxn.cursor()
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
     GROUP BY 
         name) AS latest
ON 
    l.name = latest.name
    AND l.Date = latest.MaxDate;
    """
    cursor.execute(query)
    excluded_columns = {}
    columns = [row.name for row in cursor.fetchall() if row.name not in excluded_columns]
    cursor.close()
    cnxn.close()
    return columns

# Get list names with dates
def get_list_names_with_dates():
    cnxn = pyodbc.connect(cnxn_string)
    cursor = cnxn.cursor()
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
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [
        {
            'label': f"{row.name} - {row.MaxDate.strftime('%d.%m.%Y')}",
            'value': row.name
        }
        for row in rows
    ]
    cursor.close()
    cnxn.close()
    return columns

# Get list names with dates till a specified date
def get_list_names_with_dates_till_date(max_date):
    cnxn = pyodbc.connect(cnxn_string)
    cursor = cnxn.cursor()
    if isinstance(max_date, datetime):
        max_date = max_date.strftime('%d-%m-%Y')
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
             AND Date <= ?
         GROUP BY 
             name) AS latest
    ON 
        l.name = latest.name
        AND l.Date = latest.MaxDate
    WHERE 
        l.Status = 1;
    """
    cursor.execute(query, max_date)
    columns = [{'label': f"{col.name} - {col.MaxDate.strftime('%d.%m.%Y')}", 'value': col.name} for col in cursor.fetchall()]
    cursor.close()
    cnxn.close()
    return columns

def parse_excel(file, sheet):
    try:
        # Load the Excel file
        xls = pd.ExcelFile(file)
        records = []
        if sheet == 'All':
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
        else:
            df = pd.read_excel(xls, sheet_name=sheet)

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

column_options = [{'label': col, 'value': col} for col in get_column_names()]
factor_options = [{'label': col, 'value': col} for col in get_factor_names()]
list_options = [{'label': col, 'value': col} for col in get_list_names()]
list_options_with_dates = get_list_names_with_dates()

app.layout = html.Div([
    html.Div([
        html.Img(src="/assets/logo.png", style={'height': '43px'}),
        html.H1("Nachhaltigkeitsserver", style={'margin': '0', 'padding-right': '10px', 'margin-left': 'auto'}),
    ], style={'display': 'flex'}),
    dcc.Store(id='isins-store'),
    dcc.Tabs(id='tabs', children=[
        dcc.Tab(value='tab-1', label='Unternehmensübersicht', children=[
            html.Div([
                html.Div([
                    html.H6("In der aktuellen Ansicht sehen Sie alle Unternehmen, die auf dem NH-Server verfügbar sind. Sie können das Eingabefeld für eine Suche verwenden.",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
                ]),
                dcc.Input(id='add-company-input', type='text', placeholder='ISIN-Suche', style={'width': '20%', 'margin-top': '10px', 'margin-bottom': '10px'}),
                html.Br(),
                html.Div(id='add-company-output'),
                html.Div(id='dummy-div', style={'display': 'none'}),
                dcc.Loading(
                    id="loading-company-table",
                    type="default",
                    children=[
                        dag.AgGrid(
                            id='company-table',
                            columnDefs=[
                                {'headerName': 'ISIN', 'field': 'ISSUER_ISIN', 'filter': 'agTextColumnFilter'},
                                {'headerName': 'Issuer Name', 'field': 'ISSUER_NAME', 'filter': 'agTextColumnFilter'},
                            ],
                            defaultColDef={'flex': 1, 'filter': True, 'sortable': True, 'editable': False, 'floatingFilter': True},
                            dashGridOptions={"localeText": german_locale_text},
                            style={'height': '500px', 'width': '100%'},
                        )
                    ]
                )
            ], style={'margin': '0 5%'}),
        ]),
        dcc.Tab(value='tab-2', label='ESG Daten', children=[
            html.Div([
                html.Div([
                    html.H6("In der aktuellen Ansicht können Sie alle im NH Server verfügbaren Daten sehen. Sie können die Daten nach dem Datum, der Faktorliste und der Liste der ISINs filtern.",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
                    html.H6("Zum Starten wählen Sie bitte das Datum aus.", style={'margin-top': '20px', 'margin-bottom': '20px'}),
                ]),
                html.Div([
                    html.Label("Datum:", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                ]),
                dcc.Dropdown(id='date-dropdown', options=get_unique_dates(), placeholder="Auswahl", style={'width': '300px'}),
                html.Div([
                    html.Label("ESG-Faktoren (MSCI):", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                ]),
                dcc.Dropdown(id='column-select-dropdown', options=factor_options, multi=True, placeholder='Auswahl'),
                html.Div([
                    html.Label("Upload-Listen:", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                ]),
                dcc.Dropdown(id='lists-select-dropdown', options=list_options_with_dates, multi=True, placeholder='Auswahl'),
                html.Button("Export", id="export-data-button", style={'margin-top': '10px'}),
                dcc.Download(id="download-dataframe-csv"),
                dcc.Loading(
                    id="loading-data-table",
                    type="default",
                    children=[
                        html.Div(id='datatable', children=[
                        dag.AgGrid(
                            id='data-table',
                            columnDefs=[
                                {'headerName': 'ISIN', 'field': 'ISSUER_ISIN', 'filter': 'agTextColumnFilter'},
                                {'headerName': 'Issuer Name', 'field': 'ISSUER_NAME', 'filter': 'agTextColumnFilter'},
                            ],
                            defaultColDef={'flex': 1, 'filter': True, 'sortable': True, 'editable': False, 'floatingFilter': True},
                            dashGridOptions={"localeText": german_locale_text},
                            style={'height': '500px', 'width': '100%'},
                        )
                        ]),
                    ]
                )
            ], style={'margin': '0 5%'}),
        ]),
        dcc.Tab(value='tab-3', label="ESG Datenimport", children=[
            dcc.Tabs([
                dcc.Tab(label='Manueller Datenimport', children=[
                    html.Div([
                        html.H6("Die Daten werden regelmäßig einmal am Tag heruntergeladen. Wenn Sie den Datenabruf jetzt starten wollen, drücken Sie auf die Starttaste. Der Abruf wird sofort gestartet und ist in etwa 20 Minuten fertig.",
                                style={'margin-top': '20px', 'margin-bottom': '20px'}),
                        dcc.Interval(id='interval-component', interval=1 * 1000, n_intervals=0, disabled=True),
                        html.Button("Abruf starten", id="run-script-button"),
                        dcc.Loading(children=[html.Pre(id="output"),]),
                    ], style={'margin': '0 5%'}),
                ]),
                dcc.Tab(label='Letzter Import Log', children=[
                    html.Pre(id="output-historylog"),
                    dcc.Interval(id='trigger-on-load-history', interval=1, n_intervals=0, max_intervals=1)
                ]),
            ]),
        ]),
        dcc.Tab(value='tab-4', label='Import-Faktoren bearbeiten', children=[
            html.Div([
                html.Div([
                    html.H6("In der aktuellen Ansicht sehen Sie alle Faktoren, die von der MSCI API abgerufen werden. Sie können hier zusätzliche Faktoren hinzufügen oder bestehende deaktivieren. Die Änderung des technischen Namens nimmt Zeit in Anspruch (~30 Sekunden), da das Schema der Datenbank geändert wird.",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
                ]),
                dcc.Input(id='new-factor-name-input', type='text', placeholder='Technischer Name', style={'margin-top': '10px', 'margin-bottom': '0px'}),
                html.Br(),
                dcc.Input(id='new-factor-mapping-input', type='text', placeholder='Interne Bezeichnung', style={'margin-top': '10px', 'margin-bottom': '10px'}),
                html.Br(),
                html.Button('Neuen Faktor hinzufügen', id='add-factor-button'),
                html.Div(id='add-factor-output', style={'margin-top': '10px', 'margin-bottom': '10px'}),
                html.Div([
                    html.Label("MSCI-Suche:", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                ]),
                dcc.Dropdown(id='factor-select-dropdown', options=factor_options, multi=True, placeholder='Auswahl'),
                html.Br(),
                html.Div(id='add-factor-output'),
                html.Div(id='dummy-div', style={'display': 'none'}),
                dcc.Loading(
                    id="loading-factor-table",
                    type="default",
                    children=[
                        dag.AgGrid(
                            id='factor-table',
                            columnDefs=[
                                {'headerName': 'Technischer Name', 'field': 'Name', 'editable': True},
                                {'headerName': 'Interne Bezeichnung', 'field': 'Mapping', 'editable': True},
                                {'headerName': 'Status', 'field': 'Status', 'editable': True, 'cellEditor': 'agSelectCellEditor', 'cellEditorParams': {'values': ['Aktiv', 'Inaktiv']}},
                            ],
                            defaultColDef={'flex': 1, 'filter': True, 'sortable': True, 'floatingFilter': True},
                            style={'height': '500px', 'width': '100%'},
                            dashGridOptions={"localeText": german_locale_text},
                            rowData=[],  # Initial empty data, to be filled in callback
                        )
                    ]
                )
            ], style={'margin': '0 5%'}),
        ]),
        dcc.Tab(value='tab-5', label='Übersicht der Upload-Listen', children=[
            html.Div([
                html.Div([
                    html.H6("In der aktuellen Ansicht sehen Sie alle Listen.",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
                ]),
                html.Div([
                    html.Label("Typ:", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                ]),
                dcc.Dropdown(id='list-typ-dropdown', options=['Positiv', 'Negativ'], multi=True, placeholder='Typ filtern'),
                html.Div([
                    html.Label("Status:", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                ]),
                dcc.Dropdown(id='list-status-dropdown', options=['Aktiv', 'Inaktiv'], multi=True, placeholder='Status filtern'),
                html.Div([
                    html.Label("Listen-Suche:", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                ]),
                dcc.Dropdown(id='list-select-dropdown', options=list_options, multi=True, placeholder='Listen filtern'),
                html.Br(),
                html.Div(id='add-list-output'),
                html.Div(id='dummy-div', style={'display': 'none'}),
                dcc.Loading(
                    id="loading-list-table",
                    type="default",
                    children=[
                        dag.AgGrid(
                            id='list-table',
                            columnDefs=[
                                {'headerName': 'Name', 'field': 'Name', 'editable': True},
                                {'headerName': 'Kommentar', 'field': 'Comment', 'editable': True},
                                {'headerName': 'Gültigkeitsdatum der letzten Upload-Liste', 'field': 'Date', 'editable': False},
                                {'headerName': 'Typ', 'field': 'Type', 'editable': True, 'cellEditor': 'agSelectCellEditor', 'cellEditorParams': {'values': ['Positiv', 'Negativ']}},
                                {'headerName': 'Anzahl Uploads', 'field': 'Anzahl', 'editable': False},
                                {'headerName': 'Status', 'field': 'Status', 'editable': True, 'cellEditor': 'agSelectCellEditor', 'cellEditorParams': {'values': ['Aktiv', 'Inaktiv']}},
                            ],
                            defaultColDef={'flex': 1, 'filter': True, 'sortable': True, 'floatingFilter': True},
                            style={'height': '500px', 'width': '100%'},
                            dashGridOptions={"localeText": german_locale_text},
                            rowData=[],  # Initial empty data, to be filled in callback
                        )
                    ]
                )
            ], style={'margin': '0 5%'}),
        ]),
        dcc.Tab(value='tab-6', label='Neue Liste hochladen', children=[
            html.Div([
                html.Div([
                    html.H6("In der aktuellen Ansicht kannst du die neue Liste auf den Server hochladen. Das System akzeptiert Excel- und Pdf-Dokumente. Du kannst die Liste unter dem bestehenden Namen anordnen, oder einen neuen Namen eingeben. Wenn der neue Name angegeben wird, wird die Auswahl der Liste vernachlässigt.",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
                ]),
                dcc.Upload(
                    id='upload-data',
                    children=html.Div([
                        'Drag and Drop oder ',
                        html.A('Datei wählen')
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
                html.Div(id='output-file-name', style={'margin-top': '20px', 'margin-bottom': '20px'}),
                html.Div(children=[
                    dcc.RadioItems(
                        id='list-choice',
                        options=[
                            {'label': 'Neue Liste anlegen', 'value': 'new'},
                            {'label': 'Zur bestehenden Liste hinzufügen', 'value': 'existing'}
                        ],
                        value='new',
                        style={'margin': '20px 0'}
                    )
                ], style={'margin': '10px 0'}),
                dcc.Dropdown(id='input-sheet', placeholder='Tabellenblatt wählen', style={'margin-top': '20px', 'width': '330px'}),
                html.Div(children=[
                    dcc.Dropdown(id='input-name-dropdown', options=fetch_all_lists_names(), placeholder='Name wählen', style={'margin': '0 10px 0 0px', 'width': '330px !important'}),
                    dcc.Input(id='input-name', type='text', placeholder='Name', style={'margin': '0 10px 10px 0', 'width': '330px !important'}),
                ]),
                dcc.Textarea(id='input-description', placeholder='Kommentar', style={'margin': '0 10px 0 0px', 'width': '330px'}),
                dcc.Dropdown(id='input-type', options=[{'label': 'Positiv', 'value': 'Positiv'}, {'label': 'Negativ', 'value': 'Negativ'}], placeholder='Typ wählen', style={'margin': '10px 10px 10px 0px', 'width': '330px !important'}),
                dcc.DatePickerSingle(id='input-date', placeholder='Gültigkeitsdatum wählen', clearable=True, display_format='DD.MM.YYYY', style={'margin': '10px 0px 10px 0px', 'width': '300px', 'display': 'block'}),
                html.Div(children=[
                    html.Button('Hochladen', id='upload-button', style={'margin': '0px 0px 10px 0px'}),
                ]),
                dcc.Loading(
                    id="loading-upload-result",
                    type="default",
                    children=[html.Div(id='output-upload', style={'margin': '10px 10px 10px 10px'})]
                ),
            ], style={'margin': '0 5%'}),
        ]),
    ]),
])

# Callback to update file name after upload
@app.callback(Output('output-file-name', 'children'), [Input('upload-data', 'filename')])
def update_file_name(filename):
    if filename:
        return html.Div([html.B('Ausgewählte Datei: '), filename])
    else:
        return html.Div()

# Callback to update the dropdown options based on the uploaded file
@app.callback([Output('input-sheet', 'options'), Output('input-sheet', 'style')],
              [Input('upload-data', 'contents')],
              [State('upload-data', 'filename')])
def update_dropdown_options(contents, filename):
    if contents is not None and filename.endswith('.xlsx'):
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        try:
            excel_file = pd.ExcelFile(io.BytesIO(decoded))
            sheet_names = excel_file.sheet_names
            options = [{'label': sheet, 'value': sheet} for sheet in sheet_names]
            options.insert(0, {'label': 'Alle', 'value': 'All'})
            return options, {'display': 'block'}
        except Exception as e:
            print("Error:", e)
            return [{'label': 'Fehler beim Lesen der Datei', 'value': 'error'}], {'display': 'none'}
    else:
        return [], {'display': 'none'}

# Callback to update input fields based on filename
@app.callback([Output('input-type', 'value'), Output('input-date', 'date')],
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
            date_from_filename = date.strftime('%d-%m-%Y')
        except:
            pass
        type_from_filename = 'Positiv' if 'positiv' in filename.lower() else 'Negativ' if 'negativ' in filename.lower() else None
    return type_from_filename, date_from_filename

# Callback to handle file upload
@app.callback(
    [
        Output('output-upload', 'children'),
        Output('list-select-dropdown', 'options', allow_duplicate=True),
        Output('lists-select-dropdown', 'options', allow_duplicate=True),
        Output('input-name-dropdown', 'options'),
        Output('input-name', 'value'),
        Output('input-description', 'value'),
        Output('input-type', 'value', allow_duplicate=True),
        Output('input-date', 'date', allow_duplicate=True),
        Output('input-sheet', 'value'),
        Output('upload-data', 'filename'),
        Output('upload-data', 'contents'),
        Output('list-choice', 'value')
    ],
    [Input('upload-button', 'n_clicks')],
    [
        State('input-name-dropdown', 'value'),
        State('input-name', 'value'),
        State('input-description', 'value'),
        State('input-type', 'value'),
        State('input-date', 'date'),
        State('input-sheet', 'value'),
        State('date-dropdown', 'value'),
        State('upload-data', 'filename'),
        State('upload-data', 'contents'),
        State('list-choice', 'value')
    ],
    prevent_initial_call=True
)
def upload_file(n_clicks, name_dropdown, name, description, type, date, sheet, selected_date, filename, contents, list_choice):
    ctx = dash.callback_context
    if not ctx.triggered:
        trigger_id = 'No clicks yet'
    else:
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]

    list_options = [{'label': col, 'value': col} for col in get_list_names()]
    list_options_with_dates = get_list_names_with_dates()
    input_name = fetch_all_lists_names()

    if trigger_id == 'upload-button':
        if list_choice == 'new':
            name = name
        elif name_dropdown:
            name = name_dropdown
        else:
            return html.Div(f'Bitte Name eingeben'), list_options, list_options_with_dates, input_name, '', '', '', None, '', None, None, 'new'
        if date:
            date = str(date)
        else:
            return html.Div(f'Bitte Datum eingeben'), list_options, list_options_with_dates, input_name, '', '', '', None, '', None, None, 'new'
        if description:
            description = description
        else:
            description = ""
        if list_choice == 'new':
            if type:
                type = type
            else:
                return html.Div(f'Bitte Typ wählen'), list_options, list_options_with_dates, input_name, '', '', '', None, '', None, None, 'new'
        elif name_dropdown:
            query_type = f"SELECT TOP 1 type FROM lists WHERE name = ?"
            type = "Kein Typ ausgewälht"
            with pyodbc.connect(cnxn_string) as conn:
                df = pd.read_sql_query(query_type, conn, params=[name_dropdown])
                if not df.empty:
                    type = df.loc[0, 'type']

        if filename:
            try:
                isins = []
                content_type, content_string = contents.split(',')
                decoded = base64.b64decode(content_string)
                with open(filename, 'wb') as f:
                    f.write(decoded)
                if filename.endswith('.xlsx'):
                    if sheet:
                        df = parse_excel(filename, sheet)
                    else:
                        df = parse_excel(filename, 'All')
                    if df is not None:
                        isins = df
                if filename.endswith('.pdf'):
                    df = parse_pdf(filename)
                    if df is not None:
                        isins = df

                new_record_id = add_list_record(name, description, type, date, filename)
                add_isins_to_list(isins, new_record_id)
                list_options = [{'label': col, 'value': col} for col in get_list_names()]
                if not selected_date:
                    list_options_with_dates = get_list_names_with_dates()
                else:
                    list_options_with_dates = get_list_names_with_dates_till_date(selected_date)
                input_name = fetch_all_lists_names()
                num_isins_added = len(isins)
                return html.Div(f"Datei erfolgreich hochgeladen! {num_isins_added} ISINs wurden hinzugefügt."), list_options, list_options_with_dates, input_name, '', '', '', None, '', None, None, 'new'
            except Exception as e:
                list_options = [{'label': col, 'value': col} for col in get_list_names()]
                if not selected_date:
                    list_options_with_dates = get_list_names_with_dates()
                else:
                    list_options_with_dates = get_list_names_with_dates_till_date(selected_date)
                input_name = fetch_all_lists_names()
                return html.Div(f'Fehler bei der Verarbeitung der Datei: {e}'), list_options, list_options_with_dates, input_name, '', '', '', None, '', None, None, 'new'
        else:
            list_options = [{'label': col, 'value': col} for col in get_list_names()]
            if not selected_date:
                list_options_with_dates = get_list_names_with_dates()
            else:
                list_options_with_dates = get_list_names_with_dates_till_date(selected_date)
            input_name = fetch_all_lists_names()
            return html.Div('Keine Datei ausgewählt.'), list_options, list_options_with_dates, input_name, '', '', '', None, '', None, None, 'new'
    list_options = [{'label': col, 'value': col} for col in get_list_names()]
    if not selected_date:
        list_options_with_dates = get_list_names_with_dates()
    else:
        list_options_with_dates = get_list_names_with_dates_till_date(selected_date)
    input_name = fetch_all_lists_names()
    return html.Div(), list_options, list_options_with_dates, input_name, '', '', '', None, '', None, None, 'new'

@app.callback(
    Output('output-historylog', 'children'),
    [Input('trigger-on-load-history', 'n_intervals')]
)
def update_log_contents(n_intervals):
    if n_intervals == 0:
        return "Loading..."
    try:
        with open(output_file_history, 'r') as file:
            data = file.read()
            return data
    except Exception as e:
        return f"Fehler beim Lesen der Logdatei: {str(e)}"

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
    open(output_file, 'w').close()
    thread = threading.Thread(target=run_script, daemon=True)
    thread.start()
    return False

def run_script():
    with open(output_file, "w") as file, open(output_file_history, "w") as file_history:
        process = subprocess.Popen(['python', 'utils/api_connection.py'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        for line in iter(process.stdout.readline, ''):
            file.write(line)
            file.flush()
            file_history.write(line)
            file_history.flush()
            # Debugging: Print to console
            print(line, end='')


@app.callback(
    Output('company-table', 'rowData'),
    [Input('add-company-input', 'value')]
)
def display_filtered_company_data(isin_input):
    if not isin_input:
        query = "SELECT TOP 1000 * FROM company"
    else:
        query = f"SELECT * FROM company WHERE ISSUER_ISIN LIKE '%{isin_input}%' OR ISSUER_NAME LIKE '%{isin_input}%'"
    try:
        with pyodbc.connect(cnxn_string) as conn:
            df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    except Exception as e:
        print(e)
        return []

@app.callback(
    [Output('factor-table', 'rowData'), Output('add-factor-output', 'children')],
    [Input('factor-table', 'cellValueChanged'), Input('factor-select-dropdown', 'value'), Input('add-factor-button', 'n_clicks')],
    [State('new-factor-name-input', 'value'), State('new-factor-mapping-input', 'value'), State('factor-table', 'rowData')]
)
def update_table(event, factor_input_dropdown, n_clicks, new_factor_name, new_factor_mapping, data_current):
    ctx = dash.callback_context
    triggered_component = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else None
    if triggered_component == 'dummy-div.children' or triggered_component == 'add-factor-button.n_clicks':
        try:
            with pyodbc.connect(cnxn_string) as conn:
                query = "SELECT * FROM factors ORDER BY ID DESC"
        except Exception as e:
            print(str(e))
    if triggered_component == 'add-factor-button':
        if not new_factor_name or not new_factor_mapping:
            error_message = "Bitte geben Sie sowohl einen Faktornamen als auch ein Mapping ein."
        else:
            try:
                with pyodbc.connect(cnxn_string) as conn:
                    cursor = conn.cursor()
                    alter_query = f"ALTER TABLE company_data ADD [{new_factor_name}] nvarchar(50)"
                    cursor.execute(alter_query)
                    insert_query = f"INSERT INTO factors (Name, Status, Mapping) VALUES ('{new_factor_name}', 1, '{new_factor_mapping}')"
                    cursor.execute(insert_query)
                    conn.commit()
            except Exception as e:
                if conn:
                    conn.rollback()
                return data_current, f"Fehler beim Hinzufügen des Faktors: {str(e)}"
    if not factor_input_dropdown:
        factor_input_dropdown = get_factor_names()
    if triggered_component == 'factor-table':
        pass
        if event is not None:
            changed_data = event[0]['data']
            factor_id = changed_data['Id']
            update_queries = []
            column_name_change_query = None
            old_name = None
            key = event[0]['colId']
            value = event[0]['value']
            old_name = event[0]['oldValue']
            if key == 'Status':
                update_query = f"UPDATE factors SET {key} = 1 WHERE Id = {factor_id}" if value == 'Aktiv' else f"UPDATE factors SET {key} = 0 WHERE Id = {factor_id}"
                update_queries.append(update_query)
            elif key == 'Name':
                # old_name = event['oldValue']
                new_name = value
                update_query = f"UPDATE factors SET {key} = '{value}' WHERE Id = {factor_id}"
                update_queries.append(update_query)
                column_name_change_query = f"EXEC sp_rename 'company_data.[{old_name}]', '{new_name}', 'COLUMN';"
                update_queries.append(column_name_change_query)
            else:
                update_query = f"UPDATE factors SET {key} = '{value}' WHERE Id = {factor_id}"
                update_queries.append(update_query)
            try:
                with pyodbc.connect(cnxn_string) as conn:
                    cursor = conn.cursor()
                    for query in update_queries:
                        cursor.execute(query)
                    conn.commit()
            except Exception as e:
                error_message = f"An error occurred while updating data: {str(e)}"
                print(error_message)

        return data_current, ""
    else:
        conditions = " OR ".join([f"Mapping LIKE '{factor}'" for factor in factor_input_dropdown])
        query = f"SELECT * FROM factors WHERE {conditions} ORDER BY ID DESC "
    try:
        conn = pyodbc.connect(cnxn_string)
        df = pd.read_sql_query(query, conn)
        df['Status'] = df['Status'].apply(lambda x: 'Aktiv' if x == 1 else 'Inaktiv')
        return df.to_dict('records'), ""
    except Exception as e:
        print(f"Fehler beim Abrufen der Faktoren: {str(e)}")
        return data_current, f"Fehler beim Abrufen der Faktoren: {str(e)}"
        # with pyodbc.connect(cnxn_string) as conn:
        #     df = pd.read_sql_query(query, conn)
        #     df['Status'] = df['Status'].apply(lambda x: 'Aktiv' if x == 1 else 'Inaktiv')
        #     return df.to_dict('records'), ""

@app.callback(
    [Output('list-table', 'rowData'),
     Output('list-select-dropdown', 'options'),
     Output('lists-select-dropdown', 'options')],
    [Input('list-table', 'cellValueChanged'), Input('list-select-dropdown', 'value'), Input('list-status-dropdown', 'value'), Input('list-typ-dropdown', 'value')],
    [State('list-table', 'rowData')]
)
def update_table(event, list_input_dropdown, list_status_dropdown, list_typ_dropdown, data_current):
    ctx = dash.callback_context
    triggered_component = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else None

    list_options = [{'label': col, 'value': col} for col in get_list_names()]
    list_options_with_dates = get_list_names_with_dates()

    if not list_status_dropdown:
        list_status_dropdown = ['Aktiv', 'Inaktiv']

    if not list_typ_dropdown:
        list_typ_dropdown = ['Positiv', 'Negativ']

    columns = [
        {'headerName': 'Name', 'field': 'Name', 'editable': True},
        {'headerName': 'Gültigkeitsdatum der letzten Upload-Liste', 'field': 'Date', 'editable': False},
        {'headerName': 'Typ', 'field': 'Type', 'editable': True, 'cellEditor': 'agSelectCellEditor', 'cellEditorParams': {'values': ['Positiv', 'Negativ']}},
        {'headerName': 'Anzahl Uploads', 'field': 'Anzahl', 'editable': False},
        {'headerName': 'Status', 'field': 'Status', 'editable': True, 'cellEditor': 'agSelectCellEditor', 'cellEditorParams': {'values': ['Aktiv', 'Inaktiv']}},
    ]

    if triggered_component == 'list-table':
        if event is not None:
            changed_data = event[0]['data']
            print(event)
            list_id = changed_data['Name']
            update_queries = []
            old_name = None
            key = event[0]['colId']
            value = event[0]['value']
            if key == 'Name' and event[0]['oldValue']:
                old_name = event[0]['oldValue']
                update_query = f"UPDATE lists SET name = '{value}' WHERE name = '{event[0]['oldValue']}'"
                update_queries.append(update_query)
            elif key == 'Status':
                update_query = f"UPDATE lists SET status = 1 WHERE name = '{list_id}'" if value == 'Aktiv' else f"UPDATE lists SET status = 0 WHERE name = '{list_id}'"
                update_queries.append(update_query)
            elif key == 'Comment':
                update_query = f"UPDATE lists SET comment = '{value}' WHERE id = '{list_id}'"
                update_queries.append(update_query)
            elif key == 'Type':
                update_query = f"UPDATE lists SET type = '{value}' WHERE name = '{list_id}'"
                update_queries.append(update_query)
            try:
                with pyodbc.connect(cnxn_string) as conn:
                    cursor = conn.cursor()
                    for query in update_queries:
                        cursor.execute(query)
                    conn.commit()
                # Update data_current to reflect changes in the 'Name' column
                if key == 'Name' and old_name:
                    for row in data_current:
                        if row['Name'] == old_name:
                            row['Name'] = value

            except Exception as e:
                error_message = f"An error occurred while updating data: {str(e)}"
        return data_current, list_options, list_options_with_dates
    else:
        if not list_input_dropdown:
            list_input_dropdown = get_list_names()
            conditions = " OR ".join([f"name = '{list}'" for list in list_input_dropdown])
            query = f"""
                SELECT
                    MAX(l.id) as id,
                    l.Name,
                    MAX(l.Date) AS Date,
                    COUNT(*) AS Anzahl,
                    l.Status, l.Type
                FROM
                    lists l
                WHERE {conditions}
                GROUP BY
                    l.name, l.status, l.type
            """
            with pyodbc.connect(cnxn_string) as conn:
                df = pd.read_sql_query(query, conn)
                df['Status'] = df['Status'].apply(lambda x: 'Aktiv' if x == 1 else 'Inaktiv')
                df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%d.%m.%Y')
                filtered_df = df[df['Status'].isin(list_status_dropdown) & df['Type'].isin(list_typ_dropdown)]
                return filtered_df.to_dict('records'), list_options, list_options_with_dates
        else:
            conditions = " OR ".join([f"l.Name = '{list}'" for list in list_input_dropdown])
            query = f"""
                SELECT
                    l.id,
                    MAX(l.Name) AS Name,
                    MAX(l.Comment) AS Comment,
                    MAX(l.Date) AS Date,
                    COUNT(DISTINCT ld.isin) AS AnzahlIsin,
                    MAX(CONVERT(INT, l.Status)) AS Status,
                    MAX(l.Type) AS Type
                FROM
                    lists l
                LEFT JOIN
                    lists_data ld ON l.id = ld.list_id
                WHERE {conditions}
                GROUP BY
                    l.id
                ORDER BY
                    MAX(l.Name) DESC,
                    MAX(l.Date) DESC
            """
            with pyodbc.connect(cnxn_string) as conn:
                df = pd.read_sql_query(query, conn)
                df['Status'] = df['Status'].apply(lambda x: 'Aktiv' if x == 1 else 'Inaktiv')
                df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%d.%m.%Y')
                filtered_df = df[df['Status'].isin(list_status_dropdown) & df['Type'].isin(list_typ_dropdown)]
                return filtered_df.to_dict('records'), list_options, list_options_with_dates

@app.callback(
    Output("download-dataframe-csv", "data"),
    Input("export-data-button", "n_clicks"),
    State('data-table', 'rowData'),
    State('column-select-dropdown', 'value'),
    State('lists-select-dropdown', 'value'),
    prevent_initial_call=True
)
def generate_csv(n_clicks, data, selected_columns, selected_lists):
    if n_clicks is None:
        raise PreventUpdate
    df = pd.DataFrame(data)

    default_columns = ['ISSUER_ISIN', 'ISSUER_NAME']
    if selected_columns:
        selected_columns = default_columns + selected_columns
    else:
        selected_columns = default_columns

    export_columns = [col for col in selected_columns if col in df.columns]

    if selected_lists:
        export_columns.extend([col for col in selected_lists if col in df.columns])

    df = df[export_columns]
    filename = f"exported_data_{datetime.datetime.now().strftime('%d.%m.%Y-%H:%M:%S')}.xlsx"
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name="Data", index=False)
        workbook = writer.book
        worksheet = writer.sheets["Data"]
        for col in df.columns:
            max_length = max(df[col].astype(str).map(len).max(), len(col))
            adjusted_width = max_length + 2
            column_index = df.columns.get_loc(col)
            column_letter = openpyxl.utils.get_column_letter(column_index + 1)
            worksheet.column_dimensions[column_letter].width = adjusted_width
    return dcc.send_file(filename)

@app.callback(
    Output('lists-select-dropdown', 'options', allow_duplicate=True),
    Input('date-dropdown', 'value'), prevent_initial_call=True
)
def update_lists_dropdown(selected_date):
    if not selected_date:
        list_options_with_dates = get_list_names_with_dates()
        return list_options_with_dates
    try:
        list_options_with_dates = get_list_names_with_dates_till_date(selected_date)
        return list_options_with_dates
    except Exception as e:
        print(str(e))
        list_options_with_dates = get_list_names_with_dates()
        return list_options_with_dates

@app.callback(
    Output('datatable', 'children'),
    Input('date-dropdown', 'value'),
    Input('column-select-dropdown', 'value'),
    Input('lists-select-dropdown', 'value')
)
def update_data_table(selected_date, selected_columns, selected_lists):
    try:
        if not selected_date:
            return []

        conn = pyodbc.connect(cnxn_string)
        mapping_query = "SELECT Name, Mapping FROM factors WHERE Status = 1"
        mapping_df = pd.read_sql_query(mapping_query, conn)
        column_mapping = dict(zip(mapping_df['Name'], mapping_df['Mapping']))

        if not selected_columns:
            selected_columns = []
        else:
            selected_columns = [col for col in selected_columns if col in mapping_df['Mapping'].tolist()]

        list_columns = {}
        if not selected_lists:
            selected_lists = []
        else:
            lists_query = """
                SELECT id, name FROM (
                    SELECT id, name, date,
                           ROW_NUMBER() OVER (PARTITION BY name ORDER BY date DESC) as rn
                    FROM lists
                    WHERE status = 1 AND name IN ({}) AND date <= '{}'
                ) a WHERE rn = 1
            """.format(','.join(f"'{name}'" for name in selected_lists), selected_date)
            list_df = pd.read_sql_query(lists_query, conn)
            for index, row in list_df.iterrows():
                isin_query = f"SELECT isin FROM lists_data WHERE list_id = {row['id']}"
                isins_list_df = pd.read_sql_query(isin_query, conn)
                list_columns[row['name']] = isins_list_df['isin'].tolist()

        query = f"SELECT TOP 1000 * FROM company RIGHT JOIN company_data ON company.CompanyID = company_data.CompanyID WHERE DataDate = '{selected_date}'"
        df = pd.read_sql_query(query, conn)

        all_isins = set()
        for isins in list_columns.values():
            all_isins.update(isins)
        current_isins = set(df['ISSUER_ISIN'].tolist())
        new_isins = all_isins - current_isins
        df_list = df.to_dict('records')
        for isin in new_isins:
            new_row = {'ISSUER_ISIN': isin}
            for col in df.columns:
                if col != 'ISSUER_ISIN':
                    new_row[col] = None
            df_list.append(new_row)
        df = pd.DataFrame(df_list)

        for list_name, isins in list_columns.items():
            df[list_name] = df['ISSUER_ISIN'].apply(lambda x: 'Ja' if x in isins else 'Nein')

        df.rename(columns=column_mapping, inplace=True)

        conn.close()
        issuer_isin_column = {'headerName': 'ISIN', 'field': 'ISSUER_ISIN', 'filter': 'agTextColumnFilter'}
        issuer_name_column = {'headerName': 'Firmenname', 'field': 'ISSUER_NAME', 'filter': 'agTextColumnFilter'}
        columns = [{'headerName': col, 'field': col, 'filter': 'agTextColumnFilter'} for col in df.columns if col in selected_columns]
        for col in selected_lists:
            columns.append({'headerName': col, 'field': col, 'filter': 'agTextColumnFilter'})
        columns.insert(0, issuer_name_column)
        columns.insert(0, issuer_isin_column)

        return [
            dag.AgGrid(
                id='data-table',
                columnDefs=columns,
                rowData=df.to_dict('records'),
                defaultColDef={'flex': 1, 'filter': True, 'sortable': True, 'editable': False, 'floatingFilter': True},
                dashGridOptions={"localeText": german_locale_text},
                style={'height': '500px', 'width': '100%'},
            )
        ]
    except Exception as e:
        return str(e)

@app.callback(
    [Output('input-name-dropdown', 'style'), Output('input-name', 'style'), Output('input-type', 'style')],
    [Input('list-choice', 'value')]
)
def toggle_input_fields(choice):
    if choice == 'existing':
        return {'display': 'block', 'width': '330px'}, {'display': 'none'}, {'display': 'none'}
    else:
        return {'display': 'none'}, {'display': 'block', 'width': '330px'}, {'display': 'block', 'width': '330px'}

server = app.server

if __name__ == '__main__':
    if os.path.exists("selected_date.txt"):
        os.remove("selected_date.txt")
    # app.run_server(debug=True, host='0.0.0.0', port=8050) #To run on server
    app.run_server(debug=True) #To run locally
