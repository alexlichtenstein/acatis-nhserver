import dash
from dash import dcc, html
from dash.dependencies import Input, Output
from import_data import update_company_data_dummy
import pyodbc
import pandas as pd
# Corrected import for DataTable
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

def get_unique_dates():
    try:
        with pyodbc.connect(cnxn_string) as conn:
            # Fetch the unique dates
            df = pd.read_sql_query("SELECT DISTINCT DataDate FROM company_data", conn)

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

app = dash.Dash(__name__, external_stylesheets=['styles.css',dbc.themes.BOOTSTRAP], long_callback_manager=long_callback_manager)
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

app.title = "Nachhaltigkeit Server"
# Define the layout of the app
app.layout = html.Div([
    html.Div([
        html.Img(src="/assets/logo.png", style={'height': '43px'}),
        html.H1("Nachhaltigkeit Server", style={'margin': '0', 'padding-right': '10px', 'margin-left': 'auto'}),
    ], style={'display': 'flex'}),

    dcc.Tabs([
        # Tab 1: ISINS
        dcc.Tab(label='Unternehmens Übersicht', children=[
            html.Div([
                dcc.Input(
                    id='add-company-input',
                    type='text',
                    placeholder='Unternehmens-ISIN eingeben',
                    style={'width': '20%'}
                ),
                html.Button('Suchen nach ISIN', id='add-company-button'),
                html.Br(),
                html.Div(id='add-company-output'),
                html.Div(id='dummy-div', style={'display': 'none'}),
                # html.Button('Show Company Data', id='show-company-data-button'),
                dash_table.DataTable(
                    id='company-table',
                    columns=[
                        {'name': 'ISIN', 'id': 'ISSUER_ISIN'},
                        {'name': 'Issuer Name', 'id': 'ISSUER_NAME'},
                        {'name': 'Company ID', 'id': 'CompanyID'},


                        # Add more columns as needed
                    ],
                    data=[],
                    style_table={'maxHeight': '80vh', 'overflowY': 'auto'},
                ),
            ], style={'margin': '0 5%'}),
        ]),

        # Tab 2: Data Tab
        dcc.Tab(label='ESG Daten', children=[
            html.Div([
                html.Div(""),
                html.Label("Filter:"),  # Add a label for the filters
                html.Div(""),
                # dcc.DatePickerSingle(
                #     id='date-picker',
                #     placeholder="Date",
                #     display_format='dddd, MMMM D, YYYY',  # Set a longer date format
                #     style={'width': '500px'},  # Increase the width of the date picker
                # ),
                dcc.Dropdown(
                    id='date-dropdown',
                    options=get_unique_dates(),
                    placeholder="Wählen Sie ein Datum",
                    style={'width': '300px'}  # Adjust the style as needed
                ),
                html.Div(style={'width': '10px'}),
                dcc.Dropdown(
                        id='column-select-dropdown',
                        options=column_options,
                        multi=True,
                        placeholder='Spalten filtern'
                    ),
                html.Div(style={'width': '10px'}),
                dcc.Input(
                    id='isins-input',
                    type='text',
                    placeholder='ISINS eingeben (Komma-getrennt)',
                    style={'width': '300px;height:48px;'}  # Increase the width of the input field
                ),
                html.Button('Filter ISINS', id='show-data-button'),  # Rename the button
                dash_table.DataTable(id='data-table')  # Corrected usage here
            ], style={'margin': '0 5%'}),
        ]),


        # Add more tabs if needed

        dcc.Tab(label='Manuelle Daten-Import', children=[
            html.Div([
                dcc.Interval(id='interval-component', interval=1 * 1000, n_intervals=0),
                html.Button("Skript starten", id="run-script-button"),
                html.Pre(id="output"),
            ], style={'margin': '0 5%'}),
        ]),

        # Tab for Company Data Columns
        dcc.Tab(label='Faktoren bearbeiten', children=[
            html.Div([

                html.Div([
                    html.Label("Neuer Faktor Name:"),
                    dcc.Input(
                        id='new-column-name-input',
                        type='text',
                        placeholder='Faktor Name eingeben',
                    ),
                    html.Button('Neuer Faktor hinzufügen', id='add-column-button'),
                    html.Button('Faktor löschen', id='delete-column-button'),
                    html.Div(id='add-column-output')
                ]),
                # html.H4("List of Existing Data Fields:"),
                dash_table.DataTable(
                    id='company-data-columns-table',
                    columns=[
                        {'name': 'Faktor Name', 'id': 'column_name'}
                    ],
                    data=[],
                    style_table={'height': '300px', 'overflowY': 'auto'},
                ),
            ], style={'margin': '0 5%'}),
        ]),
    ]),
])



output_file = "output.txt"

def run_script():
    with open(output_file, "w") as file:
        process = subprocess.Popen(['python', 'utils/api_connection.py'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        for line in iter(process.stdout.readline, ''):
            file.write(line)
            file.flush()

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
    [Input('add-company-button', 'n_clicks')],
    [State('add-company-input', 'value')]
)
def display_filtered_company_data(n_clicks, isin_input):
    if n_clicks is None:
        # On initial load, display all data
        query = "SELECT * FROM company"
    else:
        # If ISIN is provided, filter by ISIN; otherwise, show all data
        if isin_input:
            query = f"SELECT * FROM company WHERE ISSUER_ISIN = '{isin_input}'"
        else:
            query = "SELECT * FROM company"

    try:
        with pyodbc.connect(cnxn_string) as conn:
            df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    except Exception as e:
        return str(e)


@app.callback(
    Output('data-table', 'data'),
    Output('data-table', 'columns'),
    Input('show-data-button', 'n_clicks'),
    Input('date-dropdown', 'value'),
    Input('isins-input', 'value'),
    Input('column-select-dropdown', 'value')
)
def update_data_table(n_clicks, selected_date, isins, selected_columns):
    try:
        if not selected_date:
            # Handle the case where no date is selected
            return [], [{'name': 'Bitte wählen das Datum, um die entsprechenden Daten anzuzeigen', 'id': 'no_data'}]

        # Replace with your SQL query to fetch data from the SQL Server database
        # Establish a connection to the SQL Server database
        conn = pyodbc.connect(cnxn_string)

        # Update the columns of the data table based on selected_columns
        if not selected_columns:
            selected_columns = get_column_names()  # Default to all columns if none are selected

        # Build the SQL query based on selected_date and isins
        query = f"SELECT * FROM company RIGHT JOIN company_data ON company.CompanyID = company_data.CompanyID WHERE DataDate = '{selected_date}'"
        if isins:
            isins_list = [i.strip() for i in isins.split(',')]
            isins_condition = " OR ".join([f"ISSUER_ISIN = '{isin}'" for isin in isins_list])
            query += f" AND ({isins_condition})"

        # Fetch and filter data from the database
        df = pd.read_sql_query(query, conn)

        # Close the database connection
        conn.close()
        issuer_isin_column = {'name': 'ISSUER_ISIN', 'id': 'ISSUER_ISIN'}
        issuer_name_column = {'name': 'ISSUER_NAME', 'id': 'ISSUER_NAME'}
        issuer_id_column = {'name': 'ISSUERID', 'id': 'ISSUERID'}
        columns = [{'name': col, 'id': col} for col in selected_columns]
        columns.insert(0, issuer_isin_column)
        columns.insert(0, issuer_id_column)
        columns.insert(0, issuer_name_column)
        return df.to_dict('records'), columns
    except Exception as e:
        return str(e)


@app.callback(
    Output('company-data-columns-table', 'data'),
    Output('add-column-output', 'children'),
    Input('dummy-div', 'children'),
    Input('add-column-button', 'n_clicks'),
    State('new-column-name-input', 'value'),
)
def display_company_data_columns(_, n_clicks, new_column_name):
    ctx = callback_context
    if not ctx.triggered:
        return dash.no_update, dash.no_update

    triggered_id = ctx.triggered[0]['prop_id']

    if triggered_id == 'dummy-div.children' or triggered_id == 'add-column-button.n_clicks':
        try:
            with pyodbc.connect(cnxn_string) as conn:
                # Fetch and display the existing data fields from the 'company_data' table
                query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'company_data'"
                existing_columns = [row.COLUMN_NAME for row in conn.execute(query)]

            data = [{'column_name': column_name} for column_name in existing_columns]

            if triggered_id == 'add-column-button.n_clicks':
                if not new_column_name:
                    raise ValueError("Bitte geben einen Faktornamen ein")

                with pyodbc.connect(cnxn_string) as conn:
                    # Use the specified new_column_name and add it with the type nvarchar(50)
                    query = f"ALTER TABLE company_data ADD [{new_column_name}] nvarchar(50)"
                    cursor = conn.cursor()
                    cursor.execute(query)
                    conn.commit()

                data.append({'column_name': new_column_name})
                success_message = f"Neuer Faktor '{new_column_name}' erfolgreich hinzugefügt."
                return data, success_message
            else:
                return data, dash.no_update
        except Exception as e:
            return dash.no_update, str(e)
    else:
        return dash.no_update, dash.no_update

server = app.server

if __name__ == '__main__':
    if os.path.exists("selected_date.txt"):
        os.remove("selected_date.txt")
    # app.run_server(debug=True, host='0.0.0.0', port=8050)
    app.run_server(debug=True)
