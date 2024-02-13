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
import datetime

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

def get_unique_isins():
    try:
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
        with pyodbc.connect(cnxn_string) as conn:
            df = pd.read_sql_query("SELECT DISTINCT ISSUER_ISIN FROM company", conn)
        return [{'label': str(isin), 'value': str(isin)} for isin in df['ISSUER_ISIN'].tolist()]
    except Exception as e:
        print(e)
        return []

def get_factor_names():
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
                    html.H6("In der aktuellen Ansicht können Sie alle Unternehmen sehen, die über die MSCI API verfügbar sind. Sie können das Eingabefeld für eine Suche verwenden.",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
                ]),
                dcc.Input(
                    id='add-company-input',
                    type='text',
                    placeholder='ISIN-Suche',
                    style={'width': '20%'}
                ),
                # html.Button('Suchen', id='add-company-button'),
                html.Br(),
                html.Div(id='add-company-output'),
                html.Div(id='dummy-div', style={'display': 'none'}),
                # html.Button('Show Company Data', id='show-company-data-button'),
                dcc.Loading(
                            id="loading-company-table",
                            type="default",  # You can choose from 'graph', 'cube', 'circle', 'dot', or 'default'
                            children=[
                dash_table.DataTable(
                    id='company-table',
                    columns=[
                        {'name': 'ISIN', 'id': 'ISSUER_ISIN'},
                        {'name': 'Issuer Name', 'id': 'ISSUER_NAME'},
                        # Add more columns as needed
                    ],
                    data=[],
                    style_table={'maxHeight': '80vh', 'overflowY': 'auto'},
                    style_cell_conditional=[
                            {'if': {'column_id': 'ISSUER_ISIN'}, 'width': '200px'},  # Set specific width for the ISIN column
                            # {'if': {'column_id': 'ISSUER_NAME'}, 'width': '200px'},  # Set specific width for the Issuer Name column
                            # Add more conditions for other columns as needed
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
                    html.H6("In der aktuellen Ansicht sehen Sie die Daten, die aus den verfügbaren Datenpaketen der MSCI API abgerufen werden. Mit dem Datumsfilter können Sie das ultimative Datum auswählen. Mit weiteren Filtern können Sie die Faktorliste und die ISINs einschränken.",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
                    html.H6("Zum Starten wählen Sie bitte das Datum.",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
                ]),
                html.Div([
                    html.Label("Datum:", style={'margin-top': '10px', 'margin-bottom': '10px'}),  # Add a label for the filters

                ]),

                dcc.Dropdown(
                    id='date-dropdown',
                    options=get_unique_dates(),
                    placeholder="Wählen Sie ein Datum",
                    style={'width': '300px'}  # Adjust the style as needed
                ),
                html.Div([
                    html.Label("Faktorliste:", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                    # Add a label for the filters

                ]),
                dcc.Dropdown(
                        id='column-select-dropdown',
                        options=column_options,
                        multi=True,
                        placeholder='Spalten filtern'
                    ),
                html.Div([
                    html.Label("ISINs Liste:", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                    # Add a label for the filters

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
                                # {'if': {'column_id': 'ISSUER_NAME'}, 'width': '200px'},  # Set specific width for the Issuer Name column
                                # Add more conditions for other columns as needed
                            ],
                            fixed_columns={'headers': True, 'data': 1},
                            style_table={'overflowX': 'auto', 'width':'100%', 'minWidth': '100%'},

                                # Corrected usage here
                            )
                        ]
                    )

            ], style={'margin': '0 5%'}),
        ]),


        # Add more tabs if needed

        dcc.Tab(label='Manueller Datenimport', children=[
            html.Div([
                html.H6("Die Daten werden regelmäßig einmal am Tag heruntergeladen. Wenn Sie den Datenabruf jetzt starten wollen, drücken Sie auf die Starttaste. Der Abruf wird sofort gestartet und ist in etwa 20 Minuten fertig.",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
                dcc.Interval(id='interval-component', interval=1 * 1000, n_intervals=0),
                html.Button("Abruf starten", id="run-script-button"),
                html.Pre(id="output"),
            ], style={'margin': '0 5%'}),
        ]),

        dcc.Tab(label='Faktore Bearbeiten', children=[
            html.Div([
                html.Div([
                    html.H6("In der aktuellen Ansicht sehen Sie alle Faktoren, die von der MSCI API abgerufen werden. Sie können in den Systemeinstellungen zusätzliche Faktoren hinzufügen.",
                            style={'margin-top': '20px', 'margin-bottom': '20px'}),
                ]),
                dcc.Input(
                        id='new-factor-name-input',
                        type='text',
                        placeholder='Faktor Name eingeben',style={'margin-top': '20px', 'margin-bottom': '20px'}
                    ),
                html.Button('Neuen Faktor hinzufügen', id='add-factor-button'),
                html.Div(id='add-factor-output', style={'margin-top': '20px', 'margin-bottom': '10px'}),
                # dcc.Input(
                #     id='add-factor-input',
                #     type='text',
                #     placeholder='Faktor-Suche',
                #     style={'width': '20%'}
                # ),

                html.Div([
                    html.Label("Faktor-Suche:", style={'margin-top': '10px', 'margin-bottom': '10px'}),
                    # Add a label for the filters

                ]),
                dcc.Dropdown(
                        id='factor-select-dropdown',
                        options=factor_options,
                        multi=True,
                        placeholder='Faktore filtern'
                    ),
                # html.Button('Suchen', id='add-company-button'),
                html.Br(),
                # html.Div(id='add-factor-output'),
                html.Div(id='dummy-div', style={'display': 'none'}),
                # html.Button('Show Company Data', id='show-company-data-button'),
                dcc.Loading(
                            id="loading-factor-table",
                            type="default",  # You can choose from 'graph', 'cube', 'circle', 'dot', or 'default'
                            children=[
                dash_table.DataTable(
                    id='factor-table',
                    columns=[
                        {'name': 'Name', 'id': 'Name'},
                        {'name': 'Mapping', 'id': 'Mapping'},
                        {'name': 'Status', 'id': 'Status'},
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
                    style_table={'maxHeight': '80vh', 'overflowY': 'auto'},
                    style_cell_conditional=[
                            {'if': {'column_id': 'Status'}, 'width': '200px'},  # Set specific width for the ISIN column
                            # {'if': {'column_id': 'ISSUER_NAME'}, 'width': '200px'},  # Set specific width for the Issuer Name column
                            # Add more conditions for other columns as needed
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
                            'color': 'white'
                        }
                    ]
                ),
                                ]
                )
            ], style={'margin': '0 5%'}),
        ]),

        # Tab for Company Data Columns
        # dcc.Tab(label='Faktoren bearbeiten', children=[
        #     html.Div([
        #
        #         html.Div([
        #             html.Label("Neuer Faktor Name:"),
        #             dcc.Input(
        #                 id='new-column-name-input',
        #                 type='text',
        #                 placeholder='Faktor Name eingeben',
        #             ),
        #             html.Button('Neuer Faktor hinzufügen', id='add-column-button'),
        #             html.Button('Faktor löschen', id='delete-column-button'),
        #             html.Div(id='add-column-output')
        #         ]),
        #         # html.H4("List of Existing Data Fields:"),
        #         dash_table.DataTable(
        #             id='company-data-columns-table',
        #             columns=[
        #                 {'name': 'Faktor Name', 'id': 'column_name'}
        #             ],
        #             data=[],
        #             style_table={'height': '300px', 'overflowY': 'auto'},
        #         ),
        #     ], style={'margin': '0 5%'}),
        # ]),
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
    [Input('add-company-input', 'value')]
)
def display_filtered_company_data(isin_input):
    if not isin_input:
        # If no ISIN is provided, display default or all data
        query = "SELECT TOP 1000 * FROM company"
    else:
        # Filter by ISIN when input is provided
        query = f"SELECT * FROM company WHERE ISSUER_ISIN LIKE '%{isin_input}%'"

    try:
        with pyodbc.connect(cnxn_string) as conn:
            df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    except Exception as e:
        print(e)  # It's a good practice to log or print errors
        return []


@app.callback(
    [Output('factor-table', 'data'),
     Output('factor-table', 'dropdown'), Output('add-factor-output', 'children')],
    [
     Input('factor-table', 'data_previous'), Input('factor-select-dropdown', 'value'),Input('add-factor-button', 'n_clicks') ],
    State('new-factor-name-input', 'value')
)
def update_table( previous_data, factor_input_dropdown, n_clicks, new_factor_name):
    ctx = dash.callback_context
    triggered_component = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else None

    print('triggered')
    print(triggered_component)
    if triggered_component == 'dummy-div.children' or triggered_component == 'add-column-button.n_clicks':
        try:
            with pyodbc.connect(cnxn_string) as conn:
                # Fetch and display the existing data fields from the 'company_data' table
                query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'company_data'"
                existing_columns = [row.COLUMN_NAME for row in conn.execute(query)]

            data = [{'column_name': column_name} for column_name in existing_columns]
        except Exception as e:
            print(str(e))
    if triggered_component == 'add-factor-button':
        print('buttonadd')
        if not new_factor_name:
            raise ValueError("Bitte geben einen Faktornamen ein")

        with pyodbc.connect(cnxn_string) as conn:
            print('buttonadd1')
                    # Use the specified new_column_name and add it with the type nvarchar(50)
            query = f"ALTER TABLE company_data ADD [{new_factor_name}] nvarchar(50)"
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            print('buttonadd2')
            query = f"INSERT INTO factors (Name, Status, Mapping) VALUES ('{new_factor_name}', 1, '{new_factor_name}')"

            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()

    if not factor_input_dropdown:
        factor_input_dropdown = get_factor_names()  # Default to all columns if none are selected

    if triggered_component == 'factor-table':
        return previous_data, dash.no_update
    else:
        conditions = " OR ".join([f"Name LIKE '{factor}'" for factor in factor_input_dropdown])
        query = f"SELECT * FROM factors WHERE {conditions}"

    try:
        with pyodbc.connect(cnxn_string) as conn:
            df = pd.read_sql_query(query, conn)
            df['Status'] = df['Status'].apply(lambda x: 'Aktiv' if x == 1 else 'Inaktiv')
            dropdown_options = [{'label': i, 'value': i} for i in df['Status'].unique()]

        return df.to_dict('records'), {'Status': {'options': dropdown_options}}, ""
    except Exception as e:
        print(e)
        return [], {'Status': {'options': []}}

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

        # Update the columns of the data table based on selected_columns
        if not selected_columns:
            selected_columns = get_column_names()  # Default to all columns if none are selected

        # Build the SQL query based on selected_date and isins
        query = f"SELECT TOP 1000 * FROM company RIGHT JOIN company_data ON company.CompanyID = company_data.CompanyID WHERE DataDate = '{selected_date}'"
        if selected_isins:
            selected_isins_str = ','.join(f"'{isin}'" for isin in selected_isins)
            query += f" AND ISSUER_ISIN IN ({selected_isins_str})"

        # Fetch and filter data from the database
        df = pd.read_sql_query(query, conn)

        # Close the database connection
        conn.close()
        issuer_isin_column = {'name': 'ISSUER_ISIN', 'id': 'ISSUER_ISIN'}
        issuer_name_column = {'name': 'ISSUER_NAME', 'id': 'ISSUER_NAME'}
        # issuer_id_column = {'name': 'ISSUERID', 'id': 'ISSUERID'}
        columns = [{'name': col, 'id': col} for col in selected_columns]
        columns.insert(0, issuer_name_column)
        columns.insert(0, issuer_isin_column)
        # columns.insert(0, issuer_id_column)

        return df.to_dict('records'), columns
    except Exception as e:
        return str(e)


# @app.callback(
#     Output('company-data-columns-table', 'data'),
#     Output('add-column-output', 'children'),
#     Input('dummy-div', 'children'),
#     Input('add-column-button', 'n_clicks'),
#     State('new-column-name-input', 'value'),
# )
# def display_company_data_columns(_, n_clicks, new_column_name):
#     ctx = callback_context
#     if not ctx.triggered:
#         return dash.no_update, dash.no_update
#
#     triggered_id = ctx.triggered[0]['prop_id']
#
    # if triggered_id == 'dummy-div.children' or triggered_id == 'add-column-button.n_clicks':
    #     try:
    #         with pyodbc.connect(cnxn_string) as conn:
    #             # Fetch and display the existing data fields from the 'company_data' table
    #             query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'company_data'"
    #             existing_columns = [row.COLUMN_NAME for row in conn.execute(query)]
    #
    #         data = [{'column_name': column_name} for column_name in existing_columns]
    #
    #         if triggered_id == 'add-column-button.n_clicks':
    #             if not new_column_name:
    #                 raise ValueError("Bitte geben einen Faktornamen ein")
    #
    #             with pyodbc.connect(cnxn_string) as conn:
    #                 # Use the specified new_column_name and add it with the type nvarchar(50)
    #                 query = f"ALTER TABLE company_data ADD [{new_column_name}] nvarchar(50)"
    #                 cursor = conn.cursor()
    #                 cursor.execute(query)
    #                 conn.commit()
#
#                 data.append({'column_name': new_column_name})
#                 success_message = f"Neuer Faktor '{new_column_name}' erfolgreich hinzugefügt."
#                 return data, success_message
#             else:
#                 return data, dash.no_update
#         except Exception as e:
#             return dash.no_update, str(e)
#     else:
#         return dash.no_update, dash.no_update

server = app.server

if __name__ == '__main__':
    if os.path.exists("selected_date.txt"):
        os.remove("selected_date.txt")
    # app.run_server(debug=True, host='0.0.0.0', port=8050)
    app.run_server(debug=True)
