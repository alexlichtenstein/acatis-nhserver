import os
import uuid
import subprocess
import pandas as pd
import dash
from dash import Dash, dcc, html, dash_table
from dash.dependencies import Input, Output, State
import dash_uploader as du
from dash.exceptions import PreventUpdate
import json
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash


app = Dash(__name__)
server = app.server

# Set up HTTP Basic Authentication
auth = HTTPBasicAuth()

# User data
users = {
    "fuji": generate_password_hash("Fuji2024!"),
    "admin": generate_password_hash("Alex2024!")
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




app.title = "Fuji Oil Production Planner"
du.configure_upload(app, "temp_uploads")

# Create necessary directories if they do not exist
os.makedirs("data", exist_ok=True)
os.makedirs("temp_uploads", exist_ok=True)


def initial_data_load(file_name):
    if os.path.exists(f'data/{file_name}'):
        df = pd.read_excel(f'data/{file_name}')
        return dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df.columns],
            style_table={'overflowX': 'auto', 'width': '100%'},
        )
    return html.Div(f"{file_name} data not available.")


def initial_matrix_load():
    if os.path.exists('data/transition_matrix.xlsx'):
        return "Loading matrix data..."
    return "Matrix data not available."


app.layout = html.Div([
    html.Div([
        html.Img(src="/assets/logo.jpeg", style={'height': '100px'}),
        html.H1("Production Planner v. 1.0", style={'margin': '0', 'padding': '10px'}),
    ], style={'display': 'flex', 'align-items': 'center'}),

    html.Div([
        dcc.Tabs([
            dcc.Tab(label='Processing', children=[
                html.Div(id='file-check-container'),
                html.Button("Process", id="process-button", style={'display': 'none'}),
                html.Pre(id='log-output')
            ]),
            dcc.Tab(label='Results', value='results', children=[
                html.Div(id='results-container')
            ], disabled=True, id='results-tab'),
            dcc.Tab(label='Inventory', children=[
                html.Div([
                    du.Upload(id='upload-inventory', max_file_size=1800, filetypes=['xlsx'], upload_id=uuid.uuid1()),
                    html.Div(id='upload-status-inventory'),
                    html.Button("Download Inventory Data", id="download-button-inventory"),
                    dcc.Download(id='download-link-inventory'),
                    html.Div(id='inventory-container', children=initial_data_load('inventory.xlsx'))
                ])
            ]),
            dcc.Tab(label='Sales', children=[
                html.Div([
                    du.Upload(id='upload-sales', max_file_size=1800, filetypes=['xlsx'], upload_id=uuid.uuid1()),
                    html.Div(id='upload-status-sales'),
                    html.Button("Download Sales Data", id="download-button-sales"),
                    dcc.Download(id='download-link-sales'),
                    html.Div(id='sales-container', children=initial_data_load('sales.xlsx'))
                ])
            ]),
            dcc.Tab(label='Recipes', children=[
                html.Div([
                    du.Upload(id='upload-recipes', max_file_size=1800, filetypes=['xlsx'], upload_id=uuid.uuid1()),
                    html.Div(id='upload-status-recipes'),
                    html.Button("Download Recipes Data", id="download-button-recipes"),
                    dcc.Download(id='download-link-recipes'),
                    html.Div(id='recipes-container', children=initial_data_load('recipes.xlsx'))
                ])
            ]),
            dcc.Tab(label='Tank Capacities', children=[
                html.Div([
                    du.Upload(id='upload-tanks', max_file_size=1800, filetypes=['xlsx'], upload_id=uuid.uuid1()),
                    html.Div(id='upload-status-tanks'),
                    html.Button("Download Tank Capacities Data", id="download-button-tanks"),
                    dcc.Download(id='download-link-tanks'),
                    html.Div(id='tanks-container', children=initial_data_load('tank.xlsx'))
                ])
            ]),
            dcc.Tab(label='Transition Matrix', children=[
                html.Div([
                    du.Upload(id='upload-data', max_file_size=1800, filetypes=['xlsx'], upload_id=uuid.uuid1()),
                    html.Div(id='upload-status'),
                    html.Button("Download Current Transition Rules Excel Data", id="download-button"),
                    dcc.Download(id='download-link'),
                    html.P(
                        "Legend: Numbers represent the number of steps needed to transition from one material to another (including wash, for example)."),
                    dcc.Interval(id='progress-interval', interval=1000, n_intervals=0),  # Checks every second
                    html.Div(id='progress-status'),
                    html.Div(id='matrix-container', children=initial_matrix_load())
                ])
            ]),
        ])
    ]),
])


@app.callback(
    [Output('upload-status', 'children'), Output('progress-interval', 'disabled')],
    [Input('upload-data', 'isCompleted'), Input('upload-data', 'fileNames')],
    [State('upload-data', 'upload_id')]
)
def handle_file_upload(is_completed, file_names, upload_id):
    if is_completed and file_names is not None:
        for file_name in file_names:
            source_path = os.path.join('temp_uploads', upload_id, file_name)
            target_path = 'data/sequenceorder_fixed_v2_transition.xlsx'
            os.replace(source_path, target_path)

            # Start the script and capture progress
            subprocess.Popen(['python', 'utils/transition_matrix_generator.py'])

            return "Uploaded and processing file: " + file_name, False  # False to enable the interval

    return "", True  # True to disable the interval


@app.callback(
    Output('progress-status', 'children'),
    Input('progress-interval', 'n_intervals')
)
def update_progress_status(n):
    if os.path.exists("progress.txt"):
        with open("progress.txt", "r") as file:
            progress = file.read()
            return f"Processing: {progress}%"
    return "Waiting for processing..."


@app.callback(
    Output('matrix-container', 'children'),
    [Input('progress-status', 'children')]
)
def update_matrix_container(progress_status):
    if '100%' in progress_status:
        df = pd.read_excel('data/transition_matrix.xlsx')
        return dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df.columns],
            # fixed_columns={'headers': True, 'data': 1},
            style_table={'overflowX': 'auto', 'width': '100%'},  # Set table width to 100%
            # style_cell={'minWidth': '250px', 'width': '250px', 'maxWidth': '250px'},  # Adjust cell width
        )
    return html.Div("Loading matrix data...")


@app.callback(
    Output('download-link', 'data'),
    Input('download-button', 'n_clicks'),
    prevent_initial_call=True
)
def download_matrix(n_clicks):
    if n_clicks:
        return dcc.send_file('data/sequenceorder_fixed_v2_transition.xlsx')

# Combined callback for updating upload status for Inventory, Sales, Recipes, and Tanks
@app.callback(
    [Output('upload-status-inventory', 'children'),
     Output('upload-status-sales', 'children'),
     Output('upload-status-recipes', 'children'),
     Output('upload-status-tanks', 'children')],
    [Input('upload-inventory', 'isCompleted'),
     Input('upload-sales', 'isCompleted'),
     Input('upload-recipes', 'isCompleted'),
     Input('upload-tanks', 'isCompleted')],
    [State('upload-inventory', 'fileNames'),
     State('upload-sales', 'fileNames'),
     State('upload-recipes', 'fileNames'),
     State('upload-tanks', 'fileNames'),
     State('upload-inventory', 'upload_id'),
     State('upload-sales', 'upload_id'),
     State('upload-recipes', 'upload_id'),
     State('upload-tanks', 'upload_id')]
)
def update_upload_status(inv_completed, sales_completed, recipes_completed, tanks_completed,
                         inv_filenames, sales_filenames, recipes_filenames, tanks_filenames,
                         inv_upload_id, sales_upload_id, recipes_upload_id, tanks_upload_id):
    ctx = dash.callback_context

    if not ctx.triggered:
        return "", "", "", ""

    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
    file_names, upload_id = None, None

    if triggered_id == 'upload-inventory':
        file_names, upload_id = inv_filenames, inv_upload_id
    elif triggered_id == 'upload-sales':
        file_names, upload_id = sales_filenames, sales_upload_id
    elif triggered_id == 'upload-recipes':
        file_names, upload_id = recipes_filenames, recipes_upload_id
    elif triggered_id == 'upload-tanks':
        file_names, upload_id = tanks_filenames, tanks_upload_id

    if file_names and upload_id:
        for file_name in file_names:
            source_path = os.path.join('temp_uploads', upload_id, file_name)
            target_path = f'data/{triggered_id.replace("upload-", "")}.xlsx'
            os.replace(source_path, target_path)
            message = "Uploaded file: " + file_name
            return (message if triggered_id == 'upload-inventory' else "",
                    message if triggered_id == 'upload-sales' else "",
                    message if triggered_id == 'upload-recipes' else "",
                    message if triggered_id == 'upload-tanks' else "")

    return "", "", "", ""

@app.callback(
    Output('inventory-container', 'children'),
    [Input('upload-status-inventory', 'children')]
)
def update_inventory_container(upload_status):
    if 'Uploaded file' in upload_status:
        df = pd.read_excel('data/inventory.xlsx')
        return dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df.columns],
            style_table={'overflowX': 'auto', 'width': '100%'},
        )
    elif upload_status == "":
        # This condition is true during initial load
        # Check if file exists and load it
        return initial_data_load('inventory.xlsx')
    else:
        # Handle other cases, such as errors in upload
        return html.Div("Unable to load inventory data.")

@app.callback(
    Output('download-link-inventory', 'data'),
    Input('download-button-inventory', 'n_clicks'),
    prevent_initial_call=True
)
def download_inventory(n_clicks):
    if n_clicks:
        return dcc.send_file('data/inventory.xlsx')


@app.callback(
    Output('sales-container', 'children'),
    [Input('upload-status-sales', 'children')]
)
def update_sales_container(upload_status):
    if 'Uploaded file' in upload_status:
        df = pd.read_excel('data/sales.xlsx')
        return dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df.columns],
            style_table={'overflowX': 'auto', 'width': '100%'},
        )
    elif upload_status == "":
        # This condition is true during initial load
        # Check if file exists and load it
        return initial_data_load('sales.xlsx')
    else:
        # Handle other cases, such as errors in upload
        return html.Div("Unable to load sales data.")

@app.callback(
    Output('download-link-sales', 'data'),
    Input('download-button-sales', 'n_clicks'),
    prevent_initial_call=True
)
def download_sales(n_clicks):
    if n_clicks:
        return dcc.send_file('data/sales.xlsx')


@app.callback(
    Output('recipes-container', 'children'),
    [Input('upload-status-recipes', 'children')]
)
def update_recipes_container(upload_status):
    if 'Uploaded file' in upload_status:
        df = pd.read_excel('data/recipes.xlsx')
        return dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df.columns],
            style_table={'overflowX': 'auto', 'width': '100%'},
        )
    elif upload_status == "":
        # This condition is true during initial load
        # Check if file exists and load it
        return initial_data_load('recipes.xlsx')
    else:
        # Handle other cases, such as errors in upload
        return html.Div("Unable to load recipes data.")

@app.callback(
    Output('download-link-recipes', 'data'),
    Input('download-button-recipes', 'n_clicks'),
    prevent_initial_call=True
)
def download_recipes(n_clicks):
    if n_clicks:
        return dcc.send_file('data/recipes.xlsx')


@app.callback(
    Output('tanks-container', 'children'),
    [Input('upload-status-tanks', 'children')]
)
def update_tanks_container(upload_status):
    if 'Uploaded file' in upload_status:
        df = pd.read_excel('data/tank.xlsx')
        return dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df.columns],
            style_table={'overflowX': 'auto', 'width': '100%'},
        )
    elif upload_status == "":
        # This condition is true during initial load
        # Check if file exists and load it
        return initial_data_load('tank.xlsx')
    else:
        # Handle other cases, such as errors in upload
        return html.Div("Unable to load tanks data.")


@app.callback(
    Output('download-link-tanks', 'data'),
    Input('download-button-tanks', 'n_clicks'),
    prevent_initial_call=True
)
def download_tanks(n_clicks):
    if n_clicks:
        return dcc.send_file('data/tank.xlsx')


required_files = ['sales.xlsx', 'inventory.xlsx', 'recipes.xlsx', 'tank.xlsx', 'sequenceorder_fixed_v2_transition.xlsx']


@app.callback(
    [Output('file-check-container', 'children'), Output('process-button', 'style')],
    [Input('upload-status-inventory', 'children'),
     Input('upload-status-sales', 'children'),
     Input('upload-status-recipes', 'children'),
     Input('upload-status-tanks', 'children')]
)
def check_files(*args):
    missing_files = [file for file in required_files if not os.path.exists(f'data/{file}')]
    if missing_files:
        return f"Please upload missing files in corresponding tabs: {', '.join(missing_files)}", {'display': 'none'}
    else:
        return "All data is available", {'display': 'block'}


@app.callback(
    [Output('log-output', 'children'), Output('results-tab', 'disabled')],
    [Input('process-button', 'n_clicks')],
    prevent_initial_call=True
)
def process_data(n_clicks):
    if n_clicks:
        process = subprocess.Popen(['python', 'utils/modelv14_batches.py'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output, _ = process.communicate()
        with open("logs.txt", "w") as log_file:
            log_file.write(output.decode())
        return output.decode(), False


@app.callback(
    Output('results-container', 'children'),
    [Input('process-button', 'n_clicks')],
    prevent_initial_call=True
)
def update_results_tab(n_clicks):
    if n_clicks:
        # Replace this with more complex logic to display actual results
        json_file_path = 'data/output.json'

        # Check if the file exists
        if os.path.exists(json_file_path):
            with open(json_file_path, 'r') as file:
                data = json.load(file)

            # Process the data
            processed_data = process_data(data)

            # Create a DataFrame and return as a DataTable
            df = pd.DataFrame(processed_data)
            return dash_table.DataTable(
                data=df.to_dict('records'),
                columns=[{'name': i, 'id': i} for i in df.columns],
                style_table={'overflowX': 'auto', 'width': '100%'},
            )
        else:
            return "JSON file not found. Please check the 'data' directory."

        # return html.Div("Processing complete. Displaying results here.")


def process_data(data):
    processed = []
    batch_number = 1  # Initialize batch number

    for entry in data:
        # Extract relevant information
        orders = entry.get("orders", "")
        final_material = entry.get("final_material", "")
        inputs = entry.get("input", [])
        output = entry.get("output", {})
        outputs_info = output.get("to_tanks_info", [])
        error_message = output.get("message", "")  # Get the error message if present

        if not inputs:
            # Handle case where input is empty
            processed.append({
                "Batch Number": batch_number,
                "Step": "No input",
                "Orders": orders,
                "Message": "Materials needed for the production are not located in tanks represented in tank list",
                "Material": "",
                "From Tank": "",
                "To Tank": "",
                "Quantity": ""
            })

        # Process input materials
        for input_material in inputs:
            processed.append({
                "Batch Number": batch_number,
                "Step": "1 - Putting to DEO",
                "Orders": orders,
                "Material": input_material["material"],
                "From Tank": input_material["from_tank"],
                "To Tank": input_material["to_tank"],
                "Quantity": input_material["quantity"]
            })

        # Handle output error message
        if error_message:
            processed.append({
                "Batch Number": batch_number,
                "Step": "Output error",
                "Orders": orders,
                "Message": error_message,
                "Material": final_material,
                "From Tank": output.get("from_tank", ""),
                "To Tank": "",
                "Quantity": ""
            })
        else:
            # Process output materials
            for output_material in outputs_info:
                processed.append({
                    "Batch Number": batch_number,
                    "Step": "2 - Unloading from DEO",
                    "Orders": orders,
                    "Material": final_material,
                    "From Tank": output.get("from_tank", ""),
                    "To Tank": output_material["to_tank"],
                    "Quantity": output_material["quantity"]
                })

        batch_number += 1  # Increment batch number for next entry

    return processed

if __name__ == '__main__':
    app.run_server(debug=True)
