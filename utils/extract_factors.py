import json
import pandas as pd

# Function to extract factor names and descriptions from a JSON file and write them to an Excel file.
# Inputs:
#   json_file_path (str): Path to the JSON file containing factor data.
#   excel_file_path (str): Path to the Excel file where the data will be written.
def extract_factors_to_excel(json_file_path, excel_file_path):
    # Load JSON data from the file
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Extract factor names and descriptions
    factors = data["result"]["factors"]
    factor_names = [factor["factor_name"] for factor in factors]
    descriptions = [factor["description"] for factor in factors]

    # Create a DataFrame
    df = pd.DataFrame({
        "Factor Name": factor_names,
        "Description": descriptions
    })

    # Write the DataFrame to an Excel file
    df.to_excel(excel_file_path, index=False)

# Example usage
# json_file_path = 'response_1702995220179.json'
# excel_file_path = 'all.xlsx'
# extract_factors_to_excel(json_file_path, excel_file_path)