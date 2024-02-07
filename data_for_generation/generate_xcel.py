import pandas as pd
import glob
import openpyxl
from openpyxl.utils import get_column_letter

# Step 1: Read all CSV files in the current directory
all_csv_files = glob.glob("*.csv")

# Check if there are any CSV files
if not all_csv_files:
    raise FileNotFoundError("No CSV files found in the current directory.")

# Custom aggregation function
def format_output(series):
    values = series.tolist()
    if len(values) == 1:
        return values[0]  # Return the single value directly
    else:
        # Join the values with comma and return as a string
        return ', '.join(map(str, values))

# Try reading each file, handle potential errors
dfs = []
for file in all_csv_files:
    try:
        dfs.append(pd.read_csv(file))
    except Exception as e:
        print(f"Error reading {file}: {e}")

# Step 2: Stack the DataFrames together
# stacked_df = pd.concat(dfs)
stacked_df = pd.concat(dfs, axis=0)

# Step 3: Remove duplicates
stacked_df.drop_duplicates(inplace=True)

# # Step 4: Group by ISSUER_ISIN and stack other columns as arrays
# grouped_df = stacked_df.groupby("ISSUER_ISIN").agg({
#     'CLIENT_IDENTIFIER': list,
#     'ISSUER_NAME': list,
#     'ISSUERID': list,
#     'CIK_NUM': list,
#     'ISSUER_TICKER': list,
#     'AE_DIST_MAX_REV_PCT': list,
#     'ESG_CONTROVERSIES_COVERED': list,
#     'CARBON_EMISSIONS_SCOPE_3_TOTAL': list,
#     'GENERAT_THERMAL_COAL_PCT': list,
#     'FIREARM_PROD_MAX_REV_PCT': list,
#     'IVA_COMPANY_RATING': list,
#     'EU_TAXONOMY_ELIGIBLE_MAX_REV': list,
#     'ENVIRONMENT_TOXIC_FLAG': list,
#     'AE_PROD_MAX_REV_PCT': list,
#     'CONTRO_WEAP_CBLMBW_ANYTIE': list,
#     'FIREARM_RET_MAX_REV_PCT': list,
#     'ENERGY_CONSUMP_INTEN_EUR': list,
#     'GENDER_PAY_GAP_RATIO': list,
#     'MET_COAL_MAX_REV_PCT': list,
#     'PCT_NONRENEW_CONSUMP_PROD': list,
#     'PROD_CIV_ARMS_AUTO': list,
#     'ENVIRONMENT_LAND_FLAG': list,
#     'PCT_TOTL_ERGY_CONSUMP_NONRENEW': list,
#     'CARBON_EMISSIONS_SCOPE_1': list,
#     'CARBON_EMISSIONS_SCOPE_2': list,
#     'TOB_DIST_MAX_REV_PCT': list,
#     'PCT_TOTL_ERGY_PRODUCT_NONRENEW': list,
#     'CARBON_EMISSIONS_SALES_EUR_SCOPE123_INTEN': list,
#     'LABOR_COMPLIANCE_CORE': list,
#     'FEMALE_DIRECTORS_PCT': list,
#     'ANI_TEST_POLICY': list,
#     'OPS_PROT_BIODIV_AREAS': list,
#     'IVA_RATING_TREND': list,
#     'LABOR_DDIL_POL_ILO': list,
#     'ACTIVE_FF_SECTOR_EXPOSURE': list,
#     'ENVIRONMENT_CLIMATE_FLAG': list,
#     'WATER_EM_EFF_METRIC_TONS': list,
#     'OVERALL_FLAG': list,
#     'MECH_UN_GLOBAL_COMPACT': list,
#     'CARBON_EMISSIONS_EVIC_EUR_SCOPE123_INTEN': list,
#     'THERMAL_COAL_MAX_REV_PCT': list,
#     'TOTL_ERGY_CONSUMP_RENEW_GWH': list,
#     'OPS_PROT_BIODIV_CONTROVS': list,
#     'ANI_FACTORY_FARM': list,
#     'UNGC_COMPLIANCE': list,
#     'EST_EU_TAXONOMY_MAX_REV': list,
#     'PRED_LEND_MAX_REV': list,
#     'TOTL_ERGY_CONSUMP_NONRENEW_GWH': list,
#     'TOTL_ERGY_CONSUMP_GWH': list,
#     'CWEAP_TIE': list,
#     'FUR_PRODUCER': list,
#     'NON_PHARMCEUTICAL_TESTER': list,
#     'TOB_SUPP_MAX_REV_PCT': list,
#     'CONV_OIL_GAS_MAX_REV_PCT': list,
#     'HAZARD_WASTE_METRIC_TON': list,
#     'WEAP_MAX_REV_PCT': list,
#     'CARBON_EMISSIONS_SCOPE123': list
# })

grouped_df = stacked_df.groupby("ISSUER_ISIN").agg({
    column: lambda x: format_output(x) for column in stacked_df.columns if column != 'ISSUER_ISIN'
})

# Step 5: Write the output to an Excel file with specified column width
output_file = 'output2.xlsx'
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    grouped_df.to_excel(writer, sheet_name='Sheet1', index=True)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

    for i, column in enumerate(grouped_df.columns, 1):  # Start from 1 since Excel columns are 1-indexed
        max_length = max(grouped_df[column].astype(str).apply(len).max(), len(column))
        adjusted_width = (max_length + 2)  # Adding a small buffer
        worksheet.column_dimensions[get_column_letter(i)].width = adjusted_width