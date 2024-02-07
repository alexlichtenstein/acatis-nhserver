import pandas as pd
import pyodbc

# Dummy function to read data from an Excel file and write to the company_data table
def update_company_data_dummy(date):
    with open('output_log.txt', 'w') as f:
        print(f"Processing data for date: {date}")
        # Define the connection string
        cnxn_string = f'Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:aogency-acatis.database.windows.net,1433;Database=acatis_msci;Uid=aogency;Pwd=Acatis2023!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

        # Establish a connection to the database
        cnxn = pyodbc.connect(cnxn_string)
        cursor = cnxn.cursor()

        # Read data from the Excel file
        data = pd.read_excel("input_data.xlsx", dtype=str)
        data['DataDate'] = pd.to_datetime(date)  # Add the date to the DataFrame

        # SQL query to find company ID based on ISSUER_ISIN
        company_id_query = 'SELECT CompanyID FROM company WHERE ISSUER_ISIN = ?'

        # SQL query to insert data into the company_data table
        insert_query = '''
        INSERT INTO company_data (
        CompanyID,
        DataDate,
        ESG_CONTROVERSIES_COVERED,
        FUR_PRODUCER,
        NON_PHARMACEUTICAL_TESTER,
        ANI_FACTORY_FARM,
        ANI_TEST_POLICY,
        PRED_LEND_MAX_REV,
        AE_DIST_MAX_REV_PCT,
        AE_PROD_MAX_REV_PCT,
        CONV_OIL_GAS_MAX_REV_PCT,
        PROD_CIV_ARMS_AUTO,
        FIREARM_PROD_MAX_REV_PCT,
        FIREARM_RET_MAX_REV_PCT,
        WEAP_MAX_REV_PCT,
        CWEAP_TIE,
        MET_COAL_MAX_REV_PCT,
        THERMAL_COAL_MAX_REV_PCT,
        GENERAT_THERMAL_COAL_PCT,
        TOB_DIST_MAX_REV_PCT,
        TOB_SUPP_MAX_REV_PCT,
        UNGC_COMPLIANCE,
        ENVIRONMENT_LAND_FLAG,
        ENVIRONMENT_CLIMATE_FLAG,
        ENVIRONMENT_TOXIC_FLAG,
        LABOR_COMPLIANCE_CORE,
        IVA_COMPANY_RATING,
        IVA_RATING_TREND,
        CARBON_EMISSIONS_SCOPE_1,
        CARBON_EMISSIONS_SCOPE_2,
        CARBON_EMISSIONS_SCOPE_3_TOTAL,
        ACTIVE_FF_SECTOR_EXPOSURE,
        OVERALL_FLAG,
        OPS_PROT_BIODIV_CONTROVS,
        ENERGY_CONSUMP_INTEN_EUR,
        CONTRO_WEAP_CBLMBW_ANYTIE,
        FEMALE_DIRECTORS_PCT,
        GENDER_PAY_GAP_RATIO,
        HAZARD_WASTE_METRIC_TON,
        LABOR_DDIL_POL_ILO,
        MECH_UN_GLOBAL_COMPACT,
        OPS_PROT_BIODIV_AREAS,
        PCT_TOTL_ERGY_CONSUMP_NONRENEW,
        PCT_TOTL_ERGY_PRODUCT_NONRENEW,
        PCT_NONRENEW_CONSUMP_PROD,
        CARBON_EMISSIONS_SCOPE123,
        CARBON_EMISSIONS_EVIC_EUR_SCOPE123_INTEN,
        CARBON_EMISSIONS_SALES_EUR_SCOPE123_INTEN,
        TOTL_ERGY_CONSUMP_GWH,
        TOTL_ERGY_CONSUMP_NONRENEW_GWH,
        TOTL_ERGY_CONSUMP_RENEW_GWH,
        WATER_EM_EFF_METRIC_TONS,
        EST_EU_TAXONOMY_MAX_REV,
        EU_TAXONOMY_ELIGIBLE_MAX_REV
    )
    VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?
    );  
        '''

        # Process each row in the DataFrame
        for index, row in data.iterrows():
            # Find the company ID based on ISSUER_ISIN
            cursor.execute(company_id_query, row['ISSUER_ISIN'])
            isin = row['ISSUER_ISIN']
            result = cursor.fetchone()
            if result:
                company_id = result[0]
                # Convert values from index 7 onwards to strings
                values_as_strings = list(map(str, row.values[7:-1]))
                # print(values_as_strings)
                # Construct the list of values to insert, excluding specific columns
                insert_values = [company_id, row['DataDate']] + values_as_strings
                yield f'Importing values for {date} and ISIN {isin}'
                percentage = 100 * index / len(data)
                formatted_percentage = f'Percentage {percentage:.2f}%'
                yield formatted_percentage
                # print(insert_values)
                # Insert the data into the company_data table
                cursor.execute(insert_query, *insert_values)  # Use * to unpack the list

        # Commit the transactions
        cnxn.commit()

        # Close the cursor and connection
        cursor.close()
        cnxn.close()

    # Output a confirmation message
    return "Data inserted successfully into the 'company_data' table."

# import datetime
# # Define the filename
# filename = 'selected_date.txt'
#
# # Read the date from the file
# try:
#     with open(filename, 'r') as file:
#         selected_date_str = file.read().strip()
#         selected_date = datetime.datetime.strptime(selected_date_str, '%Y-%m-%d')  # Adjust the format as needed
#         update_company_data_dummy(selected_date)
# except FileNotFoundError:
#     print(f"File '{filename}' not found or date not available.")
#     selected_date = None