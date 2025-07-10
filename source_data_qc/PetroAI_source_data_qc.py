#%%
#---------------IMPORTS------------------
import os
import pandas as pd
import mysql.connector
#Petro scripts:
import RENAME_MAPPING as rm
import QC_functions as qf

#import mysql.connector as sql
#from sqlalchemy import create_engine, text, MetaData, Table, select, func, Column, Integer
#from sqlalchemy.orm import sessionmaker
#import time

#%%
#---------------OPTIONS------------------

#Max rows for printing
#Only use this if using Well, MonProd, Survey, and Lookup
pd.set_option('display.max_rows', 20000)

#Compares well ids across well data
compare = True

#only deletes if rows are exact duplicates for key cols
#currently only on monthly prod
# count rows beofre and after dropping duplicaes
drop_duplicates = False

#optionally save clean output files
save_cleaned_files = False

#Read from database or files
#data_source = 'files'
data_source = 'database'

#%%
# -------------- TABLE SELECTION ---------------
#Available tables
'''
    'Well',
    'WellLookup',
    'MonthlyProduction',
    'WellDirectionalSurveyPoint',
    'WellExtra',
    'GridAttributeHeader',
    'GridAttributeData',
    'GridStructureHeader',
    'GridStructureData',
    'InventoryWells',
    'MicroseismicEvent',
    'StressOrientationMeasure',
    'ReservoirStressWellLogRecord'
'''

tables_to_check = [
    'Well',
    'WellLookup',
    'MonthlyProduction',
    'WellDirectionalSurveyPoint',
    'GridAttributeHeader',
    'GridAttributeData',
    'GridStructureHeader',
    'GridStructureData'
    ]

print(f'Tables to Check: {tables_to_check}')

#%% DB Connection
host = 'pai-cloud-mysql-prod.cc9ampy7re8z.us-west-2.rds.amazonaws.com'
user = 'xx'
password = 'xx'
port = 3306

database = 'xx'

db_config = {
    "host": host,
    "user": user,
    "password": password,
    "port" : port,
    "database": database
}

#%% File Paths
srce_path = 'C:/Users/XX/'
save_path = 'C:/Users/XX/'

path_dict = {
    'Well': srce_path + 'Well.csv',
    'WellExtra': srce_path + 'WellExtra.csv',
    'WellLookup': srce_path + 'WellLookup.csv',
    'MonthlyProduction': srce_path + 'MonthlyProduction.csv',
    'WellDirectionalSurveyPoint': srce_path + 'WellDirectionalSurveyPoint.csv',
    'GridStructureData': srce_path + 'GridStructureData.csv',
    'GridAttributeData': srce_path + 'GridAttributeData.csv',
    'GridAttributeHeader': srce_path + 'GridAttributeHeader.csv',
    'GridStructureHeader': srce_path + 'GridStructureHeader.csv',
    'InventoryWells': srce_path + 'InventoryWells.csv'
}

#%%
# ------------- READING DATA INTO DF ------------
df_check = {}
df_names =[]

if data_source == 'files':
    for table_name in tables_to_check:
        file_path = path_dict[table_name]
        if file_path:
            extension = os.path.splitext(file_path)[1] 
            if extension == '.csv':
                df_check[table_name] = pd.read_csv(file_path)
                print(f'{table_name} read as CSV')
            elif extension == '.xlsx':
                df_check[table_name] = pd.read_excel(file_path)
                print(f'{table_name} read as Excel')
            elif extension == '.tsv':
                #make sure delimiter slash is backslash 
                df_check[table_name] = pd.read_csv(file_path, delimiter = '\t')
                print(f'{table_name} read as TSV')
            else:
                print(f'Unsupported file format {table_name}')
                continue

            print(f'{table_name} length: {len(df_check[table_name])}')
            globals()[f'df_{table_name}'] = df_check[table_name]
            df_names.append(f'df_{table_name}')
        else:
            print(f"No file path provided for {table_name}")
elif data_source == 'database':
    # Connect to MySQL
    conn = mysql.connector.connect(**db_config)
    for table_name in tables_to_check:
        query = f"SELECT * FROM {table_name}"
        # Read data into a DataFrame
        df_check[table_name] = pd.read_sql(query, conn)
        print(f'{table_name} read from database')
        print(f'{table_name} length: {len(df_check[table_name])}')
        globals()[f'df_{table_name}'] = df_check[table_name]
        df_names.append(f'df_{table_name}')
    # Close the connection
    conn.close()
else:
    print('Set the data_source parameter')
    
# %%
# ------------ QC Report ---------------
if data_source == 'database':
    print(f'Database: {database}')

print('')
print('-------------------Summary--------------')
print('Unique Well ID Counts by Table:')

data = []

for df_name in df_names:
    if df_name == 'df_Well':        
        unique_count = df_Well['wellId'].nunique()
        data.append(('Well', unique_count))
    if df_name == 'df_WellExtra':
        unique_count = df_WellExtra['wellId'].nunique()
        data.append(('Well Extra', unique_count))
    if df_name == 'df_MonthlyProduction':
        unique_count = df_MonthlyProduction['wellId'].nunique()
        data.append(('Monthly Production', unique_count))
    if df_name == 'df_WellDirectionalSurveyPoint':
        unique_count = df_WellDirectionalSurveyPoint['wellId'].nunique()
        data.append(('Survey', unique_count))
    if df_name == 'df_WellLookup':
        unique_count = df_WellLookup['wellId'].nunique()
        data.append(('Well Lookup', unique_count))

df_summary = pd.DataFrame(data, columns=['Table', 'Count'])
df_summary['Count'] = df_summary['Count'].apply(lambda x: "{:,}".format(x))
print(df_summary)

for df_name in df_names:
    
    if df_name == 'df_Well':
        file_path = save_path + 'Well.csv'
        date_columns = ['completionDate']
        print('----------------------WELL HEADER INFO-----------------------------')    
        df_well = globals()[df_name]
        print(f'Columns for df_well : {list(df_well)}')
        
        qf.process_well_data(df_well, file_path, rename_cols=True, save = save_cleaned_files)            

        qf.summarize_well_data(df_well, rm.relevant_columns_well)

        qf.date_checker(df_well,date_columns)        
        print('')

    if df_name == 'df_WellExtra':
        file_path = save_path + 'WellExtra.csv'
        print('----------------------WELL EXTRA INFO-----------------------------')    
        df_well_extra = globals()[df_name]
        print(f'Columns for df_well_extra : {list(df_well_extra)}')
        print('')

    if df_name == 'df_MonthlyProduction':
        date_columns = ['prodDate']
        columns_to_check = ['oilCum_bbl', 'gasCum_Mcf', 'waterCum_bbl']
        file_path = save_path + 'MonthlyProduction.csv'
        print('------------------MONTHLY PRODUCTION INFO-------------------')
        df_monProd = globals()[df_name]
        print(f'Drop duplicates: {drop_duplicates}')
        if drop_duplicates:
            df_monProd = df_monProd.drop_duplicates(
                subset = [
                'wellId',
                'prodDate',
                'oilRate_bblPerDay',
                'oilVol_bbl',
                'oilCum_bbl',
                'gasRate_McfPerDay',
                'gasVol_Mcf',
                'gasCum_Mcf',
                'waterRate_bblPerDay',
                'waterVol_bbl',
                'waterCum_bbl'
            ])

        print(f'Columns for df_monProd : {list(df_monProd)}')
        
        qf.process_monProd_data(df_monProd, file_path, rename_cols=True, save = save_cleaned_files)
        qf.process_production_data(df_monProd,rm.relevant_columns_monProd)


        if df_monProd[columns_to_check].isna().all().all():
            qf.process_cumulative_data
            print('Cums were inserted manually')
        else:
            print("Monthly Production is missing no cums")

        qf.date_checker(df_monProd,date_columns)

        print('')

    if df_name == 'df_WellDirectionalSurveyPoint':
        df_survey = globals()[df_name]
        file_path = save_path + 'WellDirectionalSurveyPoint.csv'
        print('------------------DIRECTIONAL SURVEY INFO-----------------')
        print(f'Columns for df_survey : {list(df_survey)}')

        qf.process_survey_data(df_survey, file_path, rename_cols=True, save = save_cleaned_files)

        qf.summarize_survey_data(df_survey, rm.relevant_columns_survey)

        print('')
    

    if df_name == 'df_WellLookup':
        df_lookup = globals()[df_name]
        file_path = save_path + 'WellLookup.csv'
        print('-------------------WELL LOOKUP INFO--------------')
        print(f'Columns for df_lookup : {list(df_lookup)}')

        qf.process_lookup_data(df_lookup, file_path, rename_cols=True, save = save_cleaned_files)

        qf.summarize_lookup_data(df_lookup,rm.relevant_columns_lookup)

        print('')

    if df_name == 'df_GridStructureData':
        print('---------------------STRUCTURE DATA INFO---------------------')
        df_GridStructureData = globals()[df_name]
        file_path = save_path + 'GridStructureData.csv'
        if  save_cleaned_files:
            df_GridStructureData.to_csv(file_path,index=False)
        df_GridStructureHeader = globals()['df_GridStructureHeader']
        df_GridAttributeHeader = globals()['df_GridAttributeHeader']

        result = qf.check_interval_presence_and_count(df_GridStructureHeader, df_GridStructureData,'interval')
        print('Structure header intervals in Structure Data:')
        print(result['Intervals present in Target'])
        print('Structure header intervals NOT in Structure Data:')
        print(result['Intervals not present in Target'])
        result = qf.check_interval_presence_and_count(df_GridStructureHeader, df_GridAttributeHeader,'interval')
        print('Structure header intervals in Attribute Header:')
        print(result['Intervals present in Target'])
        print('Structure header intervals NOT in Attribute Header:')
        print(result['Intervals not present in Target'])
        print('')


    if df_name == 'df_GridAttributeData':
        print('----------------GRID DATA INFO------------------')
        df_GridAttributeData = globals()[df_name]
        file_path = save_path + 'GridAttributeData.csv'
        if  save_cleaned_files:
            df_GridAttributeData.to_csv(file_path,index=False)
        df_GridAttributeHeader = globals()['df_GridAttributeHeader']

        result = qf.check_interval_presence_and_count(df_GridAttributeHeader, df_GridAttributeData,'name')
        print('Attribute header intervals in Attribute Data:')
        print(result['Intervals present in Target'])
        print('Attribute header intervals NOT in Attribute Data:')
        print(result['Intervals not present in Target'])
    
    if df_name == 'df_GridAttributeHeader':
        file_path = save_path + 'GridAttributeHeader.csv'
        df_attr_header = globals()[df_name]
        print(f'Columns for df_attr_header : {list(df_attr_header)}')
        if  save_cleaned_files:
            df_attr_header.to_csv(file_path, index = False)

    if df_name == 'df_GridStructureHeader':
        file_path = save_path + 'GridStructureHeader.csv'
        df_struc_header = globals()[df_name]
        print(f'Columns for df_struc_header : {list(df_struc_header)}')
        if  save_cleaned_files:
            df_struc_header.to_csv(file_path, index = False)


    if df_name == 'df_InventoryWells':
        df_inventory = globals()[df_name]
        print(f'Columns for df_inventory : {list(df_inventory)}')
        file_path = save_path + 'InventoryWells.csv'
        print('-------------------INVENTORY WELLS INFO--------------')
        qf.process_inventory_data(df_inventory, file_path,rename_cols=True, save = save_cleaned_files)
    
if compare:
    df_well = globals()['df_Well']
    df_monProd = globals()['df_MonthlyProduction']
    df_lookup = globals()['df_WellLookup']
    df_survey = globals()['df_WellDirectionalSurveyPoint']
    print('-------------------WELL ID CHECK--------------')
    qf.find_unique_ids(df_well, df_monProd, df_lookup, df_survey)
