#%%
#importing and reading data
import pandas as pd
import RENAME_MAPPING as rm
import mysql.connector as sql
from sqlalchemy import create_engine, text, MetaData, Table, select, func, Column, Integer
from sqlalchemy.orm import sessionmaker
import time
import os


#%%
#-----------------------WELL FUNCTIONS-------------------------
def process_well_data(df, file_path, rename_cols, save):
    

    rm.pai_cols_well 
    if rename_cols:
        
        rm.mapper_well 
        df.rename(columns=rm.mapper_well, inplace=True)

    
        df_non = df.loc[:, ~df.columns.isin(rm.pai_cols_well)]
        df = df.loc[:, df.columns.isin(rm.pai_cols_well)]
        print(f'columns dropped: {list(df_non)}')
    if save:
        df.to_csv(file_path,index = False)


rm.relevant_columns_well

def summarize_well_data(df, relevant_columns):
    
    unique_count = df['wellId'].nunique()
    total_rows = df.shape[0]
    duplicates = df[df.duplicated('wellId', keep=False)]
    duplicate_values = duplicates['wellId'].unique()

    
    non_nulls = []
    nulls = []
    relevant_cols_exist = []
    missing_cols = []

    for col in relevant_columns:
        if col in df.columns:
            count = df[col].count()
            non_nulls.append(count)
            null_count = total_rows - count
            nulls.append(null_count)
            relevant_cols_exist.append(col)
        else:
            missing_cols.append(col)
    
    
    data = {
        'column': relevant_cols_exist,
        'non_nulls': non_nulls,
        'nulls': nulls
    }
    df_dq = pd.DataFrame(data)


    print(f"Total Rows: {total_rows}")
    print(f'Unique wells: {unique_count}')
    print(f"Total duplicate well ids: {int(duplicates.shape[0]/2)}")
    print(f"Duplicate well ids: {', '.join(map(str, duplicate_values))}")
    print(df_dq.to_string(index=False))
    
    if missing_cols:
        print(f'Columns that do not exist: {", ".join(missing_cols)}')

#%%
#---------------------------MONTHLY PRODUCTION FUNCTIONS------------------------------
def process_monProd_data(df, file_path, rename_cols, save):
    rm.pai_cols_monthly_prod 
    if rename_cols:
        rm.mapper_monthly_prod
        df.rename(columns=rm.mapper_monthly_prod, inplace=True)

        df_non = df.loc[:, ~df.columns.isin(rm.pai_cols_monthly_prod)]
        df = df.loc[:, df.columns.isin(rm.pai_cols_monthly_prod)]
        print(f'columns dropped: {list(df_non)}')
    if save:
        df.to_csv(file_path,index = False) 


def process_production_data(df,relevant_columns):
    
    duplicate_counts = df.groupby(['wellId', 'prodDate']).size().reset_index(name='count')
    duplicates = duplicate_counts[duplicate_counts['count'] > 1]
    output_dup_count = duplicates.groupby('wellId')['count'].sum()
    total_rows = df.shape[0]
   
    unique_count = df['wellId'].nunique()
    total_rows = df.shape[0]

    non_nulls = []
    nulls = []
    relevant_cols_exist = []
    missing_cols = []


    print(f"Total Rows: {total_rows}")
    print(f'Unique wells: {unique_count}')

    for col in relevant_columns:
        if col in df.columns:
            count = df[col].count()
            non_nulls.append(count)
            null_count = total_rows - count
            nulls.append(null_count)
            relevant_cols_exist.append(col)
        else:
            missing_cols.append(col)
    
    
    data = {
        'column': relevant_cols_exist,
        'non_nulls': non_nulls,
        'nulls': nulls
    }
    
    df_dq = pd.DataFrame(data)

    
    print(df_dq.to_string(index=False))
    print(f' Duplicated wellId & prodDate combo : {output_dup_count}')


def process_cumulative_data(df, file_path, save):
    
    df_dict = {name: group.sort_values(by='prodDate', ascending=True) for name, group in df.groupby('wellId')}
    
    
    for name, df_group in df_dict.items():
        df_group['oilCum_bbl'] = df_group['oilVol_bbl'].cumsum()
        df_group['gasCum_Mcf'] = df_group['gasVol_Mcf'].cumsum()
        df_group['waterCum_bbl'] = df_group['waterVol_bbl'].cumsum()
        df_dict[name] = df_group
    
    
    combined_df = pd.concat(df_dict.values(), ignore_index=True)
    combined_df.sort_values(by=['wellId', 'prodDate'], ascending=True, inplace=True)
    
    if save:
        combined_df.to_csv(file_path, index=False)
    return 'saved file with cums'

def date_checker(df, date_columns):
    
    rows = min(len(df), 100)

    for column in date_columns:
        for index in range(rows):
            value = df.at[index, column] 
            if pd.isna(value):
                continue
            try:
                
                df.at[index, column] = pd.to_datetime(value).strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:

                print(f"Error converting {column} at row {index} with value '{value}'")
                continue
#%%    
#----------------GRID ATTRIBUTES AND STRUCTURES FUNCTIONS----------------------------------    
def check_interval_presence_and_count(df_source, df_target, interval_column):

    
    source_intervals_unique = df_source[interval_column].unique()
    
    
    counts_present = df_target[interval_column][df_target[interval_column].isin(source_intervals_unique)].value_counts()
    
    
    counts_not_present = df_target[interval_column][~df_target[interval_column].isin(source_intervals_unique)].value_counts()
    
    
    return {
        'Intervals present in Target': counts_present,
        'Intervals not present in Target': counts_not_present
    } 

def process_attribute_data(df, file_path, rename_cols, save):
    rm.pai_cols_monthly_prod 
    if rename_cols:
        rm.mapper_monthly_prod
        df.rename(columns=rm.mapper_monthly_prod, inplace=True)

        df_non = df.loc[:, ~df.columns.isin(rm.pai_cols_monthly_prod)]
        df = df.loc[:, df.columns.isin(rm.pai_cols_monthly_prod)]
        print(f'columns dropped: {list(df_non)}')
    if save:
        df.to_csv(file_path,index = False) 

def process_structure_data(df, file_path, rename_cols, save):
    rm.pai_cols_monthly_prod 
    if rename_cols:
        rm.mapper_monthly_prod
        df.rename(columns=rm.mapper_monthly_prod, inplace=True)

        df_non = df.loc[:, ~df.columns.isin(rm.pai_cols_monthly_prod)]
        df = df.loc[:, df.columns.isin(rm.pai_cols_monthly_prod)]
        print(f'columns dropped: {list(df_non)}')
    if save:
        df.to_csv(file_path,index = False) 
#%%   
#-------------------- SURVEY FUNCTIONS----------------------------------------------
def process_survey_data(df, file_path, rename_cols, save):
    
    rm.pai_cols_directional_survey
    if rename_cols:
        
        rm.mapper_directional_survey 
        df.rename(columns=rm.mapper_directional_survey, inplace=True)

    
        df_non = df.loc[:, ~df.columns.isin(rm.pai_cols_directional_survey)]
        df = df.loc[:, df.columns.isin(rm.pai_cols_directional_survey)]
        print(f'columns dropped: {list(df_non)}')
    #sort values by wellId and md
    df.sort_values(by=['wellId', 'md_ft'], ascending=True, inplace=True)
    
    if save:
        df.to_csv(file_path,index = False)


rm.relevant_columns_survey

def summarize_survey_data(df, relevant_columns):
    
    unique_count = df['wellId'].nunique()
    total_rows = df.shape[0]
    duplicates = df[df.duplicated('wellId', keep=False)]
    duplicate_values = duplicates['wellId'].unique()

   
    non_nulls = []
    nulls = []
    relevant_cols_exist = []
    missing_cols = []

    for col in relevant_columns:
        if col in df.columns:
            count = df[col].count()
            non_nulls.append(count)
            null_count = total_rows - count
            nulls.append(null_count)
            relevant_cols_exist.append(col)
        else:
            missing_cols.append(col)
    
    
    data = {
        'column': relevant_cols_exist,
        'non_nulls': non_nulls,
        'nulls': nulls
    }
    df_dq = pd.DataFrame(data)

    print(f"Total Rows: {total_rows}")
    print(f'Unique wells: {unique_count}')
    print(f"Total duplicate well ids: {int(duplicates.shape[0]/2)}")
    print(df_dq.to_string(index=False))
    
    if missing_cols:
        print(f'Columns that do not exist: {", ".join(missing_cols)}')

#%%
#----------------------------WELL EXTRAS FUNCTION-----------------------------------
host = "pai-cloud-mysql-prod.cc9ampy7re8z.us-west-2.rds.amazonaws.com" #enter host url
database = 'mavresources_wabregional'
user = "paicloudadmin"
password = "pqImEr6ewv7unmGGu0hqfm7"
port = 3306
max_rows = 100000


#inputs

metadata = MetaData()
   

def process_database(database, user, password, host, table_name, new_columns, df, max_rows):
    try:
        
        connection_string = f'mysql+pymysql://{user}:{password}@{host}/{database}'
        
        engine = create_engine(connection_string)
        metadata = MetaData()

        
        with engine.connect() as conn:
            table = Table(table_name, metadata, autoload_with=engine)
            for col in new_columns:
                if col not in [col.name for col in table.columns]:
                    #print('Starting column:', col)
                    
                    if df[col].dtypes == object:
                        sql_type = 'VARCHAR(255)'
                    elif df[col].dtypes == 'float64':
                        sql_type = 'FLOAT'
                    elif df[col].dtypes == 'int64':
                        sql_type = 'INT'
                    query = f'ALTER TABLE {table_name} ADD COLUMN {col} {sql_type}'
                    conn.execute(text(query))
                    
                    metadata.clear()
                    metadata.reflect(bind=engine)

            
            print('Loading to server:', table_name, len(df))
            curr_index = 0
            while curr_index < len(df):
                end_index = curr_index + max_rows
                slice_df = df[curr_index:end_index]
                print('Sending to:', curr_index, end_index)
                slice_df.to_sql(table_name, engine, if_exists='append', index=False)
                curr_index = end_index
            print(table_name + ' Loaded')

    except Exception as e:
        print(f"----------------ERROR PROCESSING------ {database} --------- {e} ------------")

    
    
#%%
#---------------------WELL LOOOKUP FUNCTIONS------------------------------------------------
def process_lookup_data(df, file_path, rename_cols, save):
    
    rm.pai_cols_lookup 
    if rename_cols:
        
        rm.mapper_lookup 
        df.rename(columns=rm.mapper_lookup, inplace=True)

    
        df_non = df.loc[:, ~df.columns.isin(rm.pai_cols_lookup)]
        df = df.loc[:, df.columns.isin(rm.pai_cols_lookup)]
        print(f'columns dropped: {list(df_non)}')
    if save:
        df.to_csv(file_path,index = False)


rm.relevant_columns_lookup

def summarize_lookup_data(df, relevant_columns):
    
    unique_count = df['wellId'].nunique()
    total_rows = df.shape[0]
    duplicates = df[df.duplicated('wellId', keep=False)]
    duplicate_values = duplicates['wellId'].unique()

    
    non_nulls = []
    nulls = []
    relevant_cols_exist = []
    missing_cols = []

    for col in relevant_columns:
        if col in df.columns:
            count = df[col].count()
            non_nulls.append(count)
            null_count = total_rows - count
            nulls.append(null_count)
            relevant_cols_exist.append(col)
        else:
            missing_cols.append(col)
    
    
    data = {
        'column': relevant_cols_exist,
        'non_nulls': non_nulls,
        'nulls': nulls
    }
    df_dq = pd.DataFrame(data)

   

    print(f"Total Rows: {total_rows}")
    print(f'Unique wells: {unique_count}')
    print(f"Total duplicate well ids: {int(duplicates.shape[0]/2)}")
    print(f"Duplicate well ids: {', '.join(map(str, duplicate_values))}")
    print(df_dq.to_string(index=False))
    
    if missing_cols:
        print(f'Columns that do not exist: {", ".join(missing_cols)}')

#%%
#-----------------------------INVENTORY FUNCTIONS-----------------------------------------
def process_inventory_data(df, file_path, rename_cols, save):
    
    rm.pai_cols_inventory 
    if rename_cols:
        
        rm.mapper_inventory 
        df.rename(columns=rm.mapper_inventory, inplace=True)

    
        df_non = df.loc[:, ~df.columns.isin(rm.pai_cols_inventory)]
        df = df.loc[:, df.columns.isin(rm.pai_cols_inventory)]
        print(f'columns dropped: {list(df_non)}')
    if save:
        df.to_csv(file_path,index = False)

#----------------------------REFERENCE CHECK----------------------------
def find_unique_ids(df_well, df_monthly, df_lookup, df_survey):
    

    # Extract unique well IDs into lists
    list0 = df_lookup['wellId'].unique().tolist()
    list1 = df_well['wellId'].unique().tolist()
    list2 = df_monthly['wellId'].unique().tolist()
    list3 = df_lookup['prodWellId'].unique().tolist()
    list4 = df_lookup['surveyWellId'].unique().tolist()
    list5 = df_survey['wellId'].unique().tolist()
    
    # Find unique elements
    unique_in_list0 = [item for item in list1 if item not in list0]
    unique_in_list1 = [item for item in list2 if item not in list1]
    unique_in_list2 = [item for item in list2 if item not in list3]
    unique_in_list3 = [item for item in list5 if item not in list4]
    unique_in_list4 = [item for item in list5 if item not in list1]

    # Print the results
    print("WellID in Well Lookup but not in Well Header:",len(unique_in_list0), unique_in_list0)
    print("WellID in MonthlyProduction but not in Well Header:",len(unique_in_list1), "(skipped printing wellIds)")
    #print("WellID in MonthlyProduction but not in Well Header:",len(unique_in_list1), unique_in_list1)
    print("WellID in Monthly Production but not in Well Lookup:", len(unique_in_list2), unique_in_list2)
    print("WellID in Survey but not in Well Lookup:", len(unique_in_list3), unique_in_list3)
    print("WellID in Survey but not in Header:", len(unique_in_list4), unique_in_list4)