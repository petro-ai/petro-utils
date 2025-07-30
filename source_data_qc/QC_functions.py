#%%
#importing and reading data
import pandas as pd
from . import RENAME_MAPPING as rm
import logging
import inspect

_log = logging.getLogger(__name__)

__version__ = "2.0.0"

class Utilities:
    @staticmethod
    def _truncate_list_for_log(items, max_items=100):
        display_items = items[:max_items]
        suffix = "..." if len(items) > max_items else ""
        return f"{display_items}{suffix}"
    
    @staticmethod
    def date_checker(df, date_columns):
        rows = min(len(df), 100)
        for column in date_columns:
            if column not in df.columns:
                continue
            null_rows = df[df[column].isnull()]
            if not null_rows.empty:
                truncated_ids = Utilities._truncate_list_for_log(null_rows['wellId'].tolist())
                _log.warning(f"Found {len(null_rows)} null values in {column}. WellIds: {truncated_ids}")
            for index in range(rows):
                value = df.at[index, column]
                if pd.isna(value):
                    continue
                try:
                    df.at[index, column] = pd.to_datetime(value).strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    _log.warning(f"Error converting {column} at row {index} with value '{value}'")

    @staticmethod
    def find_unique_ids(df_well, df_monthly, df_lookup, df_survey):
        list0 = df_lookup['wellId'].unique().tolist()
        list1 = df_well['wellId'].unique().tolist()
        list2 = df_monthly['wellId'].unique().tolist()
        list3 = df_lookup['prodWellId'].unique().tolist()
        list4 = df_lookup['surveyWellId'].unique().tolist()
        list5 = df_survey['wellId'].unique().tolist()
        unique_in_list0 = [item for item in list1 if item not in list0]
        unique_in_list1 = [item for item in list2 if item not in list1]
        unique_in_list2 = [item for item in list2 if item not in list3]
        unique_in_list3 = [item for item in list5 if item not in list4]
        unique_in_list4 = [item for item in list5 if item not in list1]
        if unique_in_list0:
            _log.warning(f"WellID in Well Lookup but not in Well Header: {len(unique_in_list0)} {_truncate_list_for_log(unique_in_list0)}")
        if unique_in_list1:
            _log.warning(f"WellID in MonthlyProduction but not in Well Header: {len(unique_in_list1)} (skipped printing wellIds)")
        if unique_in_list2:
            _log.warning(f"WellID in Monthly Production but not in Well Lookup: {len(unique_in_list2)} {_truncate_list_for_log(unique_in_list2)}")
        if unique_in_list3:
            _log.warning(f"WellID in Survey but not in Well Lookup: {len(unique_in_list3)} {_truncate_list_for_log(unique_in_list3)}")
        if unique_in_list4:
            _log.warning(f"WellID in Survey but not in Header: {len(unique_in_list4)} {_truncate_list_for_log(unique_in_list4)}")
    
    @staticmethod
    def check_interval_presence_and_count(df_source, df_target, interval_column):
        source_intervals_unique = df_source[interval_column].unique()
        counts_present = df_target[interval_column][
            df_target[interval_column].isin(source_intervals_unique)
        ].value_counts()
        counts_not_present = df_target[interval_column][
            ~df_target[interval_column].isin(source_intervals_unique)
        ].value_counts()
        return {
            'Intervals present in Target': counts_present,
            'Intervals not present in Target': counts_not_present
        }

class Process:
    @staticmethod
    def _rename_filter_dropdupe(df, table_name=None, do_drop_duplicates=True):
        if table_name is None:
            table_name = inspect.currentframe().f_back.f_code.co_name
        mapping = getattr(rm, table_name)
        df = df.rename(columns=mapping.mapper)
        df_non = df.loc[:, ~df.columns.isin(mapping.pai_cols)]
        df = df.loc[:, df.columns.isin(mapping.pai_cols)]
        _log.info(f'columns dropped: {list(df_non)}')
        if do_drop_duplicates and hasattr(mapping, 'key_cols'):
            before = len(df)
            df = df.drop_duplicates(subset=mapping.key_cols)
            _log.info(f'Dropped {before - len(df)} duplicates using keys: {mapping.key_cols}')
        return df
    
    @staticmethod
    def Well(df, do_drop_duplicates=True):
        return Process._rename_filter_dropdupe(df)

    @staticmethod
    def MonthlyProduction(df, do_drop_duplicates=True):
        df = Process._rename_filter_dropdupe(df)
        if 'prodDate' in df.columns:
            null_rows = df[df['prodDate'].isnull()]
        if not null_rows.empty:
            truncated_ids = _truncate_list_for_log(null_rows['wellId'].tolist())
            _log.warning(f"Dropping {len(null_rows)} rows with null prodDate. WellIds: {truncated_ids}")
        columns_to_check = ['oilCum_bbl', 'gasCum_Mcf', 'waterCum_bbl']
        if df[columns_to_check].isnull().all().all():
            df = Process._process_cumulative_data(df)
            _log.warning("Columns *Cum_* were empty, so process_cumulative_data was run.")

        return df

    @staticmethod
    def _process_cumulative_data(df, do_drop_duplicates=True):
        df_dict = {name: group.sort_values(by='prodDate') for name, group in df.groupby('wellId')}
        for name, group in df_dict.items():
            group['oilCum_bbl'] = group['oilVol_bbl'].cumsum()
            group['gasCum_Mcf'] = group['gasVol_Mcf'].cumsum()
            group['waterCum_bbl'] = group['waterVol_bbl'].cumsum()
            df_dict[name] = group
        return pd.concat(df_dict.values(), ignore_index=True)

    @staticmethod
    def WellLookup(df, do_drop_duplicates=True): 
        return Process._rename_filter_dropdupe(df)

    @staticmethod
    def WellDirectionalSurveyPoint(df, do_drop_duplicates=True):
        df = Process._rename_filter_dropdupe(df)
        if 'tvdss_ft' in df.columns:
            null_rows = df[df['tvdss_ft'].isnull()]
            if not null_rows.empty:
                _log.warning(f"Dropping {len(null_rows)} rows where tvdss_ft is null")
                df = df[df['tvdss_ft'].notnull()]

            if (df['tvdss_ft'] > 0).sum() > (len(df) / 2):
                _log.warning("More than 50% of tvdss_ft values are not negative. Multiplying all by -1.")
                df['tvdss_ft'] = df['tvdss_ft'] * -1

        df = df.sort_values(by=['wellId', 'md_ft'], ascending=True)
        return df

    @staticmethod
    def InventoryWells(df, do_drop_duplicates=True): 
        return Process._rename_filter_dropdupe(df)

    @staticmethod
    def GridStructureData(df, do_drop_duplicates=True):
        df = Process._rename_filter_dropdupe(df)
        if 'tvdss_ft' in df.columns:
            null_rows = df[df['tvdss_ft'].isnull()]
            if not null_rows.empty:
                _log.warning(f"Dropping {len(null_rows)} rows where tvdss_ft is null")
                df = df[df['tvdss_ft'].notnull()]

            if (df['tvdss_ft'] > 0).sum() > (len(df) / 2):
                _log.warning("More than 50% of tvdss_ft values are not negative. Multiplying all by -1.")
                df['tvdss_ft'] = df['tvdss_ft'] * -1
        return df

    @staticmethod
    def GridAttributeData(df, do_drop_duplicates=True): 
        return Process._rename_filter_dropdupe(df)

    @staticmethod
    def GridStructureHeader(df, do_drop_duplicates=True): 
        return df
    
    @staticmethod
    def GridAttributeHeader(df, do_drop_duplicates=True): 
        return df


class Summarize:
    @staticmethod
    def _summarize(df, relevant_columns=None, key_col='wellId'):
        if relevant_columns is None:
            table_name = inspect.currentframe().f_back.f_code.co_name
            mapping = getattr(rm, table_name)
            relevant_columns = mapping.relevant_columns if hasattr(mapping, 'relevant_columns') else mapping.pai_cols
        total_rows = df.shape[0]
        unique_count = df[key_col].nunique() if key_col in df.columns else 'N/A'
        duplicates = df[df.duplicated(key_col, keep=False)] if key_col in df.columns else pd.DataFrame()
        duplicate_values = duplicates[key_col].unique() if key_col in df.columns else []
        non_nulls = []
        nulls = []
        relevant_cols_exist = []
        missing_cols = []
        for col in relevant_columns:
            if col in df.columns:
                count = df[col].count()
                non_nulls.append(count)
                nulls.append(total_rows - count)
                relevant_cols_exist.append(col)
            else:
                missing_cols.append(col)
        df_dq = pd.DataFrame({
            'column': relevant_cols_exist,
            'non_nulls': non_nulls,
            'nulls': nulls
        })
        _log.info(f"Total Rows: {total_rows}")
        _log.info(f"Unique {key_col}s: {unique_count}")
        if len(duplicate_values):
            truncated_duplicates = Utilities._truncate_list_for_log(duplicate_values)
            _log.info(f"Total duplicate {key_col}s: {int(duplicates.shape[0] / 2)}")
            _log.info(f"Duplicate {key_col}s: {truncated_duplicates}")
        _log.info(f"\n{df_dq.to_string(index=False)}")
        if missing_cols:
            truncated_missing = Utilities._truncate_list_for_log(missing_cols)
            _log.warning(f"Columns that do not exist: {truncated_missing}")

    @staticmethod
    def Well(df):
        Summarize._summarize(df)
        Summarize.formation_counts(df)

    @staticmethod
    def MonthlyProduction(df):
        Summarize._summarize(df, key_col='wellId')
        duplicate_counts = df.groupby(['wellId', 'prodDate']).size().reset_index(name='count')
        duplicates = duplicate_counts[duplicate_counts['count'] > 1]
        output_dup_count = duplicates.groupby('wellId')['count'].sum()
        _log.info(f"Duplicated wellId & prodDate combo: {output_dup_count}")

    @staticmethod
    def WellLookup(df): 
        Summarize._summarize(df)
    
    @staticmethod
    def WellDirectionalSurveyPoint(df): 
        Summarize._summarize(df)
    
    @staticmethod
    def InventoryWells(df): 
        Summarize._summarize(df)

    @staticmethod
    def formation_counts(df, column='formationName'):
        if column not in df.columns:
            _log.warning(f"Column '{column}' not found in DataFrame.")
            return
        value_counts = df[column].value_counts(dropna=False).sort_index()
        _log.info(f"------------------- UNIQUE VALUE COUNTS FOR {column.upper()} -------------------")
        _log.info(f"{value_counts.to_string()}")
        _log.info(f"Total unique values (including NaN): {value_counts.shape[0]}")




