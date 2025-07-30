#%%
#---------------IMPORTS------------------
import os
import logging
import pandas as pd
import mysql.connector
from datetime import date
from pyspark.sql import DataFrame, SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#Petro scripts:
from . import RENAME_MAPPING as rm
from .QC_functions import Process, Summarize, Utilities

__version__ = "2.0.0"

class Processor:
    def __init__(self, credentials, data_source, do_compare, do_drop_duplicates, do_save_cleaned_files, tables_to_check, basin, source_path, target_path, spark=None):
        self._log = logging.getLogger(__name__)
        self.credentials = credentials
        self.data_source = data_source
        self.do_compare = do_compare
        self.do_drop_duplicates = do_drop_duplicates
        self.do_save_cleaned_files = do_save_cleaned_files
        self.tables_to_check = tables_to_check
        self.basin = basin
        self.source_path = source_path
        self.target_path = target_path
        self.spark = spark

    def execute(self):
        loader_cls = getattr(Processor, self.data_source)
        loader = loader_cls()
        self.df_dict = loader.load_tables(tables_to_check=self.tables_to_check, source_path=self.source_path, credentials=self.credentials, spark=self.spark)

        for table_name, df in self.df_dict.items():
            self._process_table(table_name, df)

        if self.do_compare:
            self._compare_ids()

    def _target_path(self, table_name):
        if self.target_path.startswith("/") or self.target_path.startswith("dbfs:/"):
            return os.path.join(self.target_path, f'{table_name}.csv')
        else:
            return f'{self.target_path}{table_name.lower()}'


    def _write_cleaned(self, df, table_name):
        if not self.do_save_cleaned_files:
            return
        target = self._target_path(table_name)
        if self.data_source in ["files", "database"]:
            df.to_csv(target, index=False)
            self._log.info(f"{table_name}: Wrote {len(df):,} rows to CSV at {target}")
            return
        elif self.data_source == "databricks":
            if not self.spark:
                from pyspark.sql import SparkSession
                self.spark = SparkSession.getActiveSession()
                self._log.info("Spark session started")
            # Convert datetime.date in object columns
            for col in df.select_dtypes(include='object').columns:
                if df[col].apply(lambda x: isinstance(x, date)).any():
                    self._log.info(f"Converting column {col} from datetime.date to datetime64[ns]")
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            try:
                chunk_size = 100_000
                total_rows = len(df)
                if total_rows > chunk_size:
                    self._log.warning(f"{table_name}: DataFrame too large ({total_rows:,} rows). Splitting into chunks of {chunk_size}")
                    for i in range(0, total_rows, chunk_size):
                        chunk = df.iloc[i:i+chunk_size]
                        spark_chunk = self.spark.createDataFrame(chunk)
                        spark_chunk.write.mode("append").format("delta").saveAsTable(target)
                        self._log.info(f"{table_name}: Wrote chunk rows {i:,} – {min(i+chunk_size, total_rows):,}")
                    # Verify total rows after write
                    final_count = self.spark.table(target).count()
                    self._log.info(f"{table_name}: Final row count in {target} is {final_count:,}")
                    if final_count != total_rows:
                        self._log.warning(f"{table_name}: Row mismatch! Input={total_rows:,}, Written={final_count:,}")
                else:
                    spark_df = self.spark.createDataFrame(df)
                    self._log.info(f"{table_name}: Writing {total_rows:,} rows to Delta table at {target}")
                    spark_df.write.mode("overwrite").format("delta").saveAsTable(target)
                    self._log.info(f"{table_name}: Overwrite write complete")
                    # Verify
                    final_count = self.spark.table(target).count()
                    self._log.info(f"{table_name}: Confirmed {final_count:,} rows written to {target}")
                    if final_count != total_rows:
                        self._log.warning(f"{table_name}: Row mismatch! Input={total_rows:,}, Written={final_count:,}")
            except Exception as e:
                self._log.error(f"{table_name}: Failed during Spark write — {e}")
        else:
            raise NotImplementedError(f"Write method not implemented for data_source: {self.data_source}")

    def _process_table(self, table_name, df):
        self._log.info(f"----------------------{table_name.upper()} INFO-----------------------------")
        self._log.info(f"list({df.columns})")

        try:
            if not hasattr(Process, table_name):
                self._log.warning(f"Skipping unknown table: {table_name}")
                return

            process_fn = getattr(Process, table_name)
            summarize_fn = getattr(Summarize, table_name, None)

            df = process_fn(df, do_drop_duplicates=self.do_drop_duplicates)

            if table_name in ['Well']:
                Utilities.date_checker(df, ['completionDate'])
            elif table_name == 'MonthlyProduction':
                Utilities.date_checker(df, ['prodDate'])

            self._write_cleaned(df, table_name)

            if summarize_fn:
                summarize_fn(df)

            if table_name == 'GridStructureData':
                header = self.df_dict.get('GridStructureHeader')
                if header is None:
                    self._log.warning("GridStructureHeader is missing. Skipping interval check.")
                else:
                    attr_header = self.df_dict.get('GridAttributeHeader')
                    result1 = Utilities.check_interval_presence_and_count(header, df, 'interval')
                    result2 = Utilities.check_interval_presence_and_count(header, attr_header, 'interval')
                    self._log.info(f'Structure header intervals in Structure Data:\n{result1["Intervals present in Target"]}')
                    self._log.info(f'Structure header intervals NOT in Structure Data:\n{result1["Intervals not present in Target"]}')
                    self._log.info(f'Structure header intervals in Attribute Header:\n{result2["Intervals present in Target"]}')
                    self._log.info(f'Structure header intervals NOT in Attribute Header:\n{result2["Intervals not present in Target"]}')

            if table_name == 'GridAttributeData':
                header = self.df_dict.get('GridAttributeHeader')
                if header is None:
                    self._log.warning("GridAttributeHeader is missing. Skipping interval check.")
                else:
                    result = Utilities.check_interval_presence_and_count(header, df, 'name')
                    self._log.info(f'Attribute header intervals in Attribute Data:\n{result["Intervals present in Target"]}')
                    self._log.info(f'Attribute header intervals NOT in Attribute Data:\n{result["Intervals not present in Target"]}')

            if table_name in ['GridAttributeHeader', 'GridAttributeData']:
                header = self.df_dict.get('GridAttributeHeader')
                data = self.df_dict.get('GridAttributeData')
                if header is not None and data is not None:
                    header_names = set(header['name'].dropna().unique())
                    data_names = set(data['name'].dropna().unique())

                    missing_in_data = header_names - data_names
                    missing_in_header = data_names - header_names

                    if missing_in_data:
                        self._log.warning(
                            f"'name' values in GridAttributeHeader but missing in GridAttributeData: "
                            f"{len(missing_in_data)} — {Utilities._truncate_list_for_log(sorted(missing_in_data))}"
                        )
                    if missing_in_header:
                        self._log.warning(
                            f"'name' values in GridAttributeData but missing in GridAttributeHeader: "
                            f"{len(missing_in_header)} — {Utilities._truncate_list_for_log(sorted(missing_in_header))}"
                        )

        except AttributeError as e:
            if not hasattr(Process, table_name):
                raise NotImplementedError(f"No handler defined for table: {table_name}") from e
            else:
                raise
    
    def _compare_ids(self):
        well = self.df_dict.get('Well')
        prod = self.df_dict.get('MonthlyProduction')
        lookup = self.df_dict.get('WellLookup')
        survey = self.df_dict.get('WellDirectionalSurveyPoint')

        missing = [k for k, v in zip(
            ['Well', 'MonthlyProduction', 'WellLookup', 'WellDirectionalSurveyPoint'],
            [well, prod, lookup, survey]
        ) if v is None]

        if missing:
            self._log.warning(f"Skipping ID comparison due to missing tables: {missing}")
            return

        Utilities.find_unique_ids(well, prod, lookup, survey)

    class files:
        def load_tables(self, tables_to_check, source_path, credentials, spark=None):
            dfs = {}
            for table in tables_to_check:
                path = os.path.join(source_path, f"{table}.csv")
                if os.path.exists(path):
                    dfs[table] = pd.read_csv(path)
            return dfs

    class database:
        def load_tables(self, tables_to_check, source_path, credentials, spark=None):
            conn = mysql.connector.connect(**credentials)
            dfs = {t: pd.read_sql(f"SELECT * FROM {t}", conn) for t in tables_to_check}
            conn.close()
            return dfs

    class databricks:
        def load_tables(self, tables_to_check, source_path, credentials, spark=None):
            if not spark:
                from pyspark.sql import SparkSession
                spark = SparkSession.getActiveSession()
            if not spark:
                raise ValueError("SparkSession is required for loading Databricks tables.")
            dfs = {}
            for table in tables_to_check:
                full_table_name = f"{source_path}{table}"
                try:
                    spark_df = spark.sql(f"SELECT * FROM {full_table_name}")
                    dfs[table] = spark_df.toPandas()
                except Exception as e:
                    logging.warning(f"Could not load table {full_table_name}: {e}")
            return dfs

def main(
    credentials: dict = {
        'host': '',
        'user': '',
        'password': '',
        'port': '',
        'database': ''
    },
    data_source: str = 'databricks', # options: databricks, files, database 
    do_compare: bool = True,
    do_drop_duplicates: bool = True,
    do_save_cleaned_files: bool = True,
    tables_to_check: list = [
        'Well',
        'WellLookup',
        'MonthlyProduction',
        'WellDirectionalSurveyPoint',
        'GridAttributeHeader',
        'GridAttributeData',
        'GridStructureHeader',
        'GridStructureData',
        'InventoryWells'
    ],
    basin: str = 'midland', 
    source_path: str = f'catalog.source_schema.basin_',       
    #source_path: str = f'C:/Users/{user}/{credentials['database']}/', 
    target_path: str = f'catalog.target_schema.basin_',      
    #target_path: str = f'C:/Users/{user}/{credentials['database']}/for-petroai/',
    spark: SparkSession = None
) -> int:
    _log = logging.getLogger(__name__)
    try:
        processor = Processor(
            credentials=credentials,
            data_source=data_source,
            do_compare=do_compare,
            do_drop_duplicates=do_drop_duplicates,
            do_save_cleaned_files=do_save_cleaned_files,
            tables_to_check=tables_to_check,
            basin=basin,
            source_path=source_path,
            target_path=target_path,
            spark=spark,
        )
        processor.execute()
    except Exception as e:
        _log.exception(e)
        return 1
    else:
        return 0



if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Run PetroAI source data QC")
    
    # Credentials
    parser.add_argument('--host', type=str, default='', help='MySQL host')
    parser.add_argument('--user', type=str, default='', help='MySQL user')
    parser.add_argument('--password', type=str, default='', help='MySQL password')
    parser.add_argument('--port', type=str, default='', help='MySQL port')
    parser.add_argument('--database', type=str, default='', help='MySQL database name')

    # Main options
    parser.add_argument('--data_source', choices=['databricks', 'files', 'database'], default='files')
    parser.add_argument('--do_compare', action='store_true', default=True)
    parser.add_argument('--no_compare', dest='do_compare', action='store_false')
    parser.add_argument('--do_drop_duplicates', action='store_true', default=True)
    parser.add_argument('--no_drop_duplicates', dest='do_drop_duplicates', action='store_false')
    parser.add_argument('--do_save_cleaned_files', action='store_true', default=True)
    parser.add_argument('--no_save_cleaned_files', dest='do_save_cleaned_files', action='store_false')

    parser.add_argument('--tables_to_check', type=str,
                        default='Well,WellLookup,MonthlyProduction,WellDirectionalSurveyPoint,GridAttributeHeader,GridAttributeData,GridStructureHeader,GridStructureData,InventoryWells',
                        help='Comma-separated list of tables')

    parser.add_argument('--basin', type=str, default='midland')
    parser.add_argument('--source_path', type=str, default='C:/Users/charles/Downloads/Midland')
    parser.add_argument('--target_path', type=str, default='C:/Users/charles/Downloads/Midland/for-petroai/')

    args = parser.parse_args()

    credentials = {
        'host': args.host,
        'user': args.user,
        'password': args.password,
        'port': args.port,
        'database': args.database
    }

    tables = [t.strip() for t in args.tables_to_check.split(',') if t.strip()]

    # Use Spark session if available
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    result = main(
        credentials=credentials,
        data_source=args.data_source,
        do_compare=args.do_compare,
        do_drop_duplicates=args.do_drop_duplicates,
        do_save_cleaned_files=args.do_save_cleaned_files,
        tables_to_check=tables,
        basin=args.basin,
        source_path=args.source_path,
        target_path=args.target_path,
        spark=spark
    )

    exit(result)