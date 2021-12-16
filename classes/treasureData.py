import pytd
import os
import logging
import pandas as pd
import time
from dotenv import load_dotenv
from pathlib import Path

class TreasureData:
    # Methods to integrate with TreasureData.

    def __init__(self) -> None:
        # Read environment variables
        load_dotenv()
        env_path = Path('..')/'.env'
        load_dotenv(dotenv_path=env_path)

        #Set up Logging
        self.logger = logging.getLogger('TreasureData')        
        logging.basicConfig(filename="tdLogfile.txt")
        self.stderrLogger=logging.StreamHandler()
        self.stderrLogger.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
        logging.getLogger().addHandler(self.stderrLogger)       
        self.logger.setLevel(os.getenv('logLevel'))
        self.logger.debug("Class Initialised")

        # Treasure Data login vars
        self.td_api_server=os.getenv('td_api_server')
        self.td_api_key=os.getenv('td_api_key')
        self.td_engine_name=os.getenv('td_engine_name')

    def connect(self, databaseName):
        self.db_client=pytd.Client(database=databaseName)

    def execute_sql(self, source_sql):
        # Read PII from Source Table and return a pandas dataframe
        # Intended for select queries that return a result.
        df="" 
        start_time = time.time()
        res = self.db_client.query(source_sql)
        self.logger.info("-- Running : [" + source_sql + "]---")
        df = pd.DataFrame(**res)
        self.logger.info("--- Total read "+str(len(df))+" source rows in : {:.3f} seconds ---".format((time.time() - start_time))) 
        return df

    def create_table(self, tablename, tabledef):
        sql='CREATE TABLE IF NOT EXISTS '+ tablename +' ( ' + tabledef + ' )'
        self.logger.debug("SQL:" + sql)
        self.db_client.query(sql)

    def clear_table(self,tablename):
        self.db_client.query('DELETE FROM '+ tablename +' WHERE 1=1')
    
    def write_to_table(self, tablename, tableColumns, tableData=[]):
        # Writes data to a Treasure Data table.
        if len(tableData) > 0:
            # Create a dataframe
            dest_df=pd.DataFrame(columns=tableColumns)
            dest_df=dest_df.append(tableData, ignore_index=True)
            self.db_client.load_table_from_dataframe(dest_df, tablename, if_exists='append')