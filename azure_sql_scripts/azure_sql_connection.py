import pyodbc
from json import load 
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from yaml import safe_load
import pandas as pd 

class AzureSQLDatabaseConnector:

    def __init__(
            self, 
            server=None,
            database=None, 
            username=None, 
            password=None, 
            driver='{ODBC Driver 18 for SQL Server}'
            ):
        self.server = server # replace with your Azure SQL Database server name
        self.database = database # replace with your Azure SQL Database name
        self.username = username # replace with your Azure SQL Database username
        self.password = password # replace with your Azure SQL Database password
        self.driver = '{ODBC Driver 18 for SQL Server}'
         
    def load_credentials(self, file_name : str, file_type: str = 'json' or 'yaml'):
        # Load in a credentials file 
        if file_type == 'json':
            with open(file_name) as file:
                credentials_file = load(file)
            
            # Reassign each attribute of the class whose entry is None to the values at these keys 
            self.server = credentials_file['server_name']
            self.database = credentials_file['database']
            self.username = credentials_file['username']
            self.password = credentials_file['password']

        elif file_type == 'yaml':
            with open(file_name) as file:
                credentials_file = safe_load(file)
        
            # Reassign each attribute of the class whose entry is None to the values at these keys 
            self.server = credentials_file['server_name']
            self.database = credentials_file['database']
            self.username = credentials_file['username']
            self.password = credentials_file['password']
        # credentials file can support either yaml or json 

        # Once loaded, can populate attributes with the fields 
        pass 
    def create_connection_string(self):
        #TODO: Add an extra parameter: database_engine which accepts the following engines 

        '''
        postgresql
        mariadb
        oracle
        mysql
        mssql
        
        all database engines aside from mssql have different ways to create their connection strings 
        '''
        # Create the connection string
        connection_string = f'Driver={self.driver};\
            Server=tcp:{self.server},1433;\
            Database={self.database};\
            Uid={self.username};\
            Pwd={self.password};\
            Encrypt=yes;\
            TrustServerCertificate=no;\
            Connection Timeout=30;'
        
        return connection_string

    def initalise_database_engine(self, connection_string : str):
        #TODO: Add an extra parameter: database_engine which accepts the following engines 

        '''
        postgresql
        mariadb
        oracle
        mysql
        mssql
        
        all database engines aside from mssql have different ways to create their connection strings 
        '''
        # Create the engine to connect to the database
        engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(connection_string))
        return engine 
        

    def test_connection(self, engine : Engine):
        try:
            engine.connect()
            print('Connection Successful')
        except OperationalError:
            print('Failed to establish connection to database')
            raise OperationalError
        

if __name__ == "__main__":
    azdb = AzureSQLDatabaseConnector()

