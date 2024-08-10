import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
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
         

    def create_connection_string(self):
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

