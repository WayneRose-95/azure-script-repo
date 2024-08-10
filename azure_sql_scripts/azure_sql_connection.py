import pyodbc
from sqlalchemy import create_engine
import pandas as pd 

server = '' # replace with your Azure SQL Database server name
database= '' # replace with your Azure SQL Database name
username = '' # replace with your Azure SQL Database username
password = '' # replace with your Azure SQL Database password
driver = '{ODBC Driver 18 for SQL Server}'

# Create the connection string
connection_string=f'Driver={driver};\
    Server=tcp:{server},1433;\
    Database={database};\
    Uid={username};\
    Pwd={password};\
    Encrypt=yes;\
    TrustServerCertificate=no;\
    Connection Timeout=30;'

# Create the engine to connect to the database
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(connection_string))

engine.connect() 

print("connection successful")

