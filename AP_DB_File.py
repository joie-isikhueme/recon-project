import pyodbc
import pandas as pd
import sqlalchemy


accountReceivables=pd.read_excel('Account_Receivables_Breakdown.xlsx',engine='openpyxl')
generalLedger=pd.read_excel('GL_Account_Receivables.xlsx',engine='openpyxl')

conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server}; SERVER=ngfwcq3f3\mssqlserver01; DATABASE=NameTestDB;Trusted_Connection=yes")
cursor = conn.cursor()

engine = sqlalchemy.create_engine("mssql+pyodbc://ngfwcq3f3\mssqlserver01/NameTestDB?driver=ODBC Driver 17 for SQL Server")

accountReceivables.to_sql('AR_Table',engine,if_exists='replace')
generalLedger.to_sql('GL_Table',engine,if_exists='replace')