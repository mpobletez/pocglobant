import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pyodbc
import json

engine = create_engine(f'mssql+pyodbc://viewAPIUser:V13w_API_Us3r!@azdadevsql03.database.windows.net/am-da-sdb-view_migr-dq?driver=ODBC Driver 17 for SQL Server')

columns = ['id','department']
df = pd.read_csv('departments.csv', header=None, names=columns)

df.to_sql('departments', con = engine, if_exists='append', index = False)


columns = ['id','job']
df = pd.read_csv('jobs.csv', header=None, names=columns)

df.to_sql('jobs', con = engine, if_exists='append', index = False)



columns = ['id','Name','datetime','department_id','job_id']
df = pd.read_csv('hired_employees.csv', header=None, names=columns)

df['job_id'] = df['job_id'].fillna(0) # clean 0 values 
df['department_id'] = df['department_id'].fillna(0)
df['datetime'] = df['datetime'].fillna('')
df['Name'] = df['Name'].fillna('')

df['job_id'] = df['job_id'].astype(int)
df['department_id'] = df['department_id'].astype(int)


df.to_sql('hired_employees', con = engine, if_exists='append', index = False)


# ##print(df.to_string()) 
# with open('ConfigDatabaseUS.json') as server_file:
#             ConfigDbUs = json.load(server_file)

# conndb = pyodbc.connect('DRIVER='+ConfigDbUs['driver']+
#                       ';Server='+ConfigDbUs['server']+
#                       ';PORT=1433;DATABASE='+ConfigDbUs['database']+
#                       ';UID='+ConfigDbUs['user']+
#                       ';PWD='+ConfigDbUs['password'] )
#                       #';Authentication='+ConfigDbUs['Authentication']) 

# conndb.autocommit = True
# cursor = conndb.cursor()

# ##df.to_sql('hired_employees', schema='dbo', con = conndb)

# for index, row in df.iterrows():
#      ##print(row.Name)
#      cursor.execute("INSERT INTO hired_employees (id,name,datetime,department_id,job_id) values(?,?,?,?,?)", row.id, row.Name, row.datetime,row.department_id,row.job_id)
# conndb.commit()
# cursor.close()

##print(df.to_string()) 