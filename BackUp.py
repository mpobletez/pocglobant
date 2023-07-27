import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pyodbc
import json
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

engine = create_engine(f'mssql+pyodbc://viewAPIUser:V13w_API_Us3r!@azdadevsql03.database.windows.net/am-da-sdb-view_migr-dq?driver=ODBC Driver 17 for SQL Server')

df = pd.read_sql('select * from departments',engine)

schema = {
    'name': 'departments',
    'type': 'record',
    'fields': [
        {'name': 'id', 'type': 'int'},
        {'name': 'department', 'type': 'string'}
    ]
}

# Parse the schema so we can use it to write the data
schema_parsed = avro.schema.Parse(json.dumps(schema))

# Write data to an avro file
with open('departments.avro', 'wb') as f:
    writer = DataFileWriter(f, DatumWriter(), schema_parsed)
    #writer.append({'id': 'Pierre-Simon Laplace', 'department': 77})
    for index, row in df.iterrows():
        writer.append({'id': row.id, 'department': row.department})
    
    writer.close()

################ END DEPARTMENTS####################################
################ JOBS ##############################################
df = pd.read_sql('select * from jobs',engine)

schema = {
    'name': 'departments',
    'type': 'record',
    'fields': [
        {'name': 'id', 'type': 'int'},
        {'name': 'job', 'type': 'string'}
    ]
}

# Parse the schema so we can use it to write the data
schema_parsed = avro.schema.Parse(json.dumps(schema))

# Write data to an avro file
with open('jobs.avro', 'wb') as f:
    writer = DataFileWriter(f, DatumWriter(), schema_parsed)
    #writer.append({'id': 'Pierre-Simon Laplace', 'department': 77})
    for index, row in df.iterrows():
        writer.append({'id': row.id, 'job': row.job})
    
    writer.close()

################ END JOBS#########################################

################ EMPLOYEES ##############################################
df = pd.read_sql('select * from hired_employees',engine)

schema = {
    'name': 'departments',
    'type': 'record',
    'fields': [
        {'name': 'id', 'type': 'int'},
        {'name': 'Name', 'type': 'string'},
        {'name': 'datetime', 'type': 'string'},
        {'name': 'department_id', 'type': 'int'},
        {'name': 'job_id', 'type': 'int'}
    ]
}

# Parse the schema so we can use it to write the data
schema_parsed = avro.schema.Parse(json.dumps(schema))

# Write data to an avro file
with open('hired_employees.avro', 'wb') as f:
    writer = DataFileWriter(f, DatumWriter(), schema_parsed)
    #writer.append({'id': 'Pierre-Simon Laplace', 'department': 77})
    for index, row in df.iterrows():
        writer.append({'id': row.id, 'Name': row.Name, 'datetime': row.datetime, 'department_id': row.department_id,'job_id': row.job_id,})
    
    writer.close()

################ END EMPLOYEES#########################################
