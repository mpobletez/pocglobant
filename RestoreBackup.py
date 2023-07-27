import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import fastavro

engine = create_engine(f'mssql+pyodbc://USER:PASS@SERVER/DB?driver=ODBC Driver 17 for SQL Server')
def avro_df(filepath, encoding):
    # Open file stream
    with open(filepath, encoding) as fp:
        # Configure Avro reader
        reader = fastavro.reader(fp)
        # Load records in memory
        records = [r for r in reader]
        # Populate pandas.DataFrame with records
        df = pd.DataFrame.from_records(records)
        # Return created DataFrame
        return df


department_df=avro_df('departments.avro','rb')
print(department_df)

department_df.to_sql('departments_restored', con = engine, if_exists='replace', index = False)

hired_employees_df=avro_df('hired_employees.avro','rb')
print(hired_employees_df)
hired_employees_df.to_sql('hired_employees_restored', con = engine, if_exists='replace', index = False)

jobs_df=avro_df('jobs.avro','rb')
print(jobs_df)
jobs_df.to_sql('jobs_restored', con = engine, if_exists='replace', index = False)
