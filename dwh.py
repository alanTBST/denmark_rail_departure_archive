# -*- coding: utf-8 -*-
"""
Created on Thu Dec 17 10:45:53 2020

@author: alkj
"""

import urllib
import logging

import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
from sqlalchemy.exc import SQLAlchemyError

log = logging.getLogger(__name__)

def write_to_dwh(df, table_name=None, departure_type='rail'):

    """put it in the data warehouse"""

    params = urllib.parse.quote_plus(
        'DRIVER={SQL Server};'+'SERVER='+'TSDW03'+
        ';DATABASE='+'dbDwhExtract'+';trusted_connection=yes'
        )

    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

    insp = reflection.Inspector.from_engine(engine)

    if departure_type == 'rail':
        current_form = insp.get_columns("departures", schema='Rejseplanen')
        current_form_f = insp.get_columns("departures_FULL", schema='Rejseplanen')
    elif departure_type == 'bus':
        current_form = insp.get_columns("bus_departures", schema='Rejseplanen')
        current_form_f = insp.get_columns("bus_departures_FULL", schema='Rejseplanen')

    current_columns = {x['name'] for x in current_form}
    current_columns_f = {x['name'] for x in current_form_f}

    data_columns = set(df.columns)

    new_columns = list(data_columns - current_columns)
    new_columns_f = list(data_columns - current_columns_f)

    if new_columns:
        for new_col in new_columns:

            new_col = '[' + new_col + ']'

            sql = """ALTER TABLE dbDwhExtract.Rejseplanen.{}
                     ADD {} bigint""".format(table_name, new_col)
            engine.execute(sql)
    if new_columns_f:
        for new_col in new_columns_f:

            new_col = '[' + new_col + ']'

            sql_2 = """ALTER TABLE dbDwhExtract.Rejseplanen.{0}_FULL
                       ADD {1} bigint""".format(table_name, new_col)
            engine.execute(sql_2)

    try:
        df.to_sql(table_name,
                  engine, 
                  chunksize=1000,
                  index=False,
                  if_exists='append',
                  schema='Rejseplanen')
    except (SQLAlchemyError, pyodbc.ProgrammingError):

        for new_col in new_columns_f:
            new_col = '[' + new_col + ']'

            sql = """ALTER TABLE dbDwhExtract.Rejseplanen.{}
                     ALTER COLUMN {} nvarchar""".format(table_name, new_col)
            engine.execute(sql)

            sql_2 = """ALTER TABLE dbDwhExtract.Rejseplanen.{}_FULL
                       ALTER COLUMN {} nvarchar""".format(table_name, new_col)
            engine.execute(sql_2)

        df.to_sql(table_name,
                  engine, 
                  chunksize=1000,
                  index=False,
                  if_exists='append',
                  schema='Rejseplanen')
    except Exception as e:
        log.warning(f"Can't write to warehouse. {table_name}. Error -> {str(e)}")
        with open('last_write_fail.txt', 'w') as fp:
            fp.write('FAILED')
       
    return