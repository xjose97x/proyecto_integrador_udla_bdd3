import pandas

from transform.transformations import month_to_number

def transform(db_context, etl_process_id):
    ingresos_extract = pandas.read_sql_table(table_name='INGRESOS_EXT', con=db_context, columns=['YEAR', 'MONTH', 'VALUE'])

    if ingresos_extract.empty:
        return
    
    ingresos_extract['ETL_PROCESS_ID'] = etl_process_id

    ingresos_extract['MONTH'] = ingresos_extract['MONTH'].apply(lambda x: month_to_number(x, locale='en'))
    ingresos_extract['VALUE'] = ingresos_extract['VALUE'].str.replace(',', '')
    ingresos_extract['VALUE'] = ingresos_extract['VALUE'].fillna(value=0)
    ingresos_extract['VALUE'] = pandas.to_numeric(ingresos_extract['VALUE'])
    ingresos_extract['VALUE'] = ingresos_extract['VALUE'].abs()

    ingresos_extract.to_sql(
        name='INGRESOS_TRA', con=db_context, if_exists='append', index=False)
