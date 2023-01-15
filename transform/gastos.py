import pandas

from transform.transformations import month_to_number

def transform(db_context, etl_process_id):
    gastos_extract = pandas.read_sql_table(table_name='GASTOS_EXT', con=db_context, columns=['YEAR', 'MONTH', 'VALUE'])

    if gastos_extract.empty:
        return
    
    gastos_extract['ETL_PROCESS_ID'] = etl_process_id

    gastos_extract['MONTH'] = gastos_extract['MONTH'].apply(lambda x: month_to_number(x, locale='en'))
    gastos_extract['VALUE'] = gastos_extract['VALUE'].str.replace(',', '')
    gastos_extract['VALUE'] = gastos_extract['VALUE'].fillna(value=0)
    gastos_extract['VALUE'] = pandas.to_numeric(gastos_extract['VALUE'])
    gastos_extract['VALUE'] = gastos_extract['VALUE'].abs()

    gastos_extract.to_sql(
        name='GASTOS_TRA', con=db_context, if_exists='append', index=False)
