import calendar
import pandas
from .transformations import month_to_number

def transform(db_context, etl_process_id):
    compra_productos_extract = pandas.read_sql_table(table_name='COMPRA_PRODUCTOS_EXT', con=db_context, columns=[
                                                     'ITEM', 'ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE'])

    if compra_productos_extract.empty:
        return
    
    compra_productos_extract = pandas.melt(compra_productos_extract, id_vars='ITEM').rename(columns={'variable': 'MONTH', 'value': 'VALUE'})
    compra_productos_extract['ETL_PROCESS_ID'] = etl_process_id

    compra_productos_extract['VALUE'] = compra_productos_extract['VALUE'].str.replace(',', '')
    compra_productos_extract['VALUE'] = compra_productos_extract['VALUE'].fillna(value=0)
    compra_productos_extract['VALUE'] = pandas.to_numeric(compra_productos_extract['VALUE'])
    compra_productos_extract['VALUE'] = compra_productos_extract['VALUE'].abs()
    compra_productos_extract['ITEM'] = compra_productos_extract['ITEM'].apply(lambda x: x[11:])

    compra_productos_extract['MONTH'] = compra_productos_extract['MONTH'].apply(lambda x: month_to_number(x))
    compra_productos_extract['YEAR'] = 2021
    
    compra_productos_extract.to_sql(
        name='COMPRA_PRODUCTOS_TRA', con=db_context, if_exists='append', index=False)
