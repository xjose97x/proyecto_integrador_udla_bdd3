import pandas
from config import DataProperties

def truncate(db_context):
    db_context.execute('TRUNCATE TABLE COMPRAS_CONSOLIDADAS_EXT')

def run(db_context):
    channels = pandas.read_csv(f'{DataProperties.DATA_PATH}/compras_consolidadas_2021_2022.csv')
    channels = channels.rename({'PRODUCTO': 'PRODUCTO', 'Total USD': 'TOTAL_USD', 'Total %': 'TOTAL_PORCENTAJE', 'Precio USD': 'PRECIO_USD', 'Precio %': 'PRECIO_PORCENTAJE', 'Volumen USD': 'VOLUMEN_USD', 'Volumen %': 'VOLUMEN_PORCENTAJE'}, axis=1)
    channels.to_sql('COMPRAS_CONSOLIDADAS_EXT', db_context, if_exists='append', index=False)