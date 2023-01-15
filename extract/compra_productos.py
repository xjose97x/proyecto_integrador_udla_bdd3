import pandas
from config import DataProperties

def truncate(db_context):
    db_context.execute('TRUNCATE TABLE COMPRA_PRODUCTOS_EXT')

def run(db_context):
    channels = pandas.read_csv(f'{DataProperties.DATA_PATH}/compra_productos.csv')
    channels.to_sql('COMPRA_PRODUCTOS_EXT', db_context, if_exists='append', index=False)