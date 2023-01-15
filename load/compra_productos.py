from utils import query_utils

def load(staging_db_context, core_db_context, etl_process_id):
    compra_productos = query_utils.read_sql_by_process(table_name='COMPRA_PRODUCTOS_TRA', columns=['ITEM', 'MONTH', 'YEAR', 'VALUE'], etl_process_id=etl_process_id, db_context=staging_db_context)
    query_utils.upsert(table_name='COMPRA_PRODUCTOS', natural_key_cols=['ITEM'], dataframe= compra_productos, db_context=core_db_context)