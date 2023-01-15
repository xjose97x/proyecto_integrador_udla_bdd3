from utils import query_utils

def load(staging_db_context, core_db_context, etl_process_id):
    compras_consolidadas = query_utils.read_sql_by_process(table_name='COMPRAS_CONSOLIDADAS_TRA', columns=['CATEGORIA', 'TOTAL_USD', 'TOTAL_PORCENTAJE', 'PRECIO_USD', 'PRECIO_PORCENTAJE', 'VOLUMEN_USD', 'VOLUMEN_PORCENTAJE'], etl_process_id=etl_process_id, db_context=staging_db_context)
    query_utils.upsert(table_name='COMPRAS_CONSOLIDADAS', natural_key_cols=['CATEGORIA'], dataframe= compras_consolidadas, db_context=core_db_context)