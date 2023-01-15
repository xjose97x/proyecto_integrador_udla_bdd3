from utils import query_utils

def load(staging_db_context, core_db_context, etl_process_id):
    ingresos = query_utils.read_sql_by_process(table_name='RIESGO_PROVEEDORES_TRA', columns=['PROVEEDOR', 'YEAR', 'RIESGO'], etl_process_id=etl_process_id, db_context=staging_db_context)
    query_utils.upsert(table_name='RIESGO_PROVEEDORES', natural_key_cols=['PROVEEDOR', 'YEAR'], dataframe= ingresos, db_context=core_db_context)