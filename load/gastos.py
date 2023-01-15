from utils import query_utils

def load(staging_db_context, core_db_context, etl_process_id):
    gastos = query_utils.read_sql_by_process(table_name='GASTOS_TRA', columns=['YEAR', 'MONTH', 'VALUE'], etl_process_id=etl_process_id, db_context=staging_db_context)
    query_utils.upsert(table_name='GASTOS', natural_key_cols=['YEAR', 'MONTH'], dataframe= gastos, db_context=core_db_context)