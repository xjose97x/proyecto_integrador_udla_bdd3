import pandas

def transform(db_context, etl_process_id):
    riesgo_proveedores_extract = pandas.read_sql_table(table_name='RIESGO_PROVEEDORES_EXT', con=db_context, columns=['PROVEEDOR', 'YEAR', 'RIESGO'])

    if riesgo_proveedores_extract.empty:
        return
    
    riesgo_proveedores_extract['ETL_PROCESS_ID'] = etl_process_id
    riesgo_proveedores_extract['PROVEEDOR'] = riesgo_proveedores_extract['PROVEEDOR'].apply(lambda x: x[9:])
    riesgo_proveedores_extract['YEAR'] = riesgo_proveedores_extract['YEAR'].astype(int)
    riesgo_proveedores_extract['RIESGO'] = riesgo_proveedores_extract['RIESGO'].astype(float)

    riesgo_proveedores_extract.to_sql(
        name='RIESGO_PROVEEDORES_TRA', con=db_context, if_exists='append', index=False)
