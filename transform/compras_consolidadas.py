import pandas

def transform(db_context, etl_process_id):
    compra_consolidadas_extract = pandas.read_sql_table(table_name='COMPRAS_CONSOLIDADAS_EXT', con=db_context, columns=['CATEGORIA', 'TOTAL_USD', 'TOTAL_PORCENTAJE', 'PRECIO_USD', 'PRECIO_PORCENTAJE', 'VOLUMEN_USD', 'VOLUMEN_PORCENTAJE'])

    if compra_consolidadas_extract.empty:
        return
    
    compra_consolidadas_extract['ETL_PROCESS_ID'] = etl_process_id

    numeric_columns = ['TOTAL_USD', 'TOTAL_PORCENTAJE', 'PRECIO_USD', 'PRECIO_PORCENTAJE', 'VOLUMEN_USD', 'VOLUMEN_PORCENTAJE']

    for column in numeric_columns:
        compra_consolidadas_extract[column] = compra_consolidadas_extract[column].str.replace(',', '')
        compra_consolidadas_extract[column] = compra_consolidadas_extract[column].fillna(value=0)
        compra_consolidadas_extract[column] = pandas.to_numeric(compra_consolidadas_extract[column])
        compra_consolidadas_extract[column] = compra_consolidadas_extract[column].abs()

    compra_consolidadas_extract.to_sql(
        name='COMPRAS_CONSOLIDADAS_TRA', con=db_context, if_exists='append', index=False)
