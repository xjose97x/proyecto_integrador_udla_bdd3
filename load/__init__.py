import time

from . import (compra_productos, compras_consolidadas, gastos, ingresos, riesgo_proveedores)

def load(staging_db_context, core_db_context, etl_process_id):
    start = time.time()
    compra_productos.load(staging_db_context, core_db_context, etl_process_id)
    compras_consolidadas.load(staging_db_context, core_db_context, etl_process_id)
    gastos.load(staging_db_context, core_db_context, etl_process_id)
    ingresos.load(staging_db_context, core_db_context, etl_process_id)
    riesgo_proveedores.load(staging_db_context, core_db_context, etl_process_id)
    end = time.time()
    print(f"LOAD TOOK: {end - start} seconds")

def tests(db_context):
    pass