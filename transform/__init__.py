import time

from . import (compra_productos, compras_consolidadas, gastos, ingresos, riesgo_proveedores)

def transform(db_context, etl_process_id):
    start = time.time()
    compra_productos.transform(db_context, etl_process_id)
    compras_consolidadas.transform(db_context, etl_process_id)
    gastos.transform(db_context, etl_process_id)
    ingresos.transform(db_context, etl_process_id)
    riesgo_proveedores.transform(db_context, etl_process_id)
    end = time.time()
    print(f"TRANSFORM TOOK: {end - start} seconds")


def tests(db_context, etl_process_id):
    pass