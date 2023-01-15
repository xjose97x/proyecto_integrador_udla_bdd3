import time
from . import (compra_productos, compras_consolidadas, gastos, ingresos, riesgo_proveedores)


def truncate(db_context):
    start = time.time()
    compra_productos.truncate(db_context)
    compras_consolidadas.truncate(db_context)
    gastos.truncate(db_context)
    ingresos.truncate(db_context)
    riesgo_proveedores.truncate(db_context)
    end = time.time()
    print(f"TRUNCATE TOOK: {end - start} seconds")


def extract(db_context):
    start = time.time()
    compra_productos.run(db_context)
    compras_consolidadas.run(db_context)
    gastos.run(db_context)
    ingresos.run(db_context)
    riesgo_proveedores.run(db_context)
    end = time.time()
    print(f"EXTRACT TOOK: {end - start} seconds")


def tests(db_context):
    compra_productos_count = db_context.execute('SELECT COUNT(*) FROM COMPRA_PRODUCTOS_EXT').scalar()
    assert compra_productos_count == 1037, f"Expected 1037 compra_productos, got {compra_productos_count}"