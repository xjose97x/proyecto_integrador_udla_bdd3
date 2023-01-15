import calendar
import csv

import pandas
from faker import Faker

from config import DataProperties


def generate_risk():
    fake = Faker()
    return fake.pyfloat(left_digits=1, right_digits=2, positive=True, max_value=10)

def truncate(db_context):
    db_context.execute('TRUNCATE TABLE RIESGO_PROVEEDORES_EXT')

def run(db_context):
    riesgo_proveedores_2021 = pandas.read_csv(f'{DataProperties.DATA_PATH}/riesgo_proveedores_2021.csv')

    proveedores = list(riesgo_proveedores_2021.iloc[:, 0])
    riesgo_2021 = list(riesgo_proveedores_2021.iloc[:, 1])
    with open(f'{DataProperties.DATA_PATH}/riesgo_proveedores.csv', 'w+') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['proveedor', 'year', 'riesgo'])
        for i in range(len(proveedores)):
            writer.writerow([proveedores[i], 2021, riesgo_2021[i]])
        for i in range(len(proveedores)):
            writer.writerow([proveedores[i], 2022, generate_risk()])

                
    gastos = pandas.read_csv(f'{DataProperties.DATA_PATH}/riesgo_proveedores.csv')
    gastos.to_sql('RIESGO_PROVEEDORES_EXT', db_context, if_exists='append', index=False)