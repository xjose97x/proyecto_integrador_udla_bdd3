import calendar
import csv

import pandas
from faker import Faker

from config import DataProperties

def generate_monthly_income():
    fake = Faker()
    return sum([float(fake.pricetag().replace('$', '').replace(',', '')) for i in range(100)])


with open(f'{DataProperties.DATA_PATH}/ingresos.csv', 'w+') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['year', 'month', 'value'])
    for i in range(2):
        for j in range(12):
            writer.writerow(
                [2021 + i, calendar.month_name[j + 1], generate_monthly_income()])


def truncate(db_context):
    db_context.execute('TRUNCATE TABLE INGRESOS_EXT')

def run(db_context):
    with open(f'{DataProperties.DATA_PATH}/ingresos.csv', 'w+', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['year', 'month', 'value'])
        for i in range(2):
            for j in range(12):
                writer.writerow(
                    [2021 + i, calendar.month_name[j + 1], generate_monthly_income()])
                
    ingresos = pandas.read_csv(f'{DataProperties.DATA_PATH}/ingresos.csv')
    ingresos.to_sql('INGRESOS_EXT', db_context, if_exists='append', index=False)