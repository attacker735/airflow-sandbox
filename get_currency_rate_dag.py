import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import pandas as pd
import sys
import csv
from airflow import DAG
import time
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context
from airflow.models import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
import re
import pandas as pd
from bs4 import BeautifulSoup

args = {
    'owner': 'amalygin',
    'start_date': datetime(2025, 6, 22),
    'retries': 0,
    'execution_timeout': timedelta(minutes=2),
    'email': 'malygin_ar@deltag.ru',
    'email_on_failure': True,
    'email_on_success': True
}

def extract_data(ti: TaskInstance = None):
    current_month = datetime.today().replace(day=1)  # Первый день текущего месяца
    currency_codes = ["R01235", "R01239", "R01375"]
    currency_names = ["Доллар США", "Евро", "Китайский юань"]

    data = []
    for currency_code, currency_name in zip(currency_codes, currency_names):
        first_day = current_month.replace(day=1)
        next_month = current_month.replace(day=28) + timedelta(days=4)  # последний день текущего месяца + 4 дня
        last_day = next_month - timedelta(days=next_month.day)

        # Формируем URL для запроса
        url = f"http://cbr.ru/scripts/XML_dynamic.asp?date_req1={first_day.strftime('%d/%m/%Y')}&date_req2={last_day.strftime('%d/%m/%Y')}&VAL_NM_RQ={currency_code}"

        response = requests.get(url)
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            rates = []
            for valute in root.findall('.//Record'):
                # print(valute)
                date = valute.attrib['Date']
                print(date)
                value = float(valute.find('Value').text.replace(',', '.'))
                print(value)
                nominal = float(valute.find('Nominal').text)
                print(nominal)
                rate = value / nominal
                rates.append({'date': date, 'rate': rate})
        else:
            print("Failed to retrieve currency rates.")
            break
        if rates:
            for rate in rates:
                data.append([rate['date'], currency_name, rate['rate']])

    ti.xcom_push(key='raw_data', value=data)

def load_data(ti: TaskInstance = None):
    if PostgresHook is None
        raise ImportError('PostgresHook не найден. Установите apache-airflow-providers-postgres')

    raw_data = ti.xcom_pull(key='raw_data')
    df = pd.DataFrame(raw_data, columns=['date', 'currency', 'rate'])
    postgres_hook = PostgresHook(postgres_conn_id='gp_test')

    create_currency_rate_table = """
    CREATE TABLE IF NOT EXISTS public.currency_rate(
    date date NULL,
    currency varchar NULL,
    rate numeric(10, 4),
    update_timestamp timestamp DEFAULT CURRENT_TIMESTAMP NULL)"""

    postgres_hook.run(create_currency_rate_table)

    rows = list(df.itertuples(index=False, name=None))

    if rows:
        postgres_hook.insert_rows(
            table="currency_rate",
            rows=rows,
            target_fields=['date', 'currency', 'rate'],
            commit_every=1000
        )
        print(f'{len(rows)} Строк успешно вставлено в таблицу currency_rate')


with DAG(
    'currency_rate_dag',
    default_args=args,
    description='DAG для парсинга курсов валют и записи в БД',
    schedule_interval='20 18 * * *',
    catchup=False,
    tags=['rate', 'parsing']
) as dag:

    parse_task = PythonOperator(task_id='parse_currency_rate_data', python_callable=extract_data,)
    insert_task = PythonOperator(task_id='insert_currency_rate_data', python_callable=load_data,)

    parse_task >> insert_task