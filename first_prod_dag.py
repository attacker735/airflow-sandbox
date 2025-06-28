from airflow import DAG
import datetime
import time
import requests
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context
from airflow.models import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
import re
import pandas as pd
from bs4 import BeautifulSoup

args = {
    'owner': 'amalygin',
    'start_date': datetime.datetime(2025, 6, 19),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=1),
    'email_on_failture': True,
    'email_on_success': True,
}

def check_requests(ti: TaskInstance = None):
    filter_days = None
    months = {
        'Январь': 1,
        'Февраль': 2,
        'Март': 3,
        'Апрель': 4,
        'Май': 5,
        'Июнь': 6,
        'Июль': 7,
        'Август': 8,
        'Сентябрь': 9,
        'Октябрь': 10,
        'Ноябрь': 11,
        'Декабрь': 12
    }

    days = {'Sun': 'вс',
        'Mon': 'пн',
        'Tue': 'вт',
        'Wed': 'ср',
        'Thu': 'чт',
        'Fri': 'пт',
        'Sat': 'сб'
            }

    year = 2025
    content = []
    calendar_data = []
    url = f"https://www.consultant.ru/law/ref/calendar/proizvodstvennye/{year}/"
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')

    start_header = soup.find('h2',
                                 string=f'Производственный календарь на {year} год при пятидневной рабочей неделе')
    end_header = soup.find('h2', string=f'Производственный календарь {year}: комментарий')
    current_element = start_header.next_sibling

    while current_element and current_element != end_header:
        content.append(current_element)
        current_element = current_element.next_sibling

    for table in soup.find_all('table', class_='cal'):
        month_name = table.find('th', class_='month').text.strip()
        month_number = months.get(month_name, 0)
        for row in table.find('tbody').find_all('tr'):
            for cell in row.find_all('td'):
                day = cell.text.strip()
                if day:
                    day_number = int(re.search(r'\d+', day).group())
                    day_type = 'рабочий день'
                    date = f"{year}-{month_number:02d}-{day_number:02d}"
                    day_of_week = datetime.strptime(date, '%Y-%m-%d').strftime('%a')
                    day_of_week = days.get(day_of_week, 0)
                    if 'holiday' in cell.get('class', []):
                        day_type = 'Государственный праздник'
                    elif 'weekend' in cell.get('class', []) and 'holiday' not in cell.get('class', []):
                        day_type = 'выходной'

                    if filter_days is None or (filter_days == 0 and day_type == 'выходной') or (
                            filter_days == 1 and day_type == 'рабочий день'):
                        calendar_data.append((date, day_type, day_of_week))

    ti.xcom_push(key='test_df', value=calendar_data)


def insert_into_db(ti: TaskInstance = None):
    if PostgresHook is None:
        raise ImportError("PostgresHook не найден. Установите apache-airflow-providers-postgres")
    
    calendar_data = ti.xcom_pull(key='test_df')
    df = pd.DataFrame(calendar_data, columns=['full_date', 'description', 'dow_name'])
    df['description'] = df['description'].apply(lambda x: x.encode('utf-8').decode('utf-8'))
    postgres_hook = PostgresHook(postgres_conn_id='gp_test')

    create_calendar_table = """
    CREATE TABLE IF NOT EXISTS public.calendar_data(
	full_date date NULL,
	description varchar NULL,
	dow_name varchar(2) NULL,
	update_timestamp timestamp DEFAULT CURRENT_TIMESTAMP NULL
    )"""

    postgres_hook.run(create_calendar_table)

    truncate_query = """TRUNCATE TABLE public.calendar_data"""

    postgres_hook.run(truncate_query)
    
    rows = list(df.itertuples(index=False, name=None))

    print(rows)
    if rows:
        postgres_hook.insert_rows(
            table="calendar_data",
            rows=rows,
            target_fields=['full_date', 'description', 'dow_name'],
            commit_every=1000
        )
        print(f"Вставлено {len(rows)} записей в таблицу 'calendar_data'")

with DAG(
    'calendar_dag',
    default_args=args,
    description='DAG для парсинга календаря и записи в БД',
    schedule_interval='10 * * * *',
    catchup=False,
    tags=['calendar', 'parsing']
) as dag:
    
    parse_task = PythonOperator(task_id='parse_calendar_data',
    python_callable=check_requests,)

    insert_task = PythonOperator(task_id='insert_calendar_data',
                                 python_callable=insert_into_db,)
    
    parse_task >> insert_task