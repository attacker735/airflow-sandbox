from airflow import DAG
import datetime
import time
import requests
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os
import json

args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2025, 3, 29),
    'provide_context': True
}

key = 'bf0df043031b4d4aa73122929252903'
print(key)
start_hour = 1
horizont_hours = 48

lat = 47.939
lng = 46.681
moscow_timezone = 3

def extract_data(key):
    # ti = kwargs['ti']
    try:
        response = requests.get(
            'http://api.worldweatheronline.com/premium/v1/weather.ashx',
            params={
                'q':'{},{}'.format(lat, lng),
                'tp':'1',
                'num_of_days':2,
                'format':'json',
                'key':key
            },
            headers={
                'Authorization': key
            }
        )

        if response.status_code == 200:
            json_data = response.json()
            print(json_data)
            return json_data

            # ti.xcom_push(key='weather_json', value=json_data)
    except Exception as e:
        print(f'error {e}')


def transform_data(json_data):
    # Преобразование JSON-строки в словарь Python
    if isinstance(json_data, str):
        json_data = json.loads(json_data)

    # Текущее время в UTC с учетом часового пояса Москвы
    start_time = datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=moscow_timezone)
    end_time = start_time + datetime.timedelta(hours=horizont_hours)

    # Удаление информации о часовом поясе (если не нужно работать с часовыми поясами)
    start_time = start_time.replace(tzinfo=None)
    end_time = end_time.replace(tzinfo=None)

    # Извлечение данных из JSON
    date_list, value_list = [], []
    weather_data = json_data['data']['weather']
    for c in range(len(weather_data)):
        temp_date = weather_data[c]['date']
        hourly_values = weather_data[c]['hourly']
        for i in range(len(hourly_values)):
            date_time_str = '{} {:02d}:00:00'.format(temp_date, int(hourly_values[i]['time']) // 100)
            date_list.append(date_time_str)
            value_list.append(hourly_values[i]['cloudcover'])

    # Создание DataFrame
    res_df = pd.DataFrame(value_list, columns=['cloud_cover'])
    res_df['date_to'] = date_list
    res_df['date_to'] = pd.to_datetime(res_df['date_to'])

    # Фильтрация данных по временному диапазону
    res_df = res_df[res_df['date_to'].between(start_time, end_time, inclusive='both')]

    # Корректировка времени на часовой пояс Москвы
    res_df['date_to'] = res_df['date_to'] + datetime.timedelta(hours=moscow_timezone)
    res_df['date_to'] = res_df['date_to'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Добавление начальной даты и даты обработки
    res_df['date_from'] = start_time
    res_df['date_from'] = pd.to_datetime(res_df['date_from']).dt.strftime('%Y-%m-%d %H:%M:%S')
    res_df['processing_date'] = res_df['date_from']

    print(res_df.head())
    # ti.xcom_push(key='weather_df', value=res_df)


def load_data(res_df):
    # ti = kwargs['ti']
    # res_df = ti.xcom_pull(key='weather_df', task_ids=['transform_data'])[0]
    print(res_df.head())


json = extract_data(key)
res = transform_data(json)
load_data(res)