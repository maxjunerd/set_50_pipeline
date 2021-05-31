from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import datetime
import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import pendulum

# Connect to postgres
conn = PostgresHook('postgres').get_conn()
cur = conn.cursor()

# Bangkok timezone
bangkok_tz = pendulum.timezone('Asia/Bangkok')

# Get current time
data_date = datetime.datetime.now()

# Column name for set_50 and overall_merket
set_50_columns = ['name', 'open', 'high', 'low', 'last', 'change', 'percent_change', 'bid', 'offer', 'volumn_shares', 'value_k_baht']
overall_market_columns = ['name', 'last', 'change', 'percent_change', 'high', 'low', 'volumn_k', 'value_m_baht']

# Separate column containing string and float types
set_50_str = set_50_columns[0]
set_50_float = set_50_columns[1:]
overall_market_str = overall_market_columns[0]
overall_market_float = overall_market_columns[1:]

default_args = {
    'owner': 'Max',
    'depends_on_past': False,
    'email': ['max@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=10)
}

# 1. Download data from Settrade website 
# 2. Check whether data on Settrade has updated since last ingestion
# 3. Save to local storage as csv files
def download_and_save_data_to_storage(
    ti, 
    conn=conn,
    set_50_columns=set_50_columns, 
    overall_market_columns=overall_market_columns, 
    **kwargs):

    # Make a get request to download set 50 data from Settrade website
    response = requests.get('https://www.settrade.com/C13_MarketSummary.jsp?detail=SET50')
    print(response.status_code)
    html = response.text
    
    # Extract stock market date
    soup = BeautifulSoup(html, 'lxml')
    market_date = re.findall(pattern=r'\d{2}/\d{2}/\d{4}', string=str(soup.find("div", {"class": "text-right"})))[0]
    market_date = datetime.datetime.strptime(market_date, '%d/%m/%Y').date()

    # Retrieve a recent date on set_50 table
    recent_date_set_50 = pd.read_sql_query('SELECT MAX(date::date) FROM set_50', conn)
    recent_date_set_50 = recent_date_set_50.iloc[0].values[0]
    # Retrieve a recent date on overall_market table
    recent_date_overall_market = pd.read_sql_query('SELECT MAX(date::date) FROM overall_market', conn)
    recent_date_overall_market = recent_date_overall_market.iloc[0].values[0]

    # Check whether data on Settrade has updated since last ingestion
    if (market_date == recent_date_set_50) | (market_date == recent_date_overall_market):
        raise Exception('Stock data has not been updated or data has ingested')

    # Save table from html to dataframe
    table = pd.read_html(html)
    # set_50 dataframe
    set_50 = table[-3]
    set_50.columns = set_50_columns
    # overall_market dataframe
    overall_market = table[0]
    overall_market.columns = overall_market_columns

    # Add date in dataframe
    set_50.insert(1, 'date', market_date)
    overall_market.insert(1, 'date', market_date)

    # Save set_50 dataframe to a csv file
    set_50_file_name = 'set_50_' + datetime.datetime.strftime(market_date, '%d-%m-%Y') + '.csv'
    set_50_file_path = f'/opt/airflow/data/stocks/set_50/{set_50_file_name}'
    set_50.to_csv(set_50_file_path, index=False)
     # Save overall_market dataframe to a csv file
    overall_market_file_name = 'overall_market_' + datetime.datetime.strftime(market_date, '%d-%m-%Y') + '.csv'
    overall_market_file_path = f'/opt/airflow/data/stocks/overall_market/{overall_market_file_name}'
    overall_market.to_csv(overall_market_file_path, index=False)

    file_paths = {
        'set_50_file_path': set_50_file_path, 
        'set_50_file_name': set_50_file_name, 
        'overall_market_file_path': overall_market_file_path,
        'overall_market_file_name': overall_market_file_name
        }

    # Push file paths for other task to use
    ti.xcom_push(key='file_names', value=file_paths)
    
# Check whether csv files in each column can convert to specific types
def check_data_schema(
    ti, 
    set_50_columns=set_50_columns, 
    overall_market_columns=overall_market_columns, 
    set_50_str=set_50_str, 
    set_50_float=set_50_float,
    overall_market_str=overall_market_str,
    overall_market_float=overall_market_float,
    **kwargs):

    # Pull file paths
    file_paths = ti.xcom_pull(key='file_names', task_ids=['download_and_save_data_to_storage'])[0]
    set_50_file_name = file_paths['set_50_file_name']
    overall_market_file_name = file_paths['overall_market_file_name']

    # Read set_50 and overall_market files
    set_50 = pd.read_csv(file_paths['set_50_file_path'])
    overall_market = pd.read_csv(file_paths['overall_market_file_path'])

    # Convert each column to specific data type. If it fails, data engineer needs to review and fix manually.
    try:
        set_50[set_50_str] = set_50[set_50_str].astype(str)
        set_50[set_50_float] = set_50[set_50_float].astype(float)
        set_50['date'] = pd.to_datetime(set_50['date'])
        overall_market[overall_market_str] = overall_market[overall_market_str].astype(str)
        overall_market[overall_market_float] = overall_market[overall_market_float].astype(float)
        overall_market['date'] = pd.to_datetime(overall_market['date'])
    except:
        set_50.to_csv(f'/opt/airflow/fail/{set_50_file_name}')
        overall_market.to_csv(f'/opt/airflow/fail/{overall_market_file_name}')
        raise Exception('Type convertion Error')

# Load set_50 data to postgres
def load_set_50_to_pg(
    ti, 
    conn = conn,
    cur = cur,
    set_50_columns = set_50_columns, 
    set_50_str = set_50_str, 
    set_50_float = set_50_float,
    data_date = data_date,
    **kwargs):

    # Pull file paths
    file_paths = ti.xcom_pull(key='file_names', task_ids=['download_and_save_data_to_storage'])[0]

    # Read set_50 and overall_market files
    set_50 = pd.read_csv(file_paths['set_50_file_path'])

    # Convert each column to specific data type. 
    set_50[set_50_str] = set_50[set_50_str].astype(str)
    set_50[set_50_float] = set_50[set_50_float].astype(float)
    set_50['date'] = pd.to_datetime(set_50['date'])
    set_50['ingestion_timestamp'] = pd.to_datetime(data_date)

    print(set_50.head())

    # Load set_50 to postgres
    for index, row in set_50.iterrows():
        cur.execute('''
            INSERT INTO set_50 (name, date, open, high, low, last, change, percent_change, bid, offer, volumn_shares, value_k_baht, ingestion_timestamp)
            VALUES (%(name)s,%(date)s, %(open)s, %(high)s, %(low)s, %(last)s, %(change)s, %(percent_change)s, %(bid)s, %(offer)s, %(volumn_shares)s, %(value_k_baht)s, %(ingestion_timestamp)s)
        ''', {
                'name': row['name'], 
                'date': row['date'],
                'open': row['open'], 
                'high': row['high'], 
                'low': row['low'], 
                'last': row['last'], 
                'change': row['change'], 
                'percent_change': row['percent_change'],
                'bid': row['bid'],
                'offer': row['offer'],
                'volumn_shares': row['volumn_shares'],
                'value_k_baht': row['value_k_baht'],
                'ingestion_timestamp': row['ingestion_timestamp']
            })

    conn.commit()

# Load overall_market data to postgres
def load_overall_market_to_pg(
    ti, 
    conn = conn,
    cur = cur,
    overall_market_columns = overall_market_columns, 
    overall_market_str = overall_market_str,
    overall_market_float = overall_market_float,
    data_date = data_date,
    **kwargs):

    # Pull file paths
    file_paths = ti.xcom_pull(key='file_names', task_ids=['download_and_save_data_to_storage'])[0]

    # Read set_50 and overall_market files
    overall_market = pd.read_csv(file_paths['overall_market_file_path'])

    print(file_paths)
    print(file_paths['overall_market_file_path'])

    # Convert each column to specific data type. 
    overall_market[overall_market_str] = overall_market[overall_market_str].astype(str)
    overall_market[overall_market_float] = overall_market[overall_market_float].astype(float)
    overall_market['date'] = pd.to_datetime(overall_market['date'])
    overall_market['ingestion_timestamp'] = pd.to_datetime(data_date)

    print(overall_market.head())

    for index, row in overall_market.iterrows():
        cur.execute('''
            INSERT INTO overall_market (name, date, last, change, percent_change, high, low, volumn_k, value_m_baht, ingestion_timestamp)
            VALUES (%(name)s, %(date)s, %(last)s, %(change)s, %(percent_change)s, %(high)s, %(low)s, %(volumn_k)s, %(value_m_baht)s, %(ingestion_timestamp)s)
        ''', {
                'name': row['name'], 
                'date': row['date'],
                'last': row['last'], 
                'change': row['change'], 
                'percent_change': row['percent_change'], 
                'high': row['high'], 
                'low': row['low'], 
                'volumn_k': row['volumn_k'], 
                'value_m_baht': row['value_m_baht'],
                'ingestion_timestamp': row['ingestion_timestamp']
            })

    conn.commit()

with DAG(
    'set_50_pipeline',
    default_args=default_args,
    description='set 50 data pipeline',
    schedule_interval='0 19 * * 1-5',
    start_date=datetime.datetime(2021, 5, 29, tzinfo=bangkok_tz)
) as dag:

    download_and_save_data_to_storage_task = PythonOperator(
        task_id = 'download_and_save_data_to_storage',
        python_callable = download_and_save_data_to_storage,
        provide_context = True,
        op_kwargs={
            'conn': conn,
            'set_50_columns': set_50_columns,
            'overall_market_columns': overall_market_columns
        },
        dag = dag
    )

    check_data_schema_task = PythonOperator(
        task_id = 'check_data_schema',
        python_callable = check_data_schema,
        provide_context = True,
        op_kwargs={
            'set_50_columns': set_50_columns,
            'overall_market_columns': overall_market_columns,
            'set_50_str': set_50_str, 
            'set_50_float': set_50_float,
            'overall_market_str': overall_market_str,
            'overall_market_float': overall_market_float,
        },
        dag = dag
    )

    load_set_50_to_pg_task = PythonOperator(
        task_id = 'load_set_50_to_pg',
        python_callable = load_set_50_to_pg,
        provide_context = True,
        op_kwargs={
            'conn': conn,
            'cur': cur,
            'set_50_columns': set_50_columns,
            'set_50_str': set_50_str, 
            'set_50_float': set_50_float,
            'data_date': data_date
        },
        dag = dag
    )

    load_overall_market_to_pg_task = PythonOperator(
        task_id = 'load_overall_market_to_pg',
        python_callable = load_overall_market_to_pg,
        provide_context = True,
        op_kwargs={
            'conn': conn,
            'cur': cur,
            'overall_market_columns': overall_market_columns,
            'overall_market_str': overall_market_str, 
            'overall_market_float': overall_market_float,
            'data_date': data_date
        },
        dag = dag
    )


    download_and_save_data_to_storage_task >> check_data_schema_task
    check_data_schema_task >> load_set_50_to_pg_task
    check_data_schema_task >> load_overall_market_to_pg_task