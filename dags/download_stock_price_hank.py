# 這份文件放在 ~/airflow/dags/download_stock_price.py

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""
# [START tutorial]
# [START import_module]
from datetime import datetime, timedelta
from textwrap import dedent
import pendulum
import yfinance as yf
import os
import pandas as pd

# sql
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pymysql
import pandas as pd

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.email import EmailOperator

# [END import_module]

# 版本一： 只試一個 ticker
# def download_prices():
#     ticker = "MSFT"
#     msft = yf.Ticker(ticker)
#     hist = msft.history(period="max")
#     print(type(hist))
#     print(hist.shape)
#     print(hist)

#     print(os.getcwd())
#     with open(f'/Users/hanklee/airflow/logs/{ticker}.csv', 'w') as writer:
#         hist.to_csv(writer, index=True)

# 版本二： 試多個 ticker
# def download_prices():
#     stock_list = ["IBM", "GE", "AAPL", "MSFT"]
#     for ticker in stock_list:
#         msft = yf.Ticker(ticker)
#         hist = msft.history(period="1mo")
#         with open(f'/Users/hanklee/airflow/logs/{ticker}.csv', 'w') as writer:
#             hist.to_csv(writer, index=True)

# 版本三： 在 airflow UI 上 定義 variable，再去取
# def download_prices():
#     # stock_list_str = Variable.get("stock_list")
#     # print(stock_list_str)
#     stock_list_json = Variable.get("stock_list_json", deserialize_json=True)
#     print(stock_list_json)

#     for ticker in stock_list_json:
#         msft = yf.Ticker(ticker)
#         print("I'm here")
#         hist = msft.history(period="max")
#         print(hist)
#         # with open(f'/Users/hanklee/airflow/logs/{ticker}.csv', 'w') as writer:
#         #     hist.to_csv(writer, index=True)
#         print("Finished downloading price data for " + ticker)

# 版本四： 在 airflow UI 上，定義 context config，並使用 trigger by config
# def download_prices(**context):
#     stock_list_json = Variable.get("stock_list_json", deserialize_json=True)
#     stocks = context["dag_run"].conf.get("stocks")
#     print(stocks)
#     if stocks:
#         stock_list = stocks
#     for ticker in stock_list_json:
#         msft = yf.Ticker(ticker)
#         print("I'm here")
#         hist = msft.history(period="1d")
#         with open(f'/Users/hanklee/airflow/logs/{ticker}.csv', 'w') as writer:
#             hist.to_csv(writer, index=True)
#         print("Finished downloading price data for " + ticker)

# 版本五： mac 一直 error，我只好手動下載虛晃一招


def download_prices(**context):
    stock_list_json = Variable.get("stock_list_json", deserialize_json=True)
    stocks = context["dag_run"].conf.get("stocks")
    print(stocks)
    if stocks:
        stock_list = stocks
    for ticker in stock_list_json:
        msft = yf.Ticker(ticker)
        # hist = msft.history(period="1d")
        # with open(f'/Users/hanklee/airflow/logs/{ticker}.csv', 'w') as writer:
        #     hist.to_csv(writer, index=True)
        print("Finished downloading price data for " + ticker)
    valid_tickers = ["IBM", "GE", "AAPL", "MSFT"]
    return valid_tickers


def get_tickers(context):
    stock_list = Variable.get("stock_list_json", deserialize_json=True)

    stocks = context["dag_run"].conf.get("stocks", None) if (
        "dag_run" in context and context["dag_run"] is not None) else None

    if stocks:
        stock_list = stocks
    return stock_list


def get_file_path(ticker):
    # NOT SAVE in distributed system.
    return f"/Users/hanklee/airflow/logs/{ticker}.csv"


def load_price_data(ticker):
    with open(get_file_path(ticker), 'r') as reader:
        lines = reader.readlines()
        return [[ticker]+line.split(',')[:5] for line in lines if line[:4] != 'Date']


def load_price_data_hank(ticker):
    temp = pd.read_csv(get_file_path(ticker))
    temp["ticker"] = ticker

    rename_dict = {
        "Date": "as_of_date",
        "Open": "open_price",
        "High": "high_price",
        "Low": "low_price",
        "Close": "close_price"
    }

    temp = temp.rename(columns=rename_dict)
    final_df = temp.loc[:, ["ticker", "as_of_date", "open_price",
                            "high_price", "low_price", "close_price"]]
    return final_df


def save_to_mysql_stage(*args, **context):
    # tickers = get_tickers(context)
    # 'ti' 是 task instance 的縮寫，所以就照寫就好
    tickers = context['ti'].xcom_pull(task_ids='download_prices')

    # 用明文來連接
    # engine = create_engine(
    #     "mysql+pymysql://root:my-secret-pw@localhost:3307/demodb")
    # conn = engine.connect()

    # 用 connector 來連接(帳密等資訊都存在 airflow webserver, 見最下面)
    from airflow.hooks.base_hook import BaseHook
    db_info = BaseHook.get_connection('demodb')
    # 這邊開始，就是在 Admin/Connections 下，設定 demodb 時，所一一對應的 login, password, host, port, schema
    username = db_info.login
    password = db_info.password
    host = db_info.host
    port = db_info.port
    dbname = db_info.schema
    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{host}:{port}/{dbname}")

    for ticker in tickers:
        val_df = load_price_data_hank(ticker)
        print(f"{ticker} shape={val_df.shape[0]}   {val_df.shape[1]}")

        val_df.to_sql(
            name="stock_prices_stage",  # table name
            con=engine,
            if_exists="append",  # 'fail', 'replace' 就是 overwrite, 'append'
            index=False
        )
        print(f"{len(val_df)} record inserted.")


def save_to_mysql():

    # 用明文來連接
    engine = create_engine(
        "mysql+pymysql://root:my-secret-pw@localhost:3307/demodb")
    conn = engine.connect()

    for ticker in tickers:
        val_df = load_price_data_hank(ticker)
        print(f"{ticker} shape={val_df.shape[0]}   {val_df.shape[1]}")

        val_df.to_sql(
            name="stock_prices_stage",  # table name
            con=engine,
            if_exists="append",  # 'fail', 'replace' 就是 overwrite, 'append'
            index=False
        )
        print(f"{len(val_df)} record inserted.")


# [START instantiate_dag]
with DAG(
    dag_id='Download_Stock_Price',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['ckshoupon@hotmail.com'],
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=30),  # minutes=5
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    # [END default_args]
    description='A simple tutorial DAG',
    schedule_interval='5 5 * * *',  # 每天的 5:05 要執行 # timedelta(days=1)
    start_date=pendulum.today('local').add(days=-2),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,  # 假如有幾次 dag 的 schedule 錯過了，要不要回去補跑？
    max_active_runs=1,  # 重要, 尤其有和 DB 連動時。設成 1，那
    tags=['hankdata'],
) as dag:
    # [END instantiate_dag]
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    download_task = PythonOperator(
        task_id="download_prices",
        python_callable=download_prices,
        provide_context=True
    )
    save_to_mysql_task = PythonOperator(
        task_id='save_to_database',
        python_callable=save_to_mysql_stage,
    )
    mysql_task = MySqlOperator(
        task_id='merge_stock_price',
        mysql_conn_id='demodb',
        sql='merge_stock_price.sql',  # 這邊可以寫 sql 語句
        dag=dag,
    )
    email_task = EmailOperator(
        task_id='send_email',
        to='ckshoupon@hotmail.com',
        subject='Stock Price is downloaded - {{ds}}',
        html_content=""" <h3>Email Test</h3> {{ ds_nodash }}<br/>{{ dag }}<br/>{{ conf }}<br/>{{ next_ds }}<br/>{{ yesterday_ds }}<br/>{{ tomorrow_ds }}<br/>{{ execution_date }}<br/>""",
        dag=dag
    )
    download_task >> save_to_mysql_task >> mysql_task >> email_task


# [END tutorial]


# 在 Admin/Variable 裡面，新增
# stock_list = IBM GE MSFT AAPL
# stock_list_json = ["IBM", "GE", "AAPL", "MSFT", "FB"]
# 之後，在 python 的 function 中，可用 Variable.get() 來取得
# stock_list = Variable.get("stock_list_json", deserialize_json=True)


# triger 時，可以臨時加參數進去，使用時機是你突然想做個實驗，去 overwrite 原本的做法
# 那方法就是，按下 tirger with config 裡面，新增 {"stocks":["FB"]}
# 然後，在 python 的 function 中，可以這樣取得結果：
# stocks = context["dag_run"].conf.get("stocks", None) if (
#         "dag_run" in context and context["dag_run"] is not None) else None

# 在 UI 的 Admin/Connections 可以選 MySQL，
# 然後 connection id 可以自己隨便寫，例如 airflow_db
# connection type 選 MySQL
# Host 選 127.0.0.1
# Schema 就是 database 的意思，所以選 demodb
# login 和 password 就寫進去


# XComs的用法: XComs 是 Cross Communication 的縮寫
# 最簡單的，就是上一個 task 是 python operator 時，那個 python function 可以 return 東西
# 例如 return 一個 valid_tickers 的 list
# 那他的下家，可以用 tickers = context['ti'].xcom_pull(task_ids="上家id")，來取得他 return 的結果

# 講了

# mail 設定，在 airflow.cfg 的裡面，改這一段
# [smtp]

# # If you want airflow to send emails on retries, failure, and you want to use
# # the airflow.utils.email.send_email_smtp function, you have to configure an
# # smtp server here
# smtp_host = smtp.gmail.com
# smtp_starttls = False
# smtp_ssl = True
# # Example: smtp_user = airflow
# smtp_user = ckshoupon@gmail.com
# # Example: smtp_password = airflow
# smtp_password = 12345678
# smtp_port = 465 # 465 對應 smtp_ssl = True, 587 對應 smtp_starttls = True
# smtp_mail_from = airflow@example.com
# smtp_timeout = 30
# smtp_retry_limit = 5


# Macro 是 存系統變數的地方
# 去 Macros reference 的地方看
# {{ ds }} 為 execution date

# BashOperator 可以這樣寫
# my_command = """
#     python xxx.py {{params.my_param}}
# """
# start = BashOperator(
#     task_id = 'start',
#     bash_command = start_command,
#     params = {'my_param': 'hahaha'}
# )

# PythonOperator 可以這樣寫
# crawl_page = PythonOperator(
#     task_id = 'crawl_page',
#     python_callable=crawl_page_links,
#     op_args=['car', "{{ds}}", "{{dag.default_args['start_date_str']}}"],
#     provide_context=True
# )
# def crawl_page_links(forum, current_date, start_date_str):
#     print("whatever~~")

# pip install -U 'apache-airflow[mysql]'
# pip install -U apache-airflow-providers-mysql

# 帳號 admin
# 密碼 FQC9B5GwEkV57mDU
# echo “export AIRFLOW_HOME=~/airflow” >> ~/.bashrc

# shut down
# ps aux | grep airflow-webserver
# 找到 master [airflow-webserver] 對應的 pid (e.g. 30632)，kill掉他
# kill -9 30632
