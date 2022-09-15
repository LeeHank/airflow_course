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

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# [END import_module]


# def download_prices():
#     ticker = "MSFT"
#     msft = yf.Ticker(ticker)
#     hist = msft.history(period="max")
#     print(type(hist))
#     print(hist.shape)
#     print(hist)

#     print(os.getcwd())
#     with open(f'~/airflow/logs/{ticker}.csv', 'w') as writer:
#         hist.to_csv(writer, index=True)
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

def download_prices(**context):
    stock_list_json = Variable.get("stock_list_json", deserialize_json=True)
    stocks = context["dag_run"].conf.get("stocks")
    print(stocks)
    if stocks:
        stock_list = stocks
    for ticker in stock_list_json:
        msft = yf.Ticker(ticker)
        print("I'm here")
        hist = msft.history(period="max")
        print(hist)
        # with open(f'/Users/hanklee/airflow/logs/{ticker}.csv', 'w') as writer:
        #     hist.to_csv(writer, index=True)
        print("Finished downloading price data for " + ticker)


# [START instantiate_dag]
with DAG(
    dag_id='Download_Stock_Price',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['ckshoupon@hotmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
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
    schedule_interval=timedelta(days=1),
    start_date=pendulum.today('local').add(days=-2),
    catchup=False,
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


# [END tutorial]


# 在 Admin/Variable 裡面，新增
# stock_list = IBM GE MSFT AAPL
# stock_list_json = ["IBM", "GE", "AAPL", "MSFT"]

# tirger with config 裡面，新增 {"stocks":["FB"]}
