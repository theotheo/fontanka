from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from pandas_profiling import ProfileReport
from ploomber import DAG
from ploomber.executors import Serial
from ploomber.products import File
from ploomber.tasks import (DownloadFromURL, NotebookRunner, PythonCallable,
                            TaskGroup)

from tasks.scrape import get_day_news_json


start_date = date(year=2022, month=1, day=1)
end_date = date(year=2022, month=5, day=1)
dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days)]


def make(data_dir: str = 'data', artifact_dir: str = 'reports'):
    dag = DAG()
    # NOTE: this is only required for testing purpose
    dag.executor = Serial(build_in_subprocess=False)

    def make_profile(product, upstream):
        df = pd.read_csv(upstream.first)
        profile = ProfileReport(df, title="News Report")
        profile.to_file(product)

    product_path = f'{artifact_dir}/news_profile.html'
    name = 'news_profile'
    profile_task = PythonCallable(make_profile, File(product_path), dag, name)

    def combine_csv(product, upstream):
        dfs = []
        for _, products in upstream.items():
            dfs.append(pd.read_csv(products['news']))
        df = pd.concat(dfs)

        pd.DataFrame(df).to_csv(product, index=False)

    product_path = f'{data_dir}/processed/all_days.csv'
    name = 'combine_days_news'
    combine_task = PythonCallable(combine_csv, File(product_path), dag, name)

    product_path = f'{artifact_dir}/eda_news.html'
    name = 'eda_news'
    eda_task = NotebookRunner(Path('./exploratory/eda_news.py'), File(product_path), dag, name)

    combine_task >> eda_task
    combine_task >> profile_task

    for date in dates:
        
        url = f'https://newsapi.fontanka.ru/v1/public/fontanka/services/archive/?regionId=478&page=1&pagesize=500&date={date}&rubricId=all'
        name = f'download_day_{date}'
        product_path = f'{data_dir}/raw/days/{date}.json' 
        day_task = DownloadFromURL(url, File(product_path), dag, name)

        name = f'parse_day_{date}'
        product_news_path = f'{data_dir}/interim/news/{date}.csv'
        product_author_path = f'{data_dir}/interim/authors/{date}.csv'
        product = {'news': File(product_news_path), 'authors': File(product_author_path)}
        day_news_task = PythonCallable(get_day_news_json, product, dag, name)

        day_task >> day_news_task >> combine_task

    return dag
