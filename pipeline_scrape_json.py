from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from ploomber import DAG
from ploomber.executors import Serial
from ploomber.products import File
from ploomber.tasks import (DownloadFromURL, NotebookRunner, PythonCallable,
                            TaskGroup)

from tasks.scrape import get_day_news_json


start_date = date(year=2022, month=1, day=1)
end_date = date(year=2022, month=5, day=1)
dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days)]


def make():
    dag = DAG()
    # NOTE: this is only required for testing purpose
    dag.executor = Serial(build_in_subprocess=False)

    def func(product, upstream):
        dfs = []
        for _, file in upstream.items():
            dfs.append(pd.read_csv(file))
        df = pd.concat(dfs)

        pd.DataFrame(df).to_csv(product, index=False)

    combine_task = PythonCallable(func, File('products/all_days.csv'), dag, name='combine_days')

    product_path = './products/reports/eda_news.html'
    eda_task = NotebookRunner(Path('./exploratory/eda_news.py'), File(product_path), dag, name='eda_news1')

    combine_task >> eda_task

    for date in dates:
        
        url = f'https://newsapi.fontanka.ru/v1/public/fontanka/services/archive/?regionId=478&page=1&pagesize=500&date={date}&rubricId=all'
        name = f'download_day_{date}'
        product_path = f'products/raw/days/{date}.json' 

        day_task = DownloadFromURL(url, File(product_path), dag, name)

        name = f'parse_day_{date}'
        product_path = f'products/interim/days/{date}.csv'
        day_news_task = PythonCallable(get_day_news_json, File(product_path), dag, name)

        name = f'parse_comments_{date}'

        day_task >> day_news_task >> combine_task

    return dag
