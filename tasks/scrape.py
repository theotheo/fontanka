import datetime as dt
import json
import logging
from pathlib import Path

import pandas as pd
import requests

from .parse import parse_comments, parse_day_news_json, parse_day_news


BASE_URL = 'https://www.fontanka.ru'

# def scrape_day_html(product, date):
#     url = f'https://www.fontanka.ru/{date.year}/{date.month}/{date.day}/all.html'
#     r = requests.get(url)
    
#     Path(product).write_bytes(r.content)

# def scrape_day_json(product, date):
#     url = f'https://newsapi.fontanka.ru/v1/public/fontanka/services/archive/?regionId=478&page=1&pagesize=150&date={date}&rubricId=all'
#     r = requests.get(url)
    
#     Path(product).write_bytes(r.content)


def parse_day_html(product, upstream):
    text = Path(upstream['scrape_day_html']).read()
    
def get_day_news_json(product, upstream):
    p = Path(upstream.first)
    json = p.read_text()
    day_news, authors = parse_day_news_json(json)
    pd.DataFrame(day_news).to_csv(product['news'], index=False)
    pd.DataFrame(authors).to_csv(product['authors'], index=False)


def get_day_news(product, upstream):
    p = Path(upstream.first)
    html = p.read_text()
    day_news = parse_day_news(html)
    pd.DataFrame(day_news).to_csv(product, index=False)
    # Path(product).write_text(json.)


def get_comments_html(product, upstream):
    Path(product).mkdir(parents=True, exist_ok=True)
    p = Path(upstream['scrape_comments_html']['htmls'])

    for fn in p.glob('*.html'):
        comments = parse_comments(fn.read_text())
        (Path(product) / fn.with_suffix(".json").name).write_text(json.dumps(comments, default=str))



def scrape_archive(product, theme):
    URL = 'https://newsapi.fontanka.ru/v1/public/fontanka/services/archive/'

    params = {
        'regionId': 478,
        'theme': theme,
        'page': 1,
        'pagesize': 500
    }

    items = []
    links = []

    while(True):
        r = requests.get(URL, params=params)
        logging.info(r.url)

        params['page'] += 1
        payload = r.json()
        error = payload.get('error')
        if error:
            if error == 'Not Found':
                break
            else:
                exit(1)
        else:
            items.extend(payload['data'])

    Path(product).write_text(json.dumps(items))


def scrape_comments_html(product, upstream):
    with open(upstream['scrape_archive']) as f:
        items = json.load(f)

    links = []
    for i, item in enumerate(items):
        id = item.get('id')
        url_comments = item.get('urls').get('urlComments')
        url = f'{BASE_URL}{url_comments}'
        r = requests.get(url)
        logging.info(f'downloaded {r.url}: {r.status_code}')
        links.append({'url': r.url, 'status': r.status_code}) 
        
        dir = Path(product["htmls"])
        dir.mkdir(parents=True, exist_ok=True)
        (dir / f'{id}.html').write_bytes(r.content)

        if i > 20:
            break
    
    pd.DataFrame(links).to_csv(product['links'], index=False)
