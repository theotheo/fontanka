import datetime as dt
import json
import locale
from typing import List

import parsel
import requests
from w3lib.html import replace_tags

locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')

THREAD_URL = 'https://newsapi.fontanka.ru/v1/public/fontanka/services/comments/{}/children?regionId=478&dateFrom=0'

def clear(blocks: List[str]) -> str:
    texts = [replace_tags(block, ' ') for block in blocks]
    return ''.join(texts).strip()

def parse_day_news(html: str) -> List[dict]:
    """Extract day news list from fontanka page
    I.e. https://www.fontanka.ru/2022/01/01/all.html 

    Args:
        html (str): page HTML-text 

    Returns:
        list[dict]: extracted news list
    """

    sel = parsel.Selector(html)
    lis = sel.css('ul.CDcf li')
    date = sel.css('time.CDdt::text').get()
    
    day_news = []
    for li in lis:
        time = li.css('time span::text').get()
        try:
            category = li.xpath('./div[1]/a').attrib['title']
        except:
            category = None

        url = li.css('div:nth-child(2) a').attrib['href']
        title = li.css('div:nth-child(2) a').attrib['title']

        try:
            n_comments = li.css('div:nth-child(2) a:nth-child(3) span::text').get()
        except:
            n_comments = 0

        try:
            comments_url = li.css('div:nth-child(2) a:nth-child(3)').attrib['href']
        except:
            comments_url = None

        datetime = dt.datetime.strptime(f'{date} {time}', "%d.%m.%Y %H:%M")

        item = {
            'title': title,
            'url': url,
            'comments_url': comments_url,
            'n_comments': n_comments ,
            'category': category,
            'datetime': datetime
        }

        day_news.append(item)

    return day_news

def parse_day_news_json(text):
    payload = json.loads(text)
    data = payload['data']

    news_list = []
    for item in data:
        news = {}

        authors = item['authors']
        lead = item['lead']
        urls = item['urls']
        rubrics = item.get('rubrics', [])
        del item['authors']
        del item['lead']
        del item['urls']
        del item['rubrics']
        news = item.copy()
        news['publishAt'] = dt.datetime.strptime(news['publishAt'],  "%d.%m.%Y %H:%M")
        try:
            news['category'] = rubrics['name']
        except:
            news['category'] = None
        news_list.append(news)

    return news_list


def get_children_comments(comment_id):
    r = requests.get(THREAD_URL.format(comment_id))
    comments = []

    if r.status_code == 200:
        payload = r.json()
        data = payload['data']

        for item in data:
            comment = {}
            comment['id'] = item['id']
            comment['datetime'] = dt.datetime.strptime(item['date'], "%d %b %Y в %H:%M")
            comment['text'] = item['decoratedText']
            comment['parent'] = item['directParentId']
            comment['username'] = item['user']['nick']
            comment['user_id'] = item['user']['id']
            comments.append(comment)

    return comments

def parse_comments(html):
    sel = parsel.Selector(html)

    comments_sel = sel.css('div[data-test="comment"]')
    comments = []

    for comment_sel in comments_sel:
        comment = {}
        comment_sel_id = comment_sel.attrib['id'] # например, comment_id_77687011
        comment['id'] = comment_sel_id.split('_')[-1] # берем только числа 
        comment['text'] = comment_sel.css('div::text').get()
        comment['username'] = comment_sel.css('a[data-test="comment-user-nick"]::text').get()

        user_link = comment_sel.css('a[data-test="comment-user-nick"]').attrib['href']
        comment['user_id'] = user_link.split("/")[2] # /users/258125221/

        datetime_str = comment_sel.css('time::text').get()
        comment['datetime'] = dt.datetime.strptime(datetime_str, "%d %b %Y в %H:%M")

        comments.append(comment)

        if comment_sel.css('button[data-test="comment-answers-count"]'):
            children = get_children_comments(comment['id'])
            comments.extend(children)
    
    return comments

