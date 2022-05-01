# ---
# jupyter:
#   jupytext:
#     notebook_metadata_filter: ploomber
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
#   ploomber:
#     injected_manually: true
# ---

# %% tags=["parameters"]
upstream = None
product = None

# %% tags=["injected-parameters"]
# Parameters
upstream = {"combine_days_news": "data/processed/news.csv"}
product = "./reports/eda_news.html"


# %%
import pandas as pd
import numpy as np

np.random.seed(0)
pd.set_option("plotting.backend", "pandas_bokeh")
pd.plotting.output_notebook()

# %%
df = pd.read_csv(upstream['combine_days_news'])
df['publishAt'] = pd.to_datetime(df['publishAt'])
df.set_index('publishAt', inplace=True)

USEFUL_COLUMNS = ['commentsCount', 'header', 'category']


# %%
df.sample(10)

# %%
df.resample('1d').count()['id'].plot(title="распределение новостей по дням")

# %%
df.resample('1M').count()['id'].plot(title="распределение новостей по месяцам")

# %%
# 
# df.plot_bokeh(kind="hist", y=['commentsCount'], bins=np.arange(0, 1000, 20), vertical_xlabel=True)
df['commentsCount'].plot(kind='hist', bins=20, title="распределение комментариев", log=True, backend="matplotlib")


# %%
df.sort_values('commentsCount', ascending=False).head(20)[USEFUL_COLUMNS]
# %%
df.resample('1d').sum()['commentsCount'].plot(title="общее число комментариев по дням")

# %%
df['category'].value_counts()

# %%
df.groupby(['category']).mean()['commentsCount'].sort_values(ascending=False)

# %%
df.query('category == "Особое мнение"').sample(5)

# %%
category_by_weeks = df.groupby('category').resample('1w').agg('size').reset_index().set_index('publishAt')
category_by_weeks.rename(columns={0: 'count'}, inplace=True)

category_by_weeks_wide = category_by_weeks.pivot(columns='category',
                   values='count').fillna(0)

category_by_weeks.sample(5)
# %%
category_by_weeks_wide.plot(kind='bar', figsize=(1200,800), stacked=True, vertical_xlabel=True, fontsize_legend=8)
