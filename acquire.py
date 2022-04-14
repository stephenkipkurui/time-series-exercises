import pandas as pd
import requests
from requests import *
import os
# from urllib.parse import urlparse
# import urllib.parse

'''

All modules saved cached local file after acquiring from API

'''


def get_items_data_from_api():
    '''
    Acquire items data from api
    '''
    base = 'https://python.zgulde.net/'
    endpoint = 'api/v1/items'
    items = []
    while True:
        url = base + endpoint
        response = requests.get(url)
        data = response.json()
        print(f'\rGetting page {data["payload"]["page"]} of {data["payload"]["max_page"]}: {url}', end='')
        items.extend(data['payload']['items'])
        endpoint = data['payload']['next_page']
        if endpoint is None:
            break
            
    items = pd.DataFrame(items)
    items.to_csv('items.csv')# Save local file
    return items

def get_stores_data_from_api():
    '''
    Acquire stores data from api
    '''
    url = 'https://python.zgulde.net/api/v1/stores'
    response = requests.get(url) 
    data = response.json()
#     max_page = data['payload']['max_page']
    stores= pd.DataFrame(data['payload']['stores'])
    stores = pd.DataFrame(stores)
    stores.to_csv('stores.csv')# Save local file
    return stores

def get_sales_data_from_api():
    
    '''
    Acquire sales data from api
    
    '''
    base_url = 'https://api.data.codeup.com/api/v1/sales?page='
    sales = []
    url = base_url + str(1)
    response = requests.get(url)
    data = response.json()
    max_page = data['payload']['max_page']
    sales.extend(data['payload']['sales'])
    page_range = range(2, max_page + 1)

    for page in page_range:
        url = base_url + str(page)
        print(f'\rFetching page {page}/{max_page} {url}', end='')
        response = requests.get(url)
        data = response.json()
        sales.extend(data['payload']['sales'])
    sales = pd.DataFrame(sales)
    sales.to_csv('sales.csv')# Save local file
    return sales

def get_combined_item_stores_sales_data():
    '''
        Acquire stores, items, and sales data from api. Combines all three functions into one
    '''
    sales = pd.read_csv('sales.csv')
    stores = pd.read_csv('stores.csv')
    items = pd.read_csv('items.csv')

    sales = sales.rename(columns={'store': 'store_id', 'item': 'item_id'})
    df = pd.merge(sales, stores, how='left', on='store_id')
    df = pd.merge(df, items, how='left', on='item_id')
    
    df = df.drop(columns = ['Unnamed: 0_x', 'Unnamed: 0_y', 'Unnamed: 0'])
    return df


def get_opsd_data():
    '''
        Acquire Open Power Systems Data for Germany data from api
    '''
    if os.path.exists('opsd.csv'):
        return pd.read_csv('opsd.csv')
    df = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
    df.to_csv('opsd.csv', index=False)
    return df