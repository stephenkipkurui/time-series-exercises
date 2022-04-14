import pandas as pd
import numpy as np
from acquire import get_combined_item_stores_sales_data, get_opsd_data
from datetime import timedelta, datetime

import warnings
warnings.filterwarnings("ignore")

from acquire import get_stores_data_from_api, get_opsd_data, get_sales_data_from_api, get_items_data_from_api



def prepare_store_data():
    '''
       This function:
       - Acquires cached store data.
       - Converts date column to datetime format
       - sets index to datetime variable
       - add month and day of week columns
       - add sales_total from sales tables (sale_amount + item_sales)
    '''
    
    df = get_combined_item_stores_sales_data()
    
    # Convert date column to datetime format.
    df.sale_date = df.sale_date.apply(lambda date: date[:-13])
    df.sale_date = pd.to_datetime(df.sale_date, format='%a, %d %b %Y')
    
    # Set the sale_date to be the datetime variable.
    df = df.set_index('sale_date').sort_index()
    
    # Add a 'month' and 'day of week' column to your dataframe.
    df['month'] = df.index.month
    df['day'] = df.index.month
    
    df['sales_total'] = df.sale_amount * df.item_price
    
#     # Drop unnecessary columns
#     df = df.drop(columns = ['Unnamed: 0_x', 'Unnamed: 0_y', 'Unnamed: 0'])

    return df


def prepare_ops_data():
    
    '''
       This function:
       - Acquires cached OPS data.
       - Sets index to be datetime variable
       - Add a month and a year column to your dataframe.
       - Fill any missing values
    '''
    
    df = get_opsd_data()
    
    df.columns = [column.replace('+','_').lower() for column in df]

     # Convert date column to datetime format.
    df['date'] = pd.to_datetime(df['date'])
    
    # Set the date to be the datetime variable.
    df = df.set_index('date').sort_index()
    
     # Add a 'month' and 'day of week' column to your dataframe.
    df['month'] = df.index.month
    df['year'] = df.index.year
    df['day'] = df.index.month
    
    df.fillna(0)
    df['wind_solar'] = df.wind + df.solar

    return df
    
    