import argparse
import os

import pandas as pd

from kaggle.api.kaggle_api_extended import KaggleApi
from kaggle.api_client import ApiClient


def download_kaggle_dataset(dataset: str ='olistbr/brazilian-ecommerce', path: str ='./data/') -> bool:
    """
    Downloads a dataset from kaggle and unzips it.
    
    Parameters
    ----------
    dataset: str
        Name of kaggle dataset in format "owner-name/dataset-name" (default "olistbr/brazilian-ecommerce").
    path: str
        Relative or absolute path to the location, where the dataset is going to be unzipped. 
        The folders in the path will be created automatically if they don't exist.
        (default "./data/")

    Returns
    -------
    Boolean or Exception
    - True when download was successful 
    - Raises error otherwise
    """

    # raises IOError when kaggle.json file wasn't found
    api = KaggleApi(ApiClient())
    api.authenticate()

    # raises ValueError when dataset was not found or zip archive failed to unzip
    # raises OSError when zip archive couldn't be deleted after unzipping it
    api.dataset_download_files(
        dataset=dataset,
        path=path,
        force=True,
        quiet=True,
        unzip=True
    )

    # the download was successful when no error was raised
    return True


def save_orders_as_pq(path: str = './data/', csv_name: str = 'olist_orders_dataset.csv', parquet_name: str = 'orders.parquet') -> bool:
    """
    Converts the orders csv file to a parquet dataset which is partitioned by year.
    The data is partitioned by year to simulate an incremental data load in dbt.
    
    Parameters
    ----------
    path: str
        Relative or absolute path to the location, where the dataset is stored (default "./data/").
    file_name: str
        Name of the csv file, which contains the Olist order data (default "olist_orders_dataset.csv").
    parquet_name: str
        Name of the parquet file, in which the Olist order data should be saved (default "orders.parquet").
    
    Returns
    -------
    Boolean or Exception
    - True when converting data to parquet was successful
    - Raises error otherwise
    """

    full_path_source = os.path.join(path, csv_name)
    full_path_target = os.path.join(path, parquet_name)

    # read data from csv
    df = pd.read_csv(
        full_path_source, 
        dtype={'order_status': 'category'},
        parse_dates=[
            'order_purchase_timestamp', 
            'order_approved_at', 
            'order_delivered_carrier_date', 
            'order_delivered_customer_date', 
            'order_estimated_delivery_date',
        ],
    )

    # add year for partitioning the data
    df['year'] = df['order_purchase_timestamp'].dt.year.astype('uint16')

    # save data as Hive partitioned parquet (pyarrow partitioning flavor)
    df.to_parquet(full_path_target, index=False, partition_cols=['year'], existing_data_behavior='delete_matching')

    # the download was successful when no error was raised 
    return True


def save_order_items_as_pq(
        path: str = './data/', 
        csv_name_items: str = 'olist_order_items_dataset.csv', 
        csv_name_orders: str = 'olist_orders_dataset.csv', 
        parquet_name: str = 'order_items.parquet') -> bool:
    """
    Converts the order items csv file to a parquet dataset which is partitioned by year.
    The data is partitioned by year to simulate an incremental data load in dbt.
    The order_purchase_timestamp from the olist_orders_dataset.csv is taken, to ensure that 
    partitioning of the order items is identical to the orders. Therefore, we join the orders data, 
    because order_purchase_timestamp is not part of the line items. 
    
    Parameters
    ----------
    path: str
        Relative or absolute path to the location, where the dataset is stored (default "./data/").
    csv_name_items: str
        Name of the csv file, which contains the Olist order items data (default "olist_order_items_dataset.csv").
    csv_name_orders: str
        Name of the csv file, which contains the Olist order data (default "olist_order_items_dataset.csv").
    parquet_name: str
        Name of the parquet file, in which the Olist order items data should be saved (default "order_items.parquet").
    
    Returns
    -------
    Boolean or Exception
    - True when converting data to parquet was successful
    - Raises error otherwise
    """

    full_path_source_items = os.path.join(path, csv_name_items)
    full_path_source_orders = os.path.join(path, csv_name_orders)
    full_path_target = os.path.join(path, parquet_name)

    # read data from csv
    df_items = pd.read_csv(
        full_path_source_items, 
        parse_dates=[
            'shipping_limit_date', 
        ],
    )

    # read data from csv
    df_orders = pd.read_csv(
        full_path_source_orders, 
        dtype={'order_status': 'category'},
        parse_dates=[
            'order_purchase_timestamp', 
            'order_approved_at', 
            'order_delivered_carrier_date', 
            'order_delivered_customer_date', 
            'order_estimated_delivery_date',
        ],
    )
    # add purchase year for partitioning the line items based on the order data purchase timestamp
    df_orders['year'] = df_orders['order_purchase_timestamp'].dt.year

    # make left join on order id
    df = df_items.merge(df_orders, how='left', on='order_id')

    # select only the columns from the order items and the year
    df = df[list(df_items.columns) + ['year']]

    # save data as Hive partitioned parquet (pyarrow partitioning flavor)
    df.to_parquet(full_path_target, index=False, partition_cols=['year'], existing_data_behavior='delete_matching')

    # the download was successful when no error was raised 
    return True


def save_customers_as_pq(path: str = './data/', csv_name: str = 'olist_customers_dataset.csv', parquet_name: str = 'customers.parquet') -> bool:
    """
    Converts the customer csv file to a parquet file.
    
    Parameters
    ----------
    path: str
        Relative or absolute path to the location, where the dataset is stored (default "./data/").
    file_name: str
        Name of the csv file, which contains the Olist customer data (default "olist_customers_dataset.csv").
    parquet_name: str
        Name of the parquet file, in which the Olist customer data should be saved (default "customers.parquet").
    
    Returns
    -------
    Boolean or Exception
    - True when converting data to parquet was successful
    - Raises error otherwise
    """

    full_path_source = os.path.join(path, csv_name)
    full_path_target = os.path.join(path, parquet_name)

    # read data from csv
    df = pd.read_csv(full_path_source)

    # save data parquet file
    df.to_parquet(full_path_target, index=False)

    # the download was successful when no error was raised 
    return True


def save_products_as_pq(path: str = './data/', csv_name: str = 'olist_products_dataset.csv', parquet_name: str = 'products.parquet') -> bool:
    """
    Converts the products csv file to a parquet file.
    
    Parameters
    ----------
    path: str
        Relative or absolute path to the location, where the dataset is stored (default "./data/").
    file_name: str
        Name of the csv file, which contains the Olist product data (default "olist_products_dataset.csv").
    parquet_name: str
        Name of the parquet file, in which the Olist product data should be saved (default "products.parquet").
    
    Returns
    -------
    Boolean or Exception
    - True when converting data to parquet was successful
    - Raises error otherwise
    """

    full_path_source = os.path.join(path, csv_name)
    full_path_target = os.path.join(path, parquet_name)

    # read data from csv
    df = pd.read_csv(full_path_source)

    # save data parquet file
    df.to_parquet(full_path_target, index=False)

    # the download was successful when no error was raised 
    return True


if __name__ == '__main__':
    
    # set up argument parser
    parser = argparse.ArgumentParser(
        description="Downloads (of download flag is set) and converts Olist's e-commerce dataset.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # add optional argument to download the data
    parser.add_argument('-d', '--download', default=False, action='store_true', help='Add this flag to download the dataset from kaggle. Otherwise it is expected, that you have downloaded the dataset already.')

    # add optional argument to disable the conversion to parquet (if you only want to download the data)
    parser.add_argument('-n', '--no-conversion', default=False, action='store_true', help='Add this flag to disable the conversion steps to save the dataset as parquet files.')


    # parse arguments
    known_args, additional_args = parser.parse_known_args()
    bool_download = known_args.download
    bool_conversion = not known_args.no_conversion # flip boolean value

    # run data conversion steps
    # this script could be integrated in a workflow orchestration engine like Airflow, Dagster, Prefect, etc with little effort
    if bool_download:
        download_kaggle_dataset()
    
    if bool_conversion:
        save_orders_as_pq()
        save_order_items_as_pq()
        save_customers_as_pq()
        save_products_as_pq()