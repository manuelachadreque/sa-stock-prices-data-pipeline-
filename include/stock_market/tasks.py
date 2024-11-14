from airflow.hooks.base import BaseHook
from airflow.hooks.S3_hook import S3Hook
import requests
import json
import boto3
from minio import Minio
from io import BytesIO
from airflow.exceptions import AirflowNotFoundException
from minio.error import S3Error
import pandas as pd


# Fetches the stock price data for a specific symbol over a one-year range with daily intervals.

BUCKET_NAME='stock-market'

def _get_stock_prices(url, symbols):
    stock_prices = {}
    
    for symbol in symbols:
        # Construct the URL for each symbol
        symbol_url = f"{url}{symbol}?metrics=high&interval=1d&range=1y"
        api = BaseHook.get_connection('stock_api')
        
        try:
            # Make the request
            response = requests.get(symbol_url, headers=api.extra_dejson['headers'])
            response.raise_for_status()  # Raise an error for bad responses
            
            # Check if the response contains the expected data
            data = response.json()
            if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                stock_prices[symbol] = data['chart']['result'][0]
            else:
                print(f"No data found for {symbol}: {data}")  # Log if no data is found
                stock_prices[symbol] = None  # Or handle it differently as needed
        
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred for {symbol}: {http_err}")  # Log HTTP errors
            stock_prices[symbol] = None
        except Exception as err:
            print(f"An error occurred for {symbol}: {err}")  # Log other errors
            stock_prices[symbol] = None
    
    return json.dumps(stock_prices)



def _get_minio_client():
    # Replace with your MinIO credentials and endpoint
    minio =  BaseHook.get_connection('minio_connection')

    client = Minio(
        endpoint = minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False  # Set to True if using HTTPS
    )
    return client

def _store_prices(stock):
    client =_get_minio_client()


    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    stock = json.loads(stock)

    data = json.dumps(stock, ensure_ascii=False).encode('utf8')

    objw=client.put_object (
        bucket_name=BUCKET_NAME,
        object_name=f'Landing/prices.json',
        data=BytesIO(data),
        length= len(data)
    )

    return f'{objw.bucket_name}'



def transform_prices(stock_prices, **kwargs):
    # Load the JSON data into a DataFrame
    json_stocks = pd.read_json(stock_prices)
    
    # Initialize an empty list to collect normalized DataFrames
    normalized_dfs = []

    # Iterate through each stock symbol in the JSON data
    for symbol in json_stocks.columns:
        # Extract relevant data lists for each field
        timestamp_list = json_stocks[symbol].get('timestamp', [])
        low_list = json_stocks[symbol]['indicators']['quote'][0].get('low', [])
        close_list = json_stocks[symbol]['indicators']['quote'][0].get('close', [])
        volume_list = json_stocks[symbol]['indicators']['quote'][0].get('volume', [])
        high_list = json_stocks[symbol]['indicators']['quote'][0].get('high', [])
        open_list = json_stocks[symbol]['indicators']['quote'][0].get('open', [])

        

        # Create a DataFrame from the lists
        combined_df = pd.DataFrame({
            'symbol': symbol,
            'timestamp': timestamp_list,
            'low': low_list,
            'close': close_list,
            'volume': volume_list,
            'high': high_list,
            'open':open_list
        })

        # Append each symbol's DataFrame to the list
        normalized_dfs.append(combined_df)

    # Concatenate all DataFrames into one
    final_df = pd.concat(normalized_dfs, ignore_index=True)

    # Push the DataFrame as JSON to XCom
    ti = kwargs['ti']  # Airflow task instance to manage XComs
    ti.xcom_push(key='transformed_data', value=final_df.to_json(orient='split'))

    # Optional return for testing purposes
    return final_df


def store_transformed_prices(**kwargs):
    # Retrieve the transformed JSON data from XCom
    ti = kwargs['ti']
    json_data = ti.xcom_pull(key='transformed_data', task_ids='transform_price')
    
    # Convert JSON data back to DataFrame
    final_df = pd.read_json(json_data, orient='split')

    # Initialize MinIO client
    client = _get_minio_client()  # Assuming _get_minio_client() is defined elsewhere

    # Ensure the bucket exists
    BUCKET_NAME = 'stock-market'  # Replace with your actual bucket name
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    # Convert DataFrame to CSV in-memory
    csv_buffer = BytesIO()
    final_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)  # Move to the beginning of the buffer

    # Upload the CSV file to MinIO
    client.put_object(
        bucket_name=BUCKET_NAME,
        object_name='Landing/formatted_prices.csv',  # Path in MinIO
        data=csv_buffer,
        length=csv_buffer.getbuffer().nbytes,
        content_type='text/csv'
    )

    return f"Data stored successfully in {BUCKET_NAME}/Landing/formatted_prices.csv"



"""
Uploads data to an S3 bucket. If the specified bucket doesn't exist, 
it creates the bucket before uploading the data.

Args:
    data (str): Data to upload, represented as a string.
    key (str): The key or path under which to store the data in S3.
    bucket_name (str): Name of the S3 bucket for storage.
"""

def get_data_from_minio(client: Minio, bucket_name: str, object_name: str) -> pd.DataFrame:
    """Retrieve the CSV file from MinIO and return as DataFrame"""
    data = client.get_object(bucket_name, object_name)
    df = pd.read_csv(data)
    return df

def upload_to_s3_from_minio(**kwargs):
    # Initialize MinIO client (assuming _get_minio_client is defined elsewhere)
    client = _get_minio_client()

    BUCKET_NAME = 'stock-market'  # MinIO bucket
    OBJECT_NAME = 'Landing/formatted_prices.csv'  # MinIO object name

    # Retrieve the CSV data from MinIO
    df = get_data_from_minio(client, BUCKET_NAME, OBJECT_NAME)

    # Initialize S3 hook to interact with AWS S3
    hook = S3Hook('AWSConnection')
    AWS_BUCKET_NAME = 'manuela-sa-stock-market-pipeline-raw'  # AWS S3 bucket

    if not hook.check_for_bucket(bucket_name=AWS_BUCKET_NAME):
        hook.create_bucket(bucket_name=AWS_BUCKET_NAME)

    # For each unique symbol in the DataFrame, upload the corresponding data to a different folder in S3
    for symbol in df['symbol'].unique():
        symbol_data = df[df['symbol'] == symbol]

        # Convert the symbol's data to CSV in-memory
        csv_buffer = BytesIO()
        symbol_data.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # Create S3 path for the symbol (separate folder for each symbol)
        s3_key = f'raw/{symbol}/prices.csv'

        # Upload to S3
        hook.load_bytes(bytes_data=csv_buffer.read(), key=s3_key, bucket_name=AWS_BUCKET_NAME, replace=True)

    return f"Data stored in {AWS_BUCKET_NAME}/raw for each symbol"
