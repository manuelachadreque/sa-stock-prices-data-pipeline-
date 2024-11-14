from airflow.decorators import dag,task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from myprovider.notifier import MyNotifier


from astro import sql as aql
from astro.files import File
from astro.sql.table import Table ,Metadata
from datetime import datetime
import requests
from include.stock_market.tasks import _get_stock_prices, upload_to_s3_from_minio,_store_prices,transform_prices,store_transformed_prices



SYMBOLS =['SHP.JO','GFI.JO', 'VDMCY', 'ABG.JO', 'SBK.JO', 'SPP.JO']


# Define the DAG
@dag(
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',  # Runs daily
    catchup=False,               # Prevent running past dates when it starts
    tags=['stock_market']         # Tags for the DAG
)


def stock_market():
    @task.sensor(poke_interval=30, timeout=300, mode='poke')


    # Check if Api is available, return None 
    
    def is_api_available()->PokeReturnValue:
        api=BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json().get('finance', {}).get('result') is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    

    # get stock prices

    get_stock_prices = PythonOperator(

        task_id='get_stock_prices',
        python_callable =_get_stock_prices,
        op_kwargs={'url': '{{task_instance.xcom_pull(task_ids="is_api_available")}}', 'symbols': SYMBOLS}

    )



    store_prices= PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock':'{{task_instance.xcom_pull(task_ids="get_stock_prices")}}'}

    )
    transform_price = PythonOperator(
    task_id='transform_price',
    python_callable=transform_prices,
    op_kwargs={'stock_prices': '{{ task_instance.xcom_pull(task_ids="get_stock_prices") }}'},
    provide_context=True  # Ensures Airflow provides kwargs for task instance (ti)
)


    store_transformed_price = PythonOperator(
    task_id='store_transformed_price',
    python_callable=store_transformed_prices,
    provide_context=True  # Enables kwargs for XCom and task instance
    )



    # Upload files to aws s3
    store_prices_in_s3 = PythonOperator(
        task_id='store_prices_in_s3',
        python_callable=upload_to_s3_from_minio,
        provide_context=True
    )
    
    
    glue_job_task = GlueJobOperator(
        task_id='run_manuela_sa_stock_market_pipeline',
        job_name='manuela-sa-stock-market-pipeline-Job',  # Name of the Glue job
        script_args=None,
        aws_conn_id='AWSConnection',        
        iam_role_name='manuela-sa-stock-market-pipeline-glue-role',   
        wait_for_completion=True, 
        region_name='us-east-1',             
        job_poll_interval=6,             
    )


    glue_crawler_task = GlueCrawlerOperator(
        task_id='run_manuela_sa_stock_market_pipeline_crawler',
        config={'Name': 'manuela-sa-stock-market-pipeline-glue-crawler'},  
        aws_conn_id='AWSConnection',         
        wait_for_completion=True,  
        region_name='us-east-1',             
        poll_interval=5                   
    )

    is_api_available() >> get_stock_prices >> store_prices>>transform_price>>store_transformed_price>>store_prices_in_s3>>glue_job_task>>glue_crawler_task



    

stock_market()