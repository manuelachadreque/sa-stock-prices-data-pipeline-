# Stock Market data pipeline part I
==============================

## Overview


The data used on this project is available on yahoo finance api(https://query1.finance.yahoo.com/v8/finance/chart/)  we will extract data from a period of 1 year for the following south African Entities. The following fields are relevant to this projects
* **Open Price**:: The price of Apple stock at the market open on that day.
* **Close Price**:: The price of Apple stock at the market close on that day.
* **High Price**: The highest price reached by Apple stock during that trading day.
* **Low Price**:: The lowest price reached by Apple stock during that trading day.
* **Volume**:: The total number of shares of Apple traded on that day.
* **Timestamp**:: The date of the stock market record. Each entry corresponds to a specific trading day
the project documents are available on the repository:


## Project Setup
we will setup the airflow using astronomer cli, astronomer offer a simple and quick way to setup airflow know more from the astronomer ([documentation ](https://www.astronomer.io/docs/astro/cli/overview) ).

before we start setting up apache airflow we need to make sure that docker is running. 

Step 1. Install astronomer cli

**Linux**


```
 # install astro   
    curl -sSL install.astronomer.io | sudo bash -s

 # test the installation
    astro --version
```

Step 2. create the project

```
# create a folder for your project
    mkdir my_projec

# Create Astro project
    astro dev init
```

*astro dev init* - will create the necessary folders for you project inside your project folder

Step 3. Start the apache airflow

```
    astro dev init
```

**Other relevant astro comands:**

* to restart airflow

 when restarting airflow if you enconter timout errors use the second option and adjust the timeout minutes as necessary

```
    astro dev restart 
    or
    astro dev restart wait 5m 
```

* to stop airflow : 
```
astro dev stop
```

Step 3. Setup minio and update requirements file




### Architecture

The project architecture is composed by different layers that come together to compose our final product.

![Screenshot](./resources/architecture.svg)

* Ingestion Layer: The pipeline will connect to a financial API to retrieve daily Apple stock prices.

* Storage Layer: The collected stock price data will be stored locally using Minio, a storage system similar to AWS S3 or Google Cloud Storage, but hosted locally.

* Processing Layer: Once the data is stored in JSON format, it will be processed to transform the data into a more usable format. This will generate a CSV file for further analysis. 

* Orchestration Layer: we use apache airflow for orchestrate the entire pipeline. this inclues using sensors to wait for events, Python functions to process data and aws operators to trigger aws services(aws glue and aws glue crawler).

* Consuption Layer: we use aws Athena for add hoc query and power bi.



### Stack

* **Apache Airflow** - we will use apache airflow are our orchestration too.
* **minio** - local storage where we will store the raw json files extracted from yahoo api.
* **aws s3** - 
* **aws Glue job** -
* **aws Glue crawler**-
* **aws Glue Athena** -
* **Power BI**-




# Dashboard
<iframe width="800" height="600" src="https://app.powerbi.com/view?r=eyJrIjoiODRmZWIzZjktNTRmZS00MTQ3LThlMmUtMThjMmEyMTA4YzQwIiwidCI6ImRmODY3OWNkLWE4MGUtNDVkOC05OWFjLWM4M2VkN2ZmOTVhMCJ9&pageName=92745ff392d64e842279" frameborder="0" allowFullScreen="true"></iframe>
