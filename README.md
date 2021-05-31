# SET 50 Pipeline
------

## Introduction

This goal of this project is to scape SET-50 data (The SET50 is the primary stock indices of Thailand which provide a benchmark of investment in The Stock Exchange of Thailand) from https://www.settrade.com and load it to a database. I main tools for this projects including Airflow as an orchestration and monitoring tool and Postgres as a database.

------

## Pipeline

![](https://raw.githubusercontent.com/maxjunerd/set_50_pipeline/main/pics/data_pipeline.jpg)

1. Scrape 2 sets of data from Settrade which are overall market and every stock in SET-50 
2. Save datasets to local storage
3. Check each column in both datasets whether they have specific data type. If the check fails, data will be saved in fail storage.
4. Overall market and SET-50 data will be saved to PostgreSQL if they pass the check.
5. All of the 4 steps above is orchestrated by Airflow

Monitor data pipeline in Airflow 1

![](https://github.com/maxjunerd/set_50_pipeline/blob/main/pics/pipeline.jpg?raw=true)

Monitor data pipeline in Airflow 2

![](https://github.com/maxjunerd/set_50_pipeline/blob/main/pics/pipeline.jpg?raw=true)

6. Final data is in PostgreSQL

Overall Market Data

![](https://github.com/maxjunerd/set_50_pipeline/blob/main/pics/overall_market_pg.jpg?raw=true)

SET-50 Data

![](https://github.com/maxjunerd/set_50_pipeline/blob/main/pics/set_50_pg.jpg?raw=true)

------

## Requirements

1. Install [Docker Community Edition (CE)](https://docs.docker.com/engine/installation/)

2. Install [Docker Compose](https://docs.docker.com/compose/install/)

   ------

## Setup

1. After you have Docker Community Edition (CE) and Docker Compose installed, download this repository.

2. Build a docker image by opening Terminal and running

   ```bash
   cd . # Change path where you extract this repository
   docker build -t airflow:stocks .
   ```

3. Run Airflow

   ```bash
   docker-compose up
   ```

4. Create a new database named 'stocks' and 2 tables named 'overall_market' and 'set_50'

   ```sql
   create database stocks;
   
   create table overall_market (
   	name varchar, 
   	date date,
   	last float, 
   	change float, 
   	percent_change float, 
   	high float, 
   	low float, 
   	volumn_k float, 
   	value_m_baht float,
   	ingestion_timestamp timestamp
   );
   
   create table set_50 (
   	name varchar, 
   	date date,
   	open float, 
   	high float, 
   	low float, 
   	last float, 
   	change float, 
   	percent_change float, 
   	bid float, 
   	offer float, 
   	volumn_shares float, 
   	value_k_baht float,
   	ingestion_timestamp timestamp
   );
   ```

5. Create a new connection in Airflow

   - Conn Id: postgres
   - Conn Type: Postgres
   - Host: postgres or localhost
   - Schema: stocks
   - Login: airflow
   - Password: airflow
   - Port: 5432

6. Data pipeline (DAG) is ready to run. Data pipeline file is in dags folder.

![](https://github.com/maxjunerd/set_50_pipeline/blob/main/pics/airflow.jpg?raw=true)

