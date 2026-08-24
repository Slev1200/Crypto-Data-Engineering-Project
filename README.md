# Crypto Market Intelligence Data Engineering Project

## Overview

I built this project to get hands-on experience designing an end-to-end data engineering pipeline using both batch and streaming processing.

The pipeline collects cryptocurrency price data from the CoinGecko API, sends the events through Kafka, stores the raw data in PostgreSQL, and then uses Spark to transform the data through Bronze, Silver, and Gold layers.

I also built a separate streaming path using Flink that continuously consumes cryptocurrency price events from Kafka and calculates 5-minute price metrics.

The main goal of the project was not to use as many technologies as possible, but to understand where each tool fits in a data pipeline and why I would use it.

## Architecture

```text
                         CoinGecko API
                              |
                              v
                        Python Producer
                              |
                              v
                            Kafka
                           /     \
                          /       \
                         v         v
              PostgreSQL Raw     Flink
                    |              |
                    v              v
                 Bronze       5-Minute Price
                    |          Window Metrics
                    v              |
                 PySpark            v
                    |          PostgreSQL
                    v
                 Silver
                    |
                    v
                 PySpark
                    |
                    v
                  Gold
                    |
                    v
              PostgreSQL
             Analytics Tables
```

The project has two main processing paths:

### Batch Pipeline

```text
CoinGecko → Kafka → PostgreSQL → Bronze → Silver → Gold → PostgreSQL
```

The batch pipeline stores incoming cryptocurrency events and then processes them with PySpark.

The data moves through a medallion architecture:

- **Bronze:** Raw cryptocurrency data
- **Silver:** Cleaned and transformed data
- **Gold:** Aggregated daily cryptocurrency metrics used for analytics

The Gold data is written to a PostgreSQL staging table before being upserted into the final fact table. This allows the pipeline to update existing records instead of creating duplicate daily metrics every time it runs.

### Streaming Pipeline

```text
CoinGecko → Kafka → Flink → PostgreSQL
```

The streaming pipeline runs separately from the batch pipeline.

Flink continuously consumes cryptocurrency price events from Kafka and calculates metrics over 5-minute windows. The results are written to the `fact_price_window_5m` table in PostgreSQL.

This allowed me to work with both batch processing and continuous stream processing in the same project and understand when each approach is useful.

## Technology Stack

### Python

Used for the CoinGecko producer, Kafka/PostgreSQL ingestion, and supporting pipeline scripts.

### Apache Kafka

Used as the event-streaming layer between the cryptocurrency data source and downstream consumers.

Instead of having every part of the pipeline communicate directly with the CoinGecko API, Kafka provides a central stream of price events that multiple consumers can process independently.

### PostgreSQL

Used to store raw ingested data as well as the final batch and streaming analytics tables.

PostgreSQL also allows the project to use staging tables, fact tables, SQL upserts, and data-quality checks.

### Apache Spark / PySpark

Used for batch transformations.

Spark processes the Bronze data, cleans and transforms it into the Silver layer, and then builds aggregated Gold-level daily cryptocurrency metrics.

### Apache Flink

Used for continuous stream processing.

Flink consumes price events directly from Kafka and calculates 5-minute windowed cryptocurrency metrics without waiting for the batch pipeline to run.

### Docker / Docker Compose

Used to run the different parts of the project in containers, including:

- Kafka
- PostgreSQL
- Spark
- Flink JobManager
- Flink TaskManager

Docker Compose allows the services to communicate with each other while keeping the local environment reproducible.

## Data Flow

### 1. Cryptocurrency Ingestion

`coingecko_to_kafka.py` retrieves cryptocurrency price data from the CoinGecko API and publishes the events to the Kafka topic:

```text
crypto.price_ticks
```

### 2. Kafka to PostgreSQL

`kafka_to_postgres.py` consumes the Kafka events and writes them into PostgreSQL.

This creates a persistent copy of the incoming events that can later be used by the batch-processing pipeline.

### 3. Bronze Layer

The Bronze layer keeps the raw ingested cryptocurrency data with minimal transformation.

The purpose of this layer is to preserve the original data before performing additional cleaning or aggregation.

### 4. Silver Layer

PySpark reads the Bronze data and produces a cleaned Silver dataset.

This layer prepares the data for downstream analytics and aggregation.

### 5. Gold Layer

PySpark aggregates the Silver data into daily cryptocurrency metrics.

The Gold dataset is then written into the PostgreSQL staging table:

```text
stg_daily_coin_metrics
```

SQL upsert logic moves the data into the final analytics fact table:

```text
fact_daily_coin_metrics
```

Using an upsert allows the pipeline to be rerun without creating duplicate records for the same cryptocurrency and date.

### 6. Real-Time Flink Processing

Flink consumes the same Kafka cryptocurrency events independently of the batch pipeline.

It groups incoming events into 5-minute windows and writes the calculated metrics into:

```text
fact_price_window_5m
```

This provides a continuously updated view of short-term cryptocurrency price activity.

## Data Model

Some of the main PostgreSQL tables used in the project include:

### Raw Data

Stores the cryptocurrency events consumed from Kafka.

### `stg_daily_coin_metrics`

Temporary staging table used when loading Gold-level batch metrics into PostgreSQL.

### `fact_daily_coin_metrics`

Stores the final daily cryptocurrency metrics produced by the Spark batch pipeline.

### `fact_price_window_5m`

Stores the 5-minute cryptocurrency metrics produced by the Flink streaming pipeline.

## Data Quality

The project also contains SQL data-quality checks used to validate the data after it moves through the pipeline.

These checks help identify issues such as missing values, duplicate records, and unexpected data before the data is used for analytics.

## Project Structure

```text
Crypto-Data-Engineering-Project/
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── example.env
│
├── sql/
│   ├── 001_create_raw_tables.sql
│   ├── 002_create_dim_tables.sql
│   ├── 003_create_fact_tables.sql
│   ├── 004_upsert_gold_daily.sql
│   ├── 005_create_realtime_metrics.sql
│   ├── analytics_queries.sql
│   ├── data_quality_checks.sql
│   └── init.sql
│
└── src/
    └── producer/
        ├── coingecko_to_kafka.py
        │
        └── consumer/
            ├── kafka_to_postgres.py
            │
            └── spark/
                ├── read_bronze.py
                ├── build_silver.py
                ├── build_daily_coin_metrics.py
                ├── build_gold_daily.py
                ├── write_gold_to_postgres.py
                │
                └── flink/
                    ├── price_window_metrics.py
                    │
                    └── dashboard/
                        └── app.py
```

## Environment Variables

The project uses environment variables for configuration and credentials.

An `example.env` file is included to show the required variables without exposing API keys or passwords.

The actual environment file is excluded from Git using `.gitignore`.

## What I Wanted to Learn From This Project

I built this project because I wanted experience with data engineering and thought this would be a really interesting, engaging, and technically challenging project to try.

I chose not to use an existing dataset because I wanted to understand the full data engineering cycle, which is why I decided to start with an API:

```text
API
→ ingestion
→ event streaming
→ raw storage
→ batch processing
→ data modeling
→ streaming processing
→ analytics tables
```

The project gave me hands-on experience with concepts including:

- Batch vs. streaming pipelines
- Kafka producers and consumers
- Bronze, Silver, and Gold medallion architecture
- PySpark transformations
- PostgreSQL staging and fact tables
- Idempotent SQL upserts
- Flink streaming windows
- Data-quality checks
- Containerized data infrastructure with Docker
- Debugging communication between multiple data systems

## Current Status

The core batch and streaming pipelines are complete and working.

The remaining part of the project focuses on the analytics layer, including:

- Building useful SQL analytics queries
- Creating visualizations from the processed data
- Adding architecture and pipeline visuals to make the project easier to understand

## Future Improvements

Some possible future improvements include:

- Cloud deployment
- Pipeline orchestration and scheduling
- Additional monitoring and error handling
- Additional real-time metrics
- Automated testing for more parts of the pipeline
