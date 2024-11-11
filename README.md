# Stock Market Data Pipeline

A robust ETL pipeline that extracts, transforms, and loads U.S. stock market data using the Alpha Vantage API, with data warehousing in Amazon Redshift and automated workflows managed by Apache Airflow on an EC2 instance.

## 🚀 Features

- Extracts top 10 US stock market data for the last 25 years using Alpha Vantage API
- Handles stock split adjustments using Yahoo Finance API
- Cloud-based data storage and warehousing using Amazon Redshift
- Automated pipeline orchestration with Apache Airflow
- Custom stored procedures for calculating N-month percentage changes
- Comprehensive data validation and error handling

## 🛠️ Technologies Used

- **Python**: 3.11.10
- **Apache Airflow**: 2.8.1
- **PostgreSQL**: 14.13
- **Cloud Services**: 
  - Amazon Redshift
  - Amazon EC2
- **APIs**:
  - Alpha Vantage
  - Yahoo Finance

## 🏗️ Architecture

                                            EC2 Instance
                                                |
                                          Apache Airflow
                                                |
                                        Orchestrates Pipeline
                                                |
                                                v
    Alpha Vantage API --> Data Extraction --> Data Transformation --> Amazon Cloud
                                                                     |
    Yahoo Finance API --> Split Data Handling                        |
                                                                     |
                                                            Amazon Redshift      
                                                                     |     
                                                        Stock Analysis Procedures  -- > Tableau

## 🔑 Key Challenges & Solutions

### Stock Split Data Handling
One of the major challenges encountered was handling stock split data, particularly noticed while analyzing last three months of stock data in the dashboard. The discrepancy was identified in NVIDIA (NVDA) stock data, where the values didn't align with the actual market performance.

![Screenshot (13)](https://github.com/user-attachments/assets/10775f66-1c72-48e9-9c61-24697e32f573)

**Solution:**
- Implemented Yahoo Finance API integration to fetch stock split ratios
- Developed a robust adjustment mechanism to accurately reflect stock splits
- Ensured data consistency across the entire pipeline

## Overview

### 1. Data Extraction
- Fetches historical data for top 10 US stocks
- Validates data integrity before transformation

### 2. Data Transformation
- Cleanses and standardizes raw stock data
- Adjusts for stock splits using Yahoo Finance data
- Normalizes data structures for warehouse compatibility

### 3. Custom Procedures
- Implemented stored procedures for N-month percentage change calculations
- Enables flexible time-range analysis across multiple stocks
- Optimized for performance with large datasets

### 4. Pipeline Automation
- Orchestrated with Apache Airflow on EC2
- Scheduled data refreshes and transformations
- Comprehensive error handling and notification system

### Prerequisites
```bash
python==3.11.10
apache-airflow==2.8.1
psycopg2==2.9.x  # For PostgreSQL connectivity
```

## 🔄 Pipeline Workflow

1. Daily data extraction from Alpha Vantage
2. Stock split verification with Yahoo Finance
3. Data transformation and validation
4. Loading into Redshift warehouse
5. Automated procedure execution for analytics

## 🚧 Future Enhancements

- **Machine Learning Integration**: 
  - Implement predictive models using 10-year historical data
  - Focus on price prediction and trend analysis

- **Real-Time Data Processing**:
  - Implement web socket connections for real-time market data
  - Add stream processing capabilities using Apache Kafka or AWS Kinesis

## Dashboard

https://public.tableau.com/shared/Q8C93XBQD?:display_count=n&:origin=viz_share_link
