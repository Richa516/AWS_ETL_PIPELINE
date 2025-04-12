### 1. Airline Data Ingestion Project

**Project Folder**: `Airline_Data_Ingestion_Project`

**Description**:
This project sets up an AWS Glue-based ETL pipeline that processes airline and airport datasets, transforming them and preparing the cleaned data for insertion into Amazon Redshift. It's ideal for use cases such as flight delay analysis or route performance.

**Contents**:
- `airports.csv` and `flights.csv`: Raw input datasets
- `glue_job.py`: PySpark-based transformation script for AWS Glue
- `redshift_create_table_commands.txt`: SQL commands to create destination tables in Redshift

**Usage**:
1. Upload datasets to S3
2. Deploy and execute Glue job via AWS Console
3. Run Redshift table creation script
4. Load final output to Redshift using COPY or Glue Connector

---

### 2. Incremental Data Load in Glue

**Project Folder**: `Incremental_Data_Load_in_Glue`

**Description**:
Demonstrates how to build an efficient Glue job that only processes new or changed data (incremental loads). Ideal for subscription systems and customer data lakes.

**Contents**:
- Multiple daily `.csv` files simulating incoming subscription data
- `incremental_data_in_glue.py`: Glue job to detect and process delta
- `pyspark_in_glue.py`: Reusable transformation logic

**Usage**:
1. Upload CSVs to S3 
2. Deploy Glue job and set up crawler to create metadata tables in data catalog
3. Write glue job using script to use metadata table from  data catalog and converting into pyspark dataframe.
4. Store final output Redshift

---

### 3. Logistics Data Warehouse Management Project

**Project Folder**: `Logistics_Data_Warehouse_Management_Project`

**Description**:
Orchestrates ingestion of logistics data into a Hive-based data warehouse using Apache Airflow. Automates batch processing workflows for logistics data across multiple dates.

**Contents**:
- Daily `.csv` logistics files
- `hive_load_airflow_dag.py`: Airflow DAG script to automate Hive loads

**Usage**:
1. Place daily data into HDFS or accessible file system
2. Deploy DAG in Airflow and schedule execution
3. Load data into Hive tables
4. Query for reporting/BI

---

### 4. Quality Movie Data Analysis Project

**Project Folder**: `Quality_Movie_Data_Analysis_Project`

**Description**:
Builds a data pipeline to process and analyze movie ratings from IMDb. Uses AWS Glue to clean and transform data, then stores it in Amazon Redshift for dashboarding.

**Contents**:
- `imdb_movies_rating.csv`: Raw dataset
- `movies_quality_ingestion_glue.py`: Glue job script
- `redshift_tables_for_imdb.txt`: Table creation scripts

**Usage**:
1. Upload raw data to S3
2. Execute Glue job
3. Create Redshift tables
4. Load and query for top-rated movies, trends

---

### 5. Sales Data Projection Project

**Project Folder**: `Sales_Data_Projection_Project`

**Description**:
A serverless event-driven architecture to handle real-time or simulated sales data using DynamoDB, Lambda, and Python. Helps track transactions and project sales patterns dynamically.

**Contents**:
- `mock_data_generator_for_dynamodb.py`: Script to generate synthetic data
- `transformation_layer_with_lambda.py`: Lambda function for processing

**Usage**:
1. Generate mock data and insert into DynamoDB
2. Attach Lambda function via stream trigger or API Gateway
3. Transform and output results to DynamoDB/S3 for analysis

