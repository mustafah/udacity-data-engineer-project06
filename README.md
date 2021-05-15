

# Udacity Data Engineering Project 06

# Capstone Project

We need to create an ETL pipeline for immigration, temperatures and US demographics datasets to be able to make smart analytics on immigration activities and patterns. Like that we can know people from warmer or cold countries prefer to immigrate to US ...

## Structure

Inside `data` folder will be save as the following:

* **`i94.csv`** >> Amazon EMR hdfs filesystem

* **Rest of files** >> S3

The project code will be in the following files:
* **main.py** load from AWS S3, process data using Spark, save result dimensional tables back to S3
* **etl.py and helper.py** - these modules contains the functions for creating fact and dimension tables, data visualizations and cleaning. 
* **config.cfg** configuration for AWS EMR. 
* **notebook.ipynb** - jupyter notebook that was used for building the ETL pipeline.



All the data for this project was loaded into S3 prior to commencing the project. The exception is the i94.csv file which was loaded into Amazon EMR hdfs filesystem.

In addition to the data files, the project workspace includes:

- **etl.py** - reads data from S3, processes that data using Spark, and writes processed data as a set of dimensional tables back to S3
- **etl.py and helper.py** - these modules contains the functions for creating fact and dimension tables, data visualizations and cleaning.
- **config.cfg** - contains configuration that allows the ETL pipeline to access AWS EMR cluster.
- **Jupyter Notebooks** - jupyter notebook that was used for building the ETL pipeline.



## Steps

## Step 1: Scope
We need to do the following, in order to create the analytics database:
* **Load** data into dataframes by using Spark.
* Perform **data cleaning** functions to identify missing values and duplications.
* Create the following *dimension tables*:
    * *Immigrations timing dimension table* from I94 immigration dataset
    * *Countries dimension table* from the I94 immigration and the temperatures dataset.
    * *Demographics dimension table* from the us cities demographics data. This table links to the fact table through the state code field.

## Step 2: Explore and Assess
> Through *notebook.ipynb* for analysis

## Step 3: Model
### 3.1 Data Model

1. *Countries dimension table* is created from *temperatures by city* and the *immigration* data.
   * Allows to study the relation between land temperatures and immigration patterns.

2. *Demographics dimension table* comes from the demographics dataset and links to the immigration fact table at state level.
   * Allow to study immigration patterns based on demographics.

3. *Visa types dimension table* comes from the immigration datasets.

4. *Immigration fact table* is the foundational part of the data model.

## Step 4: Pipeline
Defined in the *main.py*, and you can run it by the following `spark-submit --packages saurfang:spark-sas7bdat:2.0.0-s_2.10 main.py `

The pipeline steps are as follows:

* Load the data
* Clean the Immigrations data
* Create visa_types dimension table
* Create immigration timings dimension table
* Clean temperatures data
* Create countries dimension table
* Create immigrations fact table
* Load demographics data
* Clean demographics data
* Create demographic dimension table

