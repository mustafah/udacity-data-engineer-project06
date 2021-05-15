import os
import etl
import helper
import configparser
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import udf, monotonically_increasing_id, from_unixtime, to_timestamp, col, hour, dayofmonth, dayofweek, month, year, weekofyear

# Load configurations
config = configparser.ConfigParser()
config.read('config.cfg')
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

# Globals For quality checks
immigrations_fact_df = None
visa_types_df = None
immigration_time_df = None
temperatures_df = None
countries_df = None

def create_spark():
    spark = SparkSession \
        .builder.config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport().getOrCreate()
    return spark


def process_input_data(spark, input_path, output_path, immigration_file_name, temperature_file_name, countries_mapping_df):
    """
    Process input file and creates fact and dimension tables

    Parameters:
    -----------
    spark (SparkSession): spark session
    input_path (string): input file path
    output_path (string): output file path
    immigration_file_name (string): immigration file name
    temperature_file_name (string): temperatures file name
    countries_mapping_df (pandas dataframe): dataframe that map country codes to names
    """
    # get the file path to the immigration data
    immigration_file = input_path + immigration_file_name

    # load immigration files
    immigrations_df = spark.read.format('com.github.saurfang.sas.spark').load(immigration_file)
    immigrations_df = helper.clean_immigration_df(immigrations_df)

    # create visa_type dimension table & dataframe
    visa_types_df = etl.create_visa_type_df(immigrations_df, output_path)

    # create immigration_time dimension table & dataframe
    immigration_time_df = etl.create_immigration_time_df(immigrations_df, output_path)

    # get temperatures dataframe
    temperatures_df = get_temperatures_df(spark, input_path, temperature_file_name)

    # create countries dimension table & dataframe
    countries_mapping_df = etl.create_countries_df(spark, immigrations_df, temperatures_df, output_path, countries_mapping_df)

    # create immigrations fact table & df
    immigrations_fact_df = etl.create_immigration_fact_table(spark, immigrations_df, output_path)


def process_demographics_data(spark, input_path, output_path, demographics_file_name):
    """Process the demographics data and create the usa_demographics_dim table

    Parameters:
    -----------
    spark (SparkSession): spark session instance
    input_data (string): input file path
    output_data (string): output file path
    demographics_file_name (string): usa demographics csv file name
    """

    # load demographics
    file = input_path + demographics_file_name
    demographics_df = spark.read.csv(file, inferSchema=True, header=True, sep=';')

    # clean
    clean_demographics_df = helper.clean_demographics_df(demographics_df)

    # create dimension table
    etl.create_demographics_dimension_table(clean_demographics_df, output_path)


def get_temperatures_df(spark, input_file_path, file_name):
    """
    Get a dataframe of temperatures

    spark (SparkSession)
    input_file_path (string): input file path
    file_name (string)
    """

    file = input_file_path + file_name
    temperatures_df = spark.read.csv(file, header=True, inferSchema=True)

    # clean
    temperatures_df = helper.clean_temperatures_df(temperatures_df)

    return temperatures_df

def do_quality_check():
    # 1st: Count check
    table_dfs = {
        'Immigrations': immigrations_fact_df,
        'VisaTypes': visa_types_df,
        'Countries': countries_df
    }
    for table_name, table_df in table_dfs.items():
        # quality check for table
        etl.count_check(table_df, table_name)


    # 2nd: Sample integrity check
    etl.count_check(immigrations_fact_df \
                        .join(countries_df, immigrations_fact_df["country_residence_code"] == 
                            countries_df["country_code"], "left_anti"), "Join of ImmigrationCountry")

def main():
    spark = create_spark()
    input_path = "s3://sparkprojectdata/"
    output_path = "s3://sparkprojectdata/"

    immigration_file_name = 'i94_apr16_sub.sas7bdat'
    temperature_file_name = 'GlobalLandTemperaturesByCity.csv'
    countries_mapping_df = spark.read.csv((input_path + "i94.csv"), header=True, inferSchema=True)
    process_input_data(spark, input_path, output_path, immigration_file_name, temperature_file_name, countries_mapping_df)

    demographics_file_name = 'us-cities-demographics.csv'
    process_demographics_data(spark, input_path, output_path, demographics_file_name)

    do_quality_check()

if __name__ == "__main__":
    main()
