import pandas as pd
import os
import configparser
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *

from helper import aggregate_temperatures_df


def create_immigration_fact_table(spark, df, output_data):
    """This function creates an country dimension from the immigration and global land temperatures data.

    :param spark: spark session
    :param df: spark dataframe of immigration events
    :param visa_type_df: spark dataframe of global land temperatures data.
    :param output_data: path to write dimension dataframe to
    :return: spark dataframe representing calendar dimension
    """
    # get visa_type dimension
    dim_df = get_visa_type_dimension(spark, output_data)

    # create a view for visa type dimension
    dim_df.createOrReplaceTempView("visa_view")

    # create a udf to convert arrival date in SAS format to datetime object
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

    # rename columns to align with data model
    df = df.withColumnRenamed('ccid', 'record_id') \
        .withColumnRenamed('i94res', 'country_residence_code') \
        .withColumnRenamed('i94addr', 'state_code')

    # create an immigration view
    df.createOrReplaceTempView("immigrations_view")

    # create visa_type key
    df = spark.sql(
        """
        SELECT 
            immigrations_view.*, 
            visa_view.visa_type_key
        FROM immigrations_view
        LEFT JOIN visa_view ON visa_view.visatype=immigrations_view.visatype
        """
    )

    # convert arrival date into datetime object
    df = df.withColumn("arrdate", get_datetime(df.arrdate))

    # drop visatype key
    df = df.drop(df.visatype)

    # write dimension to parquet file
    df.write.parquet(output_data + "immigration_fact", mode="overwrite")

    return df


def create_demographics_dimension_table(demographics_df, output_data):
    """
    Create demographics dimension table

    demographics_df: spark dataframe of us demographics survey data
    output_data: path to write dimension dataframe to
    """
    dim_df = demographics_df.withColumnRenamed('Median Age', 'median_age') \
        .withColumnRenamed('Female Population', 'female_population') \
        .withColumnRenamed('Male Population', 'male_population') \
        .withColumnRenamed('Foreign-born', 'foreign_born') \
        .withColumnRenamed('State Code', 'state_code') \
        .withColumnRenamed('Total Population', 'total_population') \
        .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
        .withColumnRenamed('Average Household Size', 'average_household_size')
    dim_df = dim_df.withColumn('id', monotonically_increasing_id())

    # write to parquet
    dim_df.write.parquet(output_data + "demographics", mode="overwrite")

    return dim_df


def create_visa_type_df(df, output_path):
    """
    Create visa type dimension from immigration data

    df: spark dataframe of immigration data
    output_path
    """
    # create visatype df from visatype column
    visa_type_df = df.select(['visatype']).distinct()

    # create an id
    visa_type_df = visa_type_df.withColumn('visa_type_key', monotonically_increasing_id())

    # write to parquet file
    visa_type_df.write.parquet(output_path + "visatype", mode="overwrite")

    return visa_type_df


def get_visa_type_dimension(spark, output_data):
    return spark.read.parquet(output_data + "visatype")


def create_countries_df(spark, immigrations_df, temperatures_df, output_path, countries_mapping_df):
    """
    Creates a countries dimension and dataframe

    spark: spark session 
    immigrations_df: immigration events dataframe
    temperatures_df: temperatures dataframe
    output_path
    countries_mapping_df: csv file that maps country codes to country names
    """
    # view for immigrations
    immigrations_df.createOrReplaceTempView("immigrations_view")

    # view for countries mapping
    countries_mapping_df.createOrReplaceTempView("countries_mapping_view")

    # view for countries average temperatures
    aggregated_temperatures_df = aggregate_temperatures_df(temperatures_df)
    aggregated_temperatures_df.createOrReplaceTempView("average_temperatures_view")

    countries_df = spark.sql(
        """
        SELECT 
            i94res as country_code,
            Name as country_name
        FROM immigrations_view
        LEFT JOIN countries_mapping_view
        ON immigrations_view.i94res=countries_mapping_view.code
        """
    ).distinct()

    # create temp country view
    countries_df.createOrReplaceTempView("countries_view")

    countries_df = spark.sql(
        """
        SELECT 
            country_code,
            country_name,
            average_temperature
        FROM countries_view
        LEFT JOIN average_temperatures_view
        ON countries_view.country_name=average_temperatures_view.Country
        """
    ).distinct()

    # write to a parquet file
    countries_df.write.parquet(output_path + "country", mode="overwrite")

    return countries_df


def create_immigration_time_df(df, output_path):
    """
    creates an immigration time based on arrival date

    :param df: immigration events dataframe
    :param output_path
    :return: df representing immigration time
    """
    # create a udf to convert arrival date in SAS format to datetime object
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

    # create initial calendar df from arrdate column
    immigration_time_df = df.select(['arrdate']).withColumn("arrdate", get_datetime(df.arrdate)).distinct()

    # creating columns
    immigration_time_df = immigration_time_df.withColumn('arrival_weekday', dayofweek('arrdate'))
    immigration_time_df = immigration_time_df.withColumn('arrival_day', dayofmonth('arrdate'))
    immigration_time_df = immigration_time_df.withColumn('arrival_week', weekofyear('arrdate'))
    immigration_time_df = immigration_time_df.withColumn('arrival_month', month('arrdate'))
    immigration_time_df = immigration_time_df.withColumn('arrival_year', year('arrdate'))
    immigration_time_df = immigration_time_df.withColumn('id', monotonically_increasing_id())

    # write to parquet file
    immigration_time_df.write.parquet(output_path + "immigration_calendar", partitionBy=['arrival_year', 'arrival_month', 'arrival_week'], mode="overwrite")

    return immigration_time_df


# Perform quality checks here
def count_check(df, label):
    """
    Count quality checks on dataframe

    df: dataframe to check its size
    """
    count = df.count()

    if count == 0:
        print(f"Quality check failed for {label} with zero count of records !")
    else:
        print(f"Quality check passed for {label} with {count:,} records.")
    return 0
