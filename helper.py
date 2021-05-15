# Import Libraries
import configparser
import datetime as dt
import os

import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objs as go
import plotly.plotly as py
import requests
import seaborn as sns
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import (avg, col, count, dayofmonth, dayofweek,
                                   isnan, monotonically_increasing_id, month,
                                   udf, weekofyear, when, year)
from pyspark.sql.types import *

requests.packages.urllib3.disable_warnings()


def visualize_missing_values(df):
    """Given a dataframe df, visualize it's missing values by columns

    :param df:
    :return:
    """
    # lets explore missing values per column
    nulls_df = pd.DataFrame(data= df.isnull().sum(), columns=['values'])
    nulls_df = nulls_df.reset_index()
    nulls_df.columns = ['cols', 'values']

    # calculate % missing values
    nulls_df['% missing values'] = 100*nulls_df['values']/df.shape[0]

    plt.rcdefaults()
    plt.figure(figsize=(10,5))
    ax = sns.barplot(x="cols", y="% missing values", data=nulls_df)
    ax.set_ylim(0, 100)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    plt.show()



def visualize_missing_values_spark(df):
    """Visualize missing values in a spark dataframe
    
    :param df: spark dataframe
    """
    # create a dataframe with missing values count per column
    nan_count_df = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).toPandas()
    
    # convert dataframe from wide format to long format
    nan_count_df = pd.melt(nan_count_df, var_name='cols', value_name='values')
    
    # count total records in df
    total = df.count()
    
    # now lets add % missing values column
    nan_count_df['% missing values'] = 100*nan_count_df['values']/total
    
    plt.rcdefaults()
    plt.figure(figsize=(10,5))
    ax = sns.barplot(x="cols", y="% missing values", data=nan_count_df)
    ax.set_ylim(0, 100)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    plt.show()
    


def clean_immigration_df(df):
    """
    Clean immigration dataframe

    df: immigration data
    """
    records_count = df.count()
    
    print(f'Before cleaning records count: {records_count:,}')
    
    # EDA has shown these columns to exhibit over 90% missing values, and hence we drop them
    drop_columns = ['occup', 'entdepu','insnum']
    df = df.drop(*drop_columns)
    
    # drop rows where all elements are missing
    df = df.dropna(how='all')

    records_new_count = df.count()
    
    print(f'After cleaning records count: {records_new_count:,}')
    
    return df


def create_calendar_dim_table(df):
    return df.count()
    


def clean_temperatures_df(df):
    """
    Clean temperatures dataframe
    
    :param df: temperatures dataframe
    """
    records_count = df.count()
    
    print(f'Records count: {records_count:,}')
    
    # drop rows that have no average temperature
    df = df.dropna(subset=['AverageTemperature'])
    
    records_count_after_dropping_nas = df.count()
    print('Count of rows removed after dropping rows with missing AverageTemperature: {:,}'.format(records_count-records_count_after_dropping_nas))
    
    # drop duplicate rows
    df = df.drop_duplicates(subset=['dt', 'City', 'Country'])
    records_count_after_dropping_duplicated_rows = df.count()
    print('Count of rows removed after dropping duplicates: {:,}'.format(records_count_after_dropping_nas - records_count_after_dropping_duplicated_rows))
    
    return df

def aggregate_temperatures_df(temperatures_df):
    """
    Aggregate average temperatures data on country
    
    temperatures_df: spark dataframe of clean global temperaturs data
    """
    df = temperatures_df.select(['Country', 'AverageTemperature']).groupby('Country').avg()
    df = df.withColumnRenamed('avg(AverageTemperature)', 'average_temperature')
    
    return df

def clean_demographics_data(df):
    """Clean the US demographics dataset
    
    :param df: pandas dataframe of US demographics dataset
    :return: clean dataframe
    """
    # drop rows with missing values
    subset_cols = [
        'Male Population',
        'Female Population',
        'Number of Veterans',
        'Foreign-born',
        'Average Household Size'
    ]
    df = df.dropna(subset=subset_cols)
    
    # drop duplicate columns
    df = df.drop_duplicates(subset=['City', 'State', 'State Code', 'Race'])
    
    return df


def clean_demographics_df(df):
    """
    Clean the US demographics dataframe
    
    df: US demographics dataset
    """
    # drop missing values rows
    subset_cols = [
        'Number of Veterans',
        'Male Population',
        'Female Population',
        'Foreign-born',
        'Average Household Size'
    ]
    clean_df = df.dropna(subset=subset_cols)
    
    removed_rows_count = df.count() - clean_df.count()
    print("Dropped rows with missing values count: {}".format(removed_rows_count))
    
    # drop duplicate columns
    clean_df2 = clean_df.dropDuplicates(subset=['City', 'State', 'State Code', 'Race'])
    
    rows_dropped_with_duplicates = clean_df.count() - clean_df2.count()
    print(f"Dropped duplicated rows count: {rows_dropped_with_duplicates}")
    
    return clean_df2



def print_formatted_float(number):
    print('{:,}'.format(number))
