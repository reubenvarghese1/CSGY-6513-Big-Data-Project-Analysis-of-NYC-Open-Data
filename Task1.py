import os
import sys
import pyspark
import string
import csv
import json
import statistics
from itertools import combinations
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql import types as Datatype
from pyspark.sql.window import Window
from dateutil.parser import parse
import pandas as pd
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql import Row

import datetime

sc = SparkContext()

spark = SparkSession \
        .builder \
        .appName("Project_Task1") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

if not os.path.exists('JSON_Outputs'):
    os.makedirs('JSON_Outputs')

#dataset_list=os.listdir('/user/hm74/NYCOpenData/')  #give your directory name which has NYCOpenData 
dataset_name=[]    
filename={}
with open('/user/hm74/NYCOpenData/datasets.tsv') as csvfile:    #give 
    reader = csv.reader(csvfile, delimiter='\t')
    for i,row in enumerate(reader):
        filename[row[0]]=row[1]
        dataset_name.append(row[0]+'.tsv.gz')
        
def log(msg):
    print("INFO: " + str(msg))

def check_float(val):
    try:
        float(val)
        return True
    except:
        return False

def is_date(string, fuzzy=False):
    try:
        parse(string, fuzzy=fuzzy)
        return True
    except ValueError:
        return False

def return_data_types(val):
    if str(val).isdigit():
        return "INTEGER (LONG)"
    elif check_float(val):
        return "REAL"
    elif is_date(val):
        return  "DATE/TIME"
    else:
        return "TEXT"

def get_col_name(col_name):
    if "." in col_name:
        return str("`" + col_name + "`")
    return col_name

def type_convert(data_type, val):
    if data_type == "INTEGER (LONG)":
        return int(val)
    if data_type == "REAL":
        return float(val)
    else:
        return str(val)

def process_dataset(filename):
    # filename = "/user/hm74/NYCOpenData/2232-dj5q.tsv.gz" # TODO: Receive as argument from driver script
    log("Started processing - " + filename)
    input_data = spark.read.format('csv').options(header='true', delimiter='\t').load(filename)

    json_file_data = []
    json_column_data = []
    column_list = input_data.columns

    count_nan_vals = input_data.select([count(when(isnan(get_col_name(column_name)), get_col_name(column_name))).alias(column_name) for column_name in column_list])
    count_null_vals = input_data.select([count(when(col(get_col_name(column_name)).isNull(), get_col_name(column_name))).alias(column_name) for column_name in column_list])

    count_non_nan_vals = input_data.select([count(when(~isnan(get_col_name(column_name)), get_col_name(column_name))).alias(column_name) for column_name in column_list])
    count_non_null_vals = input_data.select([count(when(col(get_col_name(column_name)).isNotNull(), get_col_name(column_name))).alias(column_name) for column_name in column_list])

    #input_data_pd = input_data.toPandas()
    #count_distinct_vals = input_data_pd.nunique()
    
    count_distinct_vals = input_data.distinct().count()

    #top_5_frequent_vals = {}
    #for column_name in column_list:
    #    frequency_vals = input_data_pd[column_name].value_counts()
    #    top_5_frequent_vals[column_name] = frequency_vals[:5].index.tolist()

    for column_name in column_list:
        column_data={}
        fin_dict = {}
        fin_dict["column_name"] = column_name
        fin_dict["data_types"] = []
        d = input_data.rdd.map(lambda x: x[column_name])
        d = d.filter(lambda x: x!=None)
        m = d.map(lambda x: Row(dt=return_data_types(x), val=x)).toDF()
        #m = m.filter(m.val.isNotNull())
        dt_list = m.select("dt").distinct().collect()
        for i in dt_list:
            dt_dict = {}
            dt = i['dt']
            rdd = m.filter(m.dt==dt).rdd.map(lambda x: type_convert(dt, x['val']))
            dt_dict["type"] = dt
            dt_dict["count"] = int(rdd.count())
            if dt == "TEXT":
                text_RDD = rdd.map(lambda x: (x.lower(), len(x)))
                longest_length = text_RDD.distinct().sortBy(lambda x: x[1]).top(5, key=lambda x: x[1])
                shortest_length = text_RDD.distinct().sortBy(lambda x: x[1]).take(5)
                dt_dict["shortest_values"] = [k[0] for k in shortest_length]
                dt_dict["longest_values"] = [k[0] for k in longest_length]
            if dt == "DATE/TIME":
                dt_dict["max_value"] = str(rdd.max())
                dt_dict["min_value"] = str(rdd.min())
            if dt ==  "INTEGER (LONG)":
                dt_dict["max_value"] = int(rdd.max())
                dt_dict["min_value"] = int(rdd.min())
            if dt == "REAL":
                dt_dict["max_value"] = float(rdd.max()) # Any specific reason why max is int?
                dt_dict["min_value"] = float(rdd.min())
            if dt ==  "INTEGER (LONG)" or  dt == "REAL":
                dt_dict["mean"] = float(rdd.mean())
                dt_dict["stddev"] = float(rdd.stdev())
            fin_dict["data_types"].append(dt_dict)
        column_data["column_name"] = column_name
        column_data["number_non_empty_cells"] = int(count_non_null_vals.collect()[0][column_name])
        column_data["number_empty_cells"] = int(count_null_vals.collect()[0][column_name])
        column_data["number_distinct_values"] = int(d.distinct().count())
        freq_val = d.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], ascending=False).take(5)
        column_data["frequent_values"] = [v[0] for v in freq_val]
        column_data["data_types"] = fin_dict["data_types"]
        json_column_data.append(column_data)

    output_data = {}
    output_data["dataset_name"] = filename
    output_data["columns"] = column_list
    output_data["key_column_candidates"] = []

    json_file_data.append(output_data)
    json_file_data.append(json_column_data)

    json_data = json.dumps(json_file_data)

    # with open('test.json', 'w') as f:
    #     json.dump(json_file_data, f)

    return json_file_data


final_merged_json = []
#with open('Datasets2.txt', 'r') as f:
 #   dataset_names = f.read().split(", ")


#print(dataset_name)
counter = 0
#dataset_names = ['/user/hm74/NYCOpenData/29bw-z7pj.tsv.gz']

for dataset_name in dataset_name:
    output_json = {}
    output_json = process_dataset(dataset_name)
    final_merged_json.append(output_json)
    log("Processed dataset - " + dataset_name)
    counter += 1
    if counter ==10:
        counter = 0
        with open('task1.json', 'w') as out_file:
            json.dump(final_merged_json, out_file)


with open('task1.json', 'w') as out_file:
    json.dump(final_merged_json, out_file)