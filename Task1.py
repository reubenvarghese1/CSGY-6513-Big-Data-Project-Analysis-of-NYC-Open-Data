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
import re
from datetime import datetime
import json
import dateutil.parser
import sys
import traceback
import datetime

sc = SparkContext()

spark = SparkSession \
        .builder \
        .appName("Project_Task1") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

if not os.path.exists('JSON_Outputs'):
    os.makedirs('JSON_Outputs')             # If we want to write the output into separate forlder we can use this code and change the location of the output file accordingly, else not needed.


dataset_list=os.listdir('/home/slk522/NYCOpenData/')  #give your directory name which has NYCOpenData, scan the list of directories that lie under the given folder        

final_merged_json = []
      
        
        
def log(message):                               #loggiing is one of the best methods to debug, we need to use log for efficient warnings management
    print("Log_Message: " + str(message))





def check_float(val):                      #we check whether this valus is float or not while , we may be sometimes given an string input, so we are typecasting it and checking 
    try:
        float(val)
        return True
    except:
        return False



def check_date(Datatype): # checking date is often difficult with regular expressions, so for efficient usage, we can use the fuzzy logics to determine the given is date or not 
    try:
        z=parse(Datatype)
        return str(z)
    except:
        return None






def return_data_types(val):              #While doing data profiling, we try type casting with every possible dataset and check its compatibility. This will help us to do initial phase in data profiling
    if str(val).isdigit():
        return "INTEGER (LONG)"
    elif check_float(val):               #checking the float or not
        return "REAL"
    elif check_date(val):                  #passing the input to check_date function
        return  "DATE/TIME"
    else:
        return "TEXT"                  #if it is not the above datasets, it must be the text input










def fetch_col_name(col_name):             #Next part of data profiling is we recurse through every column and fetch the basic attributes of the column.
    if "." in col_name:
        return str("`" + col_name + "`")
    return col_name




def type_cast(data_type, val):       #This is just a function which we can use while type casting to do with rdd
    if data_type == "INTEGER (LONG)":
        return int(val)
    if data_type == "REAL":
        return float(val)
    else:
        return str(val)





def data_profile(filename):         #the data profiling function starts from here. The file name will be passed 
    
    log("Started processing - " + filename)  #we can log files if we want, this is an optional step
    filename=str(filename)
    filename='/user/hm74/NYCOpenData/'+filename.lower()
    input_data = spark.read.format('csv').options(header='true',inferschema='true').option("delimiter", "\t").load(filename)   #we created a dataframe that draw the input from the filename
    json_file_data = []
    json_column_data = []      
    
    column_list = input_data.columns    #we make a list of columns through which we can recurse
    count_nan_vals = input_data.select([count(when(isnan(fetch_col_name(column_name)), fetch_col_name(column_name))).alias(column_name) for column_name in column_list]) #Since, it's data profiling, we filter out initial "Not A Number"(NAN) values  
    count_null_vals = input_data.select([count(when(col(fetch_col_name(column_name)).isNull(), fetch_col_name(column_name))).alias(column_name) for column_name in column_list]) #we filter out all the null values in the dataset
    count_non_nan_vals = input_data.select([count(when(~isnan(fetch_col_name(column_name)), fetch_col_name(column_name))).alias(column_name) for column_name in column_list]) #the remaining non-nan values will be calculated using this
    count_non_null_vals = input_data.select([count(when(col(fetch_col_name(column_name)).isNotNull(), fetch_col_name(column_name))).alias(column_name) for column_name in column_list]) #no. of null values can be calculated using this 
    count_distinct_vals = input_data.distinct().count()  #at last, out of the non valid values, we now find the distinct values in each column
    #Till now, we have the functions that can be executed without going through much of the column, The other functions, we can easily recurse through the columns
    for column_name in column_list:    
        print(column_name+"column is being processed")
        column_data={}         
        fin_dict = {}
        fin_dict["column_name"] = column_name
        fin_dict["data_types"] = []
        data_rdd = input_data.rdd.map(lambda x: x[column_name]) #Trying to convert dataframe into an rdd since I believe rdd is better in handling both memory as well as time 
        data_rdd = data_rdd.filter(lambda x: x!=None)    #rdd fetches distinct values
        if(data_rdd.count())==0:
            continue
        #x = x.filter(x.val.isNotNull())
        distinct_values = data_rdd.map(lambda x: Row(data_type=return_data_types(x), val=x)).toDF()  #
        dt_list = distinct_values.select("data_type").distinct().collect()    #we get the rdd values into a list of values
        for i in dt_list:        
            dt_dict = {} #From here, since we are using rdd, we format using the values of rdd such that it matches the output metadata
            data_type = i['data_type'] 
            rdd = distinct_values.filter(distinct_values.data_type==data_type).rdd.map(lambda x: type_cast(data_type, x['val']))  #we can use the valid values which we can use for type casting
            dt_dict["type"] = data_type  #we find the data type
            dt_dict["count"] = int(rdd.count())   #no.of values inside the datatype, we now proceed the  
            if data_type == "TEXT":                 #what to do if the datatype is text??? we can find the longest values, shortest values, we cant find maximum, minimum. So, my data profiling are intended to work with only these functions
                text_RDD = rdd.map(lambda x: (x.lower(), len(x)))
                longest_length = text_RDD.distinct().sortBy(lambda x: x[1]).top(5, key=lambda x: x[1])
                shortest_length = text_RDD.distinct().sortBy(lambda x: x[1]).take(5)
                dt_dict["shortest_values"] = [k[0] for k in shortest_length]
                dt_dict["longest_values"] = [k[0] for k in longest_length]
            if data_type == "DATE/TIME":                            #if the data type is time or a date, tehre can be maximum, or minimum, but no need of longest or shortest. We can have mean, std deviation
                dt_dict["max_value"] = str(rdd.max())
                dt_dict["min_value"] = str(rdd.min())
                
            if data_type ==  "INTEGER (LONG)":                     #find the maximum and minimum of this integer, we can have mean, std deviation 
                dt_dict["max_value"] = int(rdd.max())
                dt_dict["min_value"] = int(rdd.min())
                dt_dict["mean"] = float(rdd.mean())
                dt_dict["stddev"] = float(rdd.stdev())
                
            if data_type == "REAL":
                dt_dict["max_value"] = float(rdd.max())     #find the maximum and minimum if data type is float value which happens more in the real world scenerio. 
                dt_dict["min_value"] = float(rdd.min())
                dt_dict["mean"] = float(rdd.mean())
                dt_dict["stddev"] = float(rdd.stdev())            
            fin_dict["data_types"].append(dt_dict)        #at last, we append all these formatted things according to output under the dataset.             
        column_data["column_name"] = column_name          #call the name of the column function 
        column_data["number_non_empty_cells"] = int(count_non_null_vals.collect()[0][column_name])    #no. of non empty cells we display below the dataset 
        column_data["number_empty_cells"] = int(count_null_vals.collect()[0][column_name])           #no. of empty cells are displayed below the dataset
        column_data["number_distinct_values"] = int(data_rdd.distinct().count())                            #no.of distinct values in the column are displayed 
        freq_val = data_rdd.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], ascending=False).take(5)  #The frequency is calculated in the column and most frequent top 5  
        column_data["frequent_values"] = [v[0] for v in freq_val]   #display the frequent values
        column_data["data_types"] = fin_dict["data_types"]   #datatypes are the ones which are displayed at last 
        json_column_data.append(column_data) #dump finally all into the a column data. This will be like a dictionary inside main dictionary called the dataset_name
    output_data = {}
    output_data["dataset_name"] = filename  #Dataset name is displayed
    output_data["columns"] = column_list   #no.of columns are displayed
    output_data["key_column_candidates"] = []  #key column candidates are displayed here.
    json_file_data.append(output_data)        #append the dataset related first to a file 
    json_file_data.append(json_column_data)   #append remain entire column data which we did in above, we use this here to append to a file
    json_data = json.dumps(json_file_data)  #the file entire data is take out using dumps in order to merged this output and append to a common file so that we will have only one file which can give all the outputs in proper json format.
    return json_file_data   #this fn can be return here from the data profiling

counter = 0
for dataset_name in dataset_list:           #we recurse through the file which are elements of the list which we generated at first.
    output_json = {}        
    output_json = data_profile(dataset_name)   #call our data profiling method which modifies the dataset and get the output required
    final_merged_json.append(output_json)         #this might be the merged json file which can be taken as the merged json for all the datasets.
    log("Processed dataset - " + dataset_name)    #This operation is completed, so jsut trying to log the dataset 
    with open('task1.json', 'w') as out_file:   #Writing json for the particular dataset
        json.dump(final_merged_json, out_file)


with open('task1.json', 'w') as out_file:    #this is the merged json file for the entire datasset.
    json.dump(final_merged_json, out_file)
