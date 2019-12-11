#!/usr/bin/env python
# coding: utf-8
try:
    spark.stop()
except:
    pass
import sys
import pyspark
import string
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from pyspark.sql import Row

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *

import matplotlib.pyplot as plt
plt.ioff()
import modin.pandas as dd
import json
import pandas as pd
import numpy as np
import os
import string
from fuzzywuzzy import fuzz
import fuzzyset
import re
from geotext import GeoText
import fuzzy

from nltk import word_tokenize

import nltk
from nltk.tag import StanfordNERTagger


stanford_ner_tagger = StanfordNERTagger(
    '/home/ubuntu/stanfordner/stanford-ner-2018-10-16/' + 'classifiers/english.all.3class.distsim.crf.ser.gz',
    '/home/ubuntu/stanfordner/stanford-ner-2018-10-16/' + 'stanford-ner-3.9.2.jar'
)




sc = SparkContext()
spark = SparkSession.builder.appName("project").config("spark.some.config.option", "some-value").getOrCreate()

sqlContext = SQLContext(spark)


data = []
with open('cluster1.txt') as fp:
    data = [line.replace('[','').replace(']','').replace("'","") for line in fp]
data = data[0].split(",")
cols=[p.split('.')[1] for p in data]


f=os.listdir('NYCColumns/')



def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass
 
    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
 
    return False

def process(word):
    word = word.strip()
    lsword = word.split('_')
    wr = ""
    for w in lsword:
        if is_number(w):
            continue
        wr += w
    return wr.lower()

predictions = []
def predicttypesusingcolumnnames():
    types = [row.split('.')[1] for row in data]
    list_to_match = ["Person name","Last name", "First name", "Middle name", "Full name","Business name", "Phone Number",
                     "Address", "Street name","City","Neighborhood", "Latitude Longitude", "Zip", "Borough",
                     "School name", "Vehicle Color", "Vehicle Car make", "City agency",
                     "Areas of study", "Subjects",
                     "School Levels", "College/University names","Websites",
                     "Building Classification","Vehicle Type",
                     "Type of location","dba"]
    
    fz = fuzzyset.FuzzySet()    
    for l in list_to_match:
        fz.add(l.lower())
    count =0
    for row in types:
        actualdatasetname = data[count]
        lp =fz.get(process(row))
        count = count + 1
        predictions.append(list(lp[0])[1].lower())
predicttypesusingcolumnnames()




diction ={'kevu-8hby.STR_NAME.txt.gz' : 'street name', 'jz4z-kudi.Violation_Location__Zip_Code_.txt.gz':'zip','i8ys-e4pm.CORE_COURSE_9_12_ONLY_.txt.gz':'subjects','6wcu-cfa3.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz':'subjects','qgea-i56i.PREM_TYP_DESC.txt.gz':'type of location','8vgb-zm6e.City__State__Zip_.txt.gz':'zip','w9ak-ipjd.Filing_Representative_First_Name.txt.gz':'first name','sxx4-xhzg.Park_Site_Name.txt.gz':'park','d3ge-anaz.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz':'subjects','x5tk-fa54.Agency_Name.txt.gz':'City agency','72mk-a8z7.ORGANIZATION_PHONE.txt.gz':'Phone Number','6rrm-vxj9.parkname.txt.gz':'park','t8hj-ruu2.Business_Phone_Number.txt.gz':'phone number','ipu4-2q9a.Site_Safety_Mgr_s_Last_Name.txt.gz':'last name','w9ak-ipjd.Filing_Representative_City.txt.gz':'city',
'vw9i-7mzq.interest5.txt.gz':'areas of study','jz4z-kudi.Violation_Location__Zip_Code_.txt.gz':'zip','5uac-w243.PREM_TYP_DESC.txt.gz':'building classification','uq7m-95z8.interest5.txt.gz':'areas of study','jz4z-kudi.Respondent_Address__City_.txt.gz':'city','p937-wjvj.HOUSE_NUMBER.txt.gz':'other','erm2-nwe9.Landmark.txt.gz':'street name', 'jz4z-kudi.Respondent_Address__City_.txt.gz':'city','erm2-nwe9.Park_Facility_Name.txt.gz':'park','uzcy-9puk.Park_Facility_Name.txt.gz':'park','feu5-w2e2.BusinessCity.txt.gz':'business name','by6m-6zpb.interest.txt.gz':'areas of study','vw9i-7mzq.interest6.txt.gz':'areas of study','vw9i-7mzq.interest2.txt.gz':'areas of study','ph7v-u5f3.TOP_VEHICLE_MODELS___5.txt.gz':'vehicle car make', 'aiww-p3af.Park_Facility_Name.txt.gz':'park','sxmw-f24h.Park_Facility_Name.txt.gz':'park','bty7-2jhb.Site_Safety_Mgr_s_Last_Name.txt.gz':'last name','uq7m-95z8.interest3.txt.gz':'areas of study','erm2-nwe9.Landmark.txt.gz':'street name','feu5-w2e2.BusinessZip.txt.gz':'zip','p2d7-vcsb.COMPLAINT_INQUIRY_STREET_ADDRESS.txt.gz':'address','uq7m-95z8.interest4.txt.gz':'areas of study',
'uq7m-95z8.interest2.txt.gz':'areas of study'}

def getalloftype(fep):
    fep = fep.lower()
    out = []
    count =0
    for a in predictions:
        if(a == fep):
            out.append(data[count].replace("'","").strip())
        count = count + 1
    return out


def building_classification(word):
    word = word.lower()
    bdata = []
    with open('ValueData/buildingclassifications.txt') as fp:
        bdata = [line for line in fp]
    bdata = bdata[0].split(",")
    for p in bdata:
        p = p.replace("'","")
        r=fuzz.partial_ratio(p.lower(), word)
        if r>=80:
            return 1
    return 0

def vehicle_type(word):
    word = word.lower()
    vdata = []
    with open('ValueData/vehicletypes.txt') as fp:
        vdata = [line for line in fp]
    vdata = vdata[0].split(",")
    
    for p in vdata:
        p = p.replace("'","")
        r=fuzz.ratio(p.lower(), word)
        if r>=60:
            return 1
    return 0


def location_type(word):
    word = word.lower()
    vdata = []
    with open('ValueData/typesoflocation.txt') as fp:
        vdata = [line for line in fp]
    vdata = vdata[0].split(",")
    
    for p in vdata:
        p = p.replace("'","")
        r=fuzz.ratio(p.lower(), word)
        if r>=80:
            return 1
    return 0

def park_playground(word):
    word = word.lower()
    vdata = []
    with open('ValueData/parks.txt') as fp:
        vdata = [line for line in fp]
    vdata = vdata[0].split(",")
    
    for p in vdata:
        p = p.replace("'","")
        r=fuzz.ratio(p.lower(), word)
        if r>=80:
            return 1
    return 0

def college_name(word):
    word = word.lower()
    vdata = []
    with open('ValueData/colleges.txt') as fp:
        vdata = [line for line in fp]
    vdata = vdata[0].split(",")
    if ("college" in word) or ("university" in word):
        return 1
    for p in vdata:
        p = p.replace("'","").replace('"','')
        r=fuzz.ratio(p.lower(), word)
        if r>=80:
            return 1
    return 0

def school_name(word):
    word = word.lower()
    vdata = []
    if "school" in word:
        return 1
    
    with open('ValueData/schoolname.txt') as fp:
        vdata = [line for line in fp]
    vdata = vdata[0].split(",")
    
    for p in vdata:
        p = p.replace("'","").strip()
        r=fuzz.ratio(p.lower(), word)
        if r>=85:
            return 1
    return 0

def city_agency(word):
    word = word.lower()
    vdata = []
    with open('ValueData/cityagencieslist.txt') as fp:
        vdata = [line for line in fp]
    vdata = vdata[0].split(",")
    
    for p in vdata:
        p = p.replace("'","").strip()
        r=fuzz.ratio(p.lower(), word)
        if r>=80:
            return 1
    return 0

def area_of_study(word):
    word = word.lower()
    vdata = []
    with open('ValueData/areasofstudy.txt') as fp:
        vdata = [line for line in fp]
    vdata = vdata[0].split(",")
    
    for p in vdata:
        p = p.replace("'","").strip()
        r=fuzz.partial_ratio(p.lower(), word)
        if r>=75:
            return 1
    return 0

def website(word):
    word = word.lower()
    ls = re.split('\.|\/',word.lower())
    lst = [ 'nyc','ml','www','gov', 'net', 'com','me','info','edu', 'http', 'org', 'https','acad',]
    if any(item in ls for item in lst):
        is_website=1
    else:
        is_website=0
    return is_website


def borough(word):
    word = word.lower()
    firstcheck=['r','k','x','q','x']
    if len(word) == 1:
        if word in firstcheck:
            return 1
    
    vdata = ['bronx','brooklyn','manhattan','queens','staten island']
    for p in vdata:
        p = p.replace("'","").strip()
        r=fuzz.ratio(p.lower(), word)
        if r>=60:
            return 1
    return 0

def car_make(word):
    word = word.lower()
    vdata = [ 'ACURA','lincoln','AUTOC','MASSA', 'SUBAR','audi', 'DODGE','AMC','toyota', 'LEXUS','mitsubishi', 'VOLKS',
                'ACADE','AERO', 'INFIN',
            'BMW','M/B','ANCIA','lexus', 'VOLVO','gmc','AS/M', 'KIA','HIN',
                'LINCO', 'MINI','MITSU','nissan','INTER','subaru', 'JEEP','MACK', 'NISSA','kia','ford','KENWO','BL/B','MINI','CHRYS', 'FORD','JAGUA','cadillac','mazda','DUCAT','FERRA','AJAX','acura','HARLE','CADIL','HINO','BRIDG', 'AUDI','volks','FRUEH','CARGO','ISUZU','ALEXA',
                'FIAT', 'BUICK','APRIL','DORSE','volkswagen',
                'BERTO','HUMME','mercedes','AUSTI','CHECK','hyundai','jeep','nissan', 'TOYOT','MERCU','NS/OT', 
                'HONDA','chrysler','BENTL','BL/BI','tesla', 'GMC','bmw','buick','dodge','ALFAR','chevrolet','honda', 'HYUND','infiniti', 'CHEVR','MAZDA','KAWAS',]
    for p in vdata:
        p = p.replace("'","").strip()
        r=fuzz.partial_ratio(p.lower(), word)
        if r>=70:
            return 1
    return 0

def school_level(word):
    word = word.lower()
    vdata = ['Elementary School','K-8','K-3' 'Elementary','High School','YABC', 'High', 'Middle School','D75','K-2', 'Middle', 'Transfer']
    for p in vdata:
        p = p.replace("'","").strip()
        r=fuzz.ratio(p.lower(), word)
        if r>=70:
            return 1
    return 0

def address(value):
    if type(value)!=str or value==None:
        return 0
    street_name = ['street','st.','avenue','boulevard', 'pl', 'rd', 'alley', 'ave', 'bridge', 'blvd', 'ln', 'dr',
                  'slip', 'broaway', 'terrace', 'plaza', 'square', 'expy']
    for street in street_name:
        if fuzz.partial_ratio(street,value.lower())>=85 and value[0].isdigit() == True:
            return 1
    return 0

def subject_in_school(word):
    word = word.lower()
    vdata = ['english','math','science','social studies','studies','algebra','assumed team teaching','chemistry','earth science','economics','geometry','history','environment','physics','law','government','US GOVERNMENT']
    for p in vdata:
        p = p.replace("'","").strip()
        r=fuzz.partial_ratio(p.lower(), word)
        if r>=75:
            return 1
    return 0

def zip_code(word):
    if word is None:
        return 0
    regex= "^[0-9]{5}(?:-[0-9]{4})?$"
    if re.search(regex, word):
        return 1
    else:
        return 0

def neighborhood(word):
    word = word.lower()
    vdata = []
    
    with open('ValueData/neighborhoods.txt') as fp:
        vdata = [line for line in fp]
    vdata = vdata[0].split(",")
    
    for p in vdata:
        p = p.replace("'","").strip()
        r=fuzz.ratio(p.lower(), word)
        if r>=85:
            return 1
    return 0

def city(word):
    if word is None:
        return 0
    word =word.strip()
    word= word.title()
    y = word.split(" ")
    if len(y)>2:
        return 0
    cityout = GeoText(word)
    if(len(cityout.cities)==0):
        return 0
    else:
        return 1
    
def street_name(value):
    if type(value)!=str or value==None:
        return 0
    street_name = ['street','st.','avenue','boulevard', 'pl', 'rd', 'alley', 'ave', 'bridge', 'blvd', 'ln', 'dr',
                  'slip', 'broaway', 'terrace', 'plaza', 'square', 'expy']
    for street in street_name:
        if fuzz.partial_ratio(street,value.lower())>=85 and value[0].isdigit() == False:
            return 1
    return 0

def phone_number(word):
    if word is None:
        return 0
    if type(word)==int:
        phnum=str(phnum)
    phnum = word.replace('-','')
    if re.search(r"((\(\d{3}\)?)|(\d{3}))([\s./]?)(\d{3})([\s./]?)(\d{4})", phnum):
        return 1
    else:
        return 0

def business_name(word):
    if type(word)!=str or word==None:
        return 0
    lasttext = ['inc','inc.','p.c.','llc','l.l.c.','pc','lp','l.p.','corp','corp.','corporation','company','co.','co','ltd','ltd.','capital','holdings','services']
    for end in lasttext:
        ratio=fuzz.ratio(end, word.lower())
        if ratio>=90:
            return 1
    return 1

def person_name(word):
    if type(word)!=str or word==None:
        return 0
    if re.search(r"^[a-zA-Z]+(([',. -][a-zA-Z ])?[a-zA-Z]*)*$",word):
        return 1
    return 0

def color(word):
    
    codes =["BK","BL","GL","GY","MR","OR","PK","PR","RD","TN","WH","YW","NOCL"]
    for w in codes:
        if word.lower() == w.lower():
            return 1

    names = [ "White", "Black", "Gold", "Gray", "Maroon","Yellow", "Red", "Blue", "Green", "Brown", "Pink", "Orange", "Purple","Tan"]

    for p in names:
        p = p.replace("'","").strip()
        r=fuzz.partial_ratio(p.lower(), word)
        if r>=65:
            return 1
    return 0

import re
def lat_lon_cord(value):
    try:
        lag = value.split(',')[0][1:-1]
        s = r'^[\-\+]?(0(\.\d{1,10})?|([1-9](\d)?)(\.\d{1,10})?|1[0-7]\d{1}(\.\d{1,10})?|180\.0{1,10})$'
        if re.match(s, lag) != None:
            return 1
        else:
            return 0
    except:
        return 0


# In[155]:


predfunc = {'person name':'person_name','first name':'person_name','middle name':'person_name','websites':'process_website', 'city':'city','zip':'zip_code','phone number':'phone_number','school levels':'school_level',
'college/university names': 'college_name','borough':'borough','last name':'person_name','full name':'person_name','business name':'business_name','address':'address','street name':'street_name','neighborhood':'neighborhood','latitude longitude':'lat_lon_cord','school name':'school_name','vehicle color':'color',
'vehicle car make':'car_make','city agency':'city_agency','areas of study':'area_of_study','subjects':'subject_in_school','college/univeristy names':'college_name','websites':'website','building classification':'building_classification','vehicle type':'vehicle_type','type of location':'location_type','dba':'business_name','park':'park_playground','vechicle car make':'car_make'}

columntypepredicted = ""
def find_type(word):
    if word is None:
        return "other"
    if(globals()[predfunc[columntypepredicted]](word) == 1):
        return predfunc[columntypepredicted]
    if word.isdigit():
        if phone_number(word)==1:
            return 'phone_number'
        elif zip_code(word)==1:
            return 'zip_code'
        elif lat_lon_cord(word)==1:
            return 'lat_lon_cord'
        else:
            return 'other'
    
    if phone_number(word)==1:
        return 'phone_number'
    elif zip_code(word)==1:
        return 'zip_code'
    elif lat_lon_cord(word)==1:
        return 'lat_lon_cord'
    elif borough(word)==1:
        return 'borough'
    elif lat_lon_cord(word)==1:
        return 'lat_lon_cord'
    elif city(word)==1:
        return 'city'
    
    elif color(word)==1:
        return 'color'
    elif website(word)==1:
        return 'website'
    elif school_level(word)==1:
        return 'school_level'
    elif subject_in_school(word)==1:
        return 'subject_in_school'
    
    elif area_of_study(word)==1:
        return 'area_of_study'
    
    elif location_type(word)==1:
        return 'location_type'
    elif college_name(word)==1:
        return 'college_name'
    elif neighborhood(word)==1:
        return 'neighborhood'
    elif park_playground(word)==1:
        return 'park_playground'
    elif school_name(word)==1:
        return 'school_name'
    elif street_name(word)==1:
        return 'street_name'
    elif address(word)==1:
        return 'address'
    elif car_make(word)==1:
        return 'car_make'
    elif building_classification(word)==1:
        return 'building_classification'
    elif vehicle_type(word)==1:
        return 'vehicle_type'
    elif city_agency(word)==1:
        return 'city_agency'
    elif business_name(word)==1:
        if(predfunc[columntypepredicted]=='business_name'):
            return 'business_name'
        elif(predfunc[columntypepredicted]=='person_name'):
            return 'person_name'
        return 'other'
    elif person_name(word)==1:
        if(predfunc[columntypepredicted]=='person_name'):
            return 'person_name'
        return 'other'
    else:
        return 'other'
find_type = F.udf(find_type)

if not os.path.exists('Task_2_Results'):
    os.makedirs('Task_2_Results')
if not os.path.exists('Task_2_Results/plots'):
    os.makedirs('Task_2_Results/plots')
if not os.path.exists('Task_2_Results/json'):
    os.makedirs('Task_2_Results/json')
    
from collections import Counter 
from collections import defaultdict

given_label_list=["person_name", "business_name", "phone_number", "address", "street_name",
"city", "neighborhood", "lat_lon_cord", "zip_code", "borough", "school_name",
"color", "car_make", "city_agency", "area_of_study", "subject_in_school",
"school_level", "college_name", "website", "building_classification",
"vehicle_type", "location_type", "park_playground", "other"]
pred_labels=[]

actualcolumns=dict.fromkeys(given_label_list)
outputcolumns=dict.fromkeys(given_label_list)
actualcolumns = {x: 0 for x in actualcolumns}
outputcolumns = {x: 0 for x in outputcolumns}
matchedcolumns = dict.fromkeys(given_label_list)
matchedcolumns = {x: 0 for x in matchedcolumns}

count = 0
    
for i,x in enumerate(data):
    columntypepredicted=predictions[count]
    for p in diction:
        if(x.strip().lower() == p.strip().lower()):
            columntypepredicted=diction[x.strip()]
    actualcolumns[predfunc[columntypepredicted]] = actualcolumns.get(predfunc[columntypepredicted],0)+1
    col_name=x.split('.')[1]
    x=x.strip()
    fp='NYCColumns/'+x
    df = spark.read.format("csv").option("header","false").option("sep", "\t").load(fp).toDF(x.split('.')[1], 'y')
    df = df.drop('y')
    df = df.withColumn('Identified_Labels', find_type(F.col(col_name)))
    df=df.groupBy('Identified_Labels').count()
    pred_labels=df.select("*").distinct().collect()
    json_dict={"column_name": col_name, "semantic_types":[]}
    identifiedlabelsforgraph = []
    identifiesvaluesforgraph = []
    for label in pred_labels:
        rowi=label.asDict()["Identified_Labels"] 
        counti=label.asDict()["count"]
        identifiedlabelsforgraph.append(rowi)
        identifiesvaluesforgraph.append(counti)
        if rowi in given_label_list and rowi!='other':
            semantic_dict={"semantic_type":rowi, "count": int(counti)}
            json_dict["semantic_types"].append(semantic_dict)
        else:
            semantic_dict={"semantic_type":"Other","label":rowi ,"count": int(counti)}
            json_dict["semantic_types"].append(semantic_dict)
    res = {identifiedlabelsforgraph[i]: identifiesvaluesforgraph[i] for i in range(len(identifiedlabelsforgraph))} 
    k = Counter(res)
    top5 = k.most_common(5)
    top5key = []
    top5val = []
    if(list(top5[0])[0] == predfunc[columntypepredicted]):
        matchedcolumns[list(top5[0])[0]] = matchedcolumns.get(list(top5[0])[0],0) +1
    outputcolumns[list(top5[0])[0]] = outputcolumns.get(list(top5[0])[0],0) + 1
    for i in top5:
        list(i)
        top5key.append(i[0])
        top5val.append(i[1])
    json_file_path='Task_2_Results/json/'+x+'.json'
    plot_file_path='Task_2_Results/plots/'+x+'.png'
    
    with open(json_file_path, 'w', newline='\n') as json_file:
        json.dump(json_dict, json_file,indent=4, sort_keys=True)
    
    count = count+1
    #Plotting a bar graph for each column
    plt.bar(top5key,top5val, color="black")
    plt.savefig(plot_file_path)
    plt.close()
    
top5outputkey =[]
top5outputval =[]
k1 = Counter(outputcolumns)
top5output = k1.most_common(5)
for i in top5output:
    list(i)
    top5outputkey.append(i[0])
    top5outputval.append(i[1])
    
plt.bar(top5outputkey,top5outputval, color="black")
plt.savefig('Task_2_Results/plots/outputcolumn.png')
plt.close()

top5actualkey =[]
top5actualval =[]
k2 = Counter(actualcolumns)
top5actual = k2.most_common(5)
for i in top5:
    list(i)
    top5actualkey.append(i[0])
    top5actualval.append(i[1])
    
plt.bar(top5actualkey,top5actualval, color="black")
plt.savefig('Task_2_Results/plots/actualcolumn.png')
plt.close()

top5matchedkey =[]
top5matchedval =[]
k2 = Counter(matchedcolumns)
top5matched = k2.most_common(5)
for i in top5matched:
    list(i)
    top5matchedkey.append(i[0])
    top5matchedval.append(i[1])
    
plt.bar(top5matchedkey,top5matchedval, color="black")
plt.savefig('Task_2_Results/plots/matchedcolumn.png')
plt.close()

precision ={}
recall = {}
matchedkeys=[]
matchedvals=[]
actualkeys=[]
actualvals=[]
for key, value in matchedcolumns.items():
    actval = actualcolumns[key]
    outval = outputcolumns[key]
    matched = value
    if(outval!=0):
        precision[key]=str(matched/outval)
    else:
        precision[key]= "NA"
    if(actval!=0):
        recall[key]= str(matched/actval)
    else:
        recall[key] = "NA"