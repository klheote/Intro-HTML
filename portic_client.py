#!/usr/bin/python3
# -*-coding:UTF-8 -*
'''
Created on 12 october 2020
@author: cplumejeaud, 

Used to read and parse data coming from the data.portic.fr API
'''
import http.client
from io import StringIO, BytesIO, TextIOWrapper
import json
import pandas as pd

## If you grab data from another Web server


#target_url = "http://data.portic.fr/api/ports/?shortenfields=false&both_to=false&date=1787"

## Doc : https://docs.python.org/3.7/library/http.client.html#httpresponse-objects

## Try this  
#conn = http.client.HTTPSConnection("data.portic.fr")
conn = http.client.HTTPConnection("data.portic.fr")

conn.request("GET", "/")
r1 = conn.getresponse()
print(r1.status, r1.reason)
#404 NOT FOUND

conn = http.client.HTTPConnection("data.portic.fr")
conn.request("GET", "/api/ports/?shortenfields=false&both_to=false&date=1787")
r1 = conn.getresponse()
print(r1.status, r1.reason)
print(r1)

data1 = r1.read()  # This will return entire content.
type(data1) #bytes
b = BytesIO(data1)
b.seek(0) #Start of stream (the default).  pos should be  = 0;
data = json.load(b)
type(data)

print(data)

## If you read data in a file on your server
'''
with open('ports.json') as f:
  data = json.load(f)
'''

type(data) #<class 'list'>
df = pd.DataFrame(data)

df.shape
#(5, 12) (993, 12)

df.columns
'''Index(['admiralty', 'belonging_states', 'belonging_substates', 'geonameid',
       'ogc_fid', 'point', 'province', 'shiparea', 'status', 'toponym',
       'total', 'uhgs_id'],
      dtype='object')
      '''

df.admiralty.unique()

# Dealing with null values
df.admiralty.isnull().values.any() #True 
values = {'admiralty': 'X'}
df = df.fillna(value=values)
df.admiralty.isnull().values.any() #False 

# Listing of admiralties
df.admiralty.unique() # array(['X'], dtype=object)
df.admiralty.unique().size #52

print(df)
# How many ports by admiralty ?
df.groupby('admiralty')['ogc_fid'].count()

# How many ports by belonging_states ?
df.groupby('belonging_states')['ogc_fid'].count()


def getStateForDate(jsonelement, dateparam):
    """
    jsonelement : [{"1749-1815" : "Toscane"},{"1801-1807" : "Royaume d'Étrurie"},{"1808-1814" : "Empire français"}]
    Internal method to output state for 1787 as dateparam
    return name of the state of the period including dateparam 
    state = getStateForDate(json.load(StringIO('{"1749-1815" : "Toscane"}')), 1787)
    """
    eltjson = json.loads(jsonelement) 
    for k in eltjson :
        for dates, state in k.items():
            datesarray = dates.split('-')
            start = datesarray[0]
            end = datesarray[1]
            if (int(start) <= dateparam <= int(end)):
                return state
            
for elt in df['belonging_states'].unique():
    if elt is None :
        df.loc[df.belonging_states.eq(elt), 'state_1787'] = 'X' 
    else :
        state = getStateForDate(elt, 1787)
        if state is not None:
            df.loc[df.belonging_states.eq(elt), 'state_1787'] = state

#https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html

## Check the results
df[['toponym', 'belonging_states', 'state_1787']]        
df.shape

type(df)
df.loc[0, ['toponym', 'belonging_states', 'state_1787']]

# How many ports by belonging_states ?
df.groupby('state_1787')['ogc_fid'].count()

# Dump dataframe as html
df.to_html()