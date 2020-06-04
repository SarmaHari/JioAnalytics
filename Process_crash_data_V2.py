#4-Jun-2020: Crash Info /Stack splitting into various columns
# Started Coding on 03-Jun-2020 - Sarma
#V2 as part of File name indicates, data format sent on 03-Jun-2020, This data format is different from earlier one

def Read_Five_Column_File(file_name): #alternately use Pandas reading, when reading gets complicated with more columns or selection is required etc
    with open(file_name, 'r', encoding='utf-8') as f_input:
        csv_input = csv.reader(f_input, delimiter=',', skipinitialspace=True)
        x = []
        y = []
        p = []
        q = []
        r = []
        i = 0

        for cols in csv_input:
            x.append(cols[0])
            y.append(cols[1])
            p.append(cols[2])
            q.append(cols[3])
            r.append(cols[4])
            i = i + 1
        f_input.close()

    return x, y, p, q, r
  
## Main Program Starts from here  
import sys
import time
import requests
import re
import os
import csv
from datetime import datetime,date
from datetime import timedelta
import ast
import json
import fnmatch
from os import path
import pandas as pd
from datetime import datetime, time as datetime_time, timedelta
from math import ceil
from elasticsearch import Elasticsearch

#These parameters will be parmetrized after development over
stb,datetime,event,version,jsonstr = Read_Five_Column_File('stbdata.csv') #Parameterize later
outcsv = open("crashlog.csv","w")  #parameterize

#print("Totally Read %d records" %(len(jsonstr)))
i=0
eventcnt=0
failcnt=0
recordsuccess=0 #0 is succesful

while (i<len(stb)):
    if (not (event[i].strip() == 'XPSE9' or event[i] == 'XPSE10')):# Need to process only two events
        print("Skipping event[%d]=%s" %(i,event[i]))
        i=i+1
        continue # Go To Next Record
        
    #Start Processinng XPSE9 & XPSE10 events
    outputstb = stb[i]
    outputdatetime = datetime[i]
    outputevent = event[i].strip()
    outputver = version[i]
    # Remove First and Last characters of Json
    jsonstr[i] = jsonstr[i][1:]
    jsonstr[i] = jsonstr[i][:-1]
    outeventdes = ''
    outprocessname = ''
    outprocessinfo = ''
    outcrashtype = ''
    data = json.loads(jsonstr[i])
    #If no of ifs, events increase, think of defining a function
    if (outputevent == "XPSE9"):
        try:
            outeventdes = data["XPS2"]
            outprocessname = data["PS21"].strip()
            outprocessinfo = data["PS22"].strip()
            seppos =  outprocessinfo.find(':')
            if (seppos > -1):
                outcrashtype = outprocessinfo[0:seppos].strip()
                outprocessinfo = outprocessinfo[seppos+1:].strip()
        except Exception as e:
            #print("Record Number =%d, Problem in getting data" %(i))
            failcnt += 1
            recordsuccess = 1
            pass
    else: #XPSE10
        outeventdes = data["XPS3"]
        outprocessname = data["PS31"]
        outprocessinfo = data["PS32"]
        
    #Write Output
    if (recordsuccess == 0):
        outstr ="%s,%s,%s,%s,%s,%s,%s,%s\n" %(outputstb,outputdatetime,outputevent,outputver,outeventdes,outprocessname.replace(',','!').replace('\n','@'),outcrashtype.replace(',','!').replace('\n','@'),outprocessinfo.replace(',','!').replace('\n','@')) #Replace Required only for csv, while inserting into ES, will remove
        outcsv.write(outstr)
    else: #Reset Recordstatus to success for next record
        recordsuccess = 0
    #Increment, counters as required
    eventcnt += 1
    i += 1
#For loop Over
outcsv.close()
print("Total Processed=%d, eventcnt=%d,failcnt=%d" %(i,eventcnt,failcnt))
    