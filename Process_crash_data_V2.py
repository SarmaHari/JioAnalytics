#10-Jun2020: Subtracting 5.30 hours and inserting into ES
#4-Jun-2020: Crash Info /Stack splitting into various columns
# Started Coding on 03-Jun-2020 - Sarma
#V2 as part of File name indicates, data format sent on 03-Jun-2020, This data format is different from earlier one

def listToString(s):  
    
    # initialize an empty string 
    str1 = ""  
    
    # traverse in the string   
    for ele in s:  
        str1 += ele   
    
    # return string   
    return str1  

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
    
def process_anr_info(input_data):
    anr_process_name = 'NA'
    PID = 'NA'
    Flags = 'NA'
    Package = 'NA'
    Foreground = 'NA'
    Executing = 'NA'
    Build = 'NA'
    seppos1 = input_data.find('Process:')
    seppos2 = input_data.find('\n')
    if (seppos1 > -1):
        anr_process_name = input_data[seppos1+8:seppos2].strip()
        input_data = input_data[seppos2+1:]
    
    seppos1 = input_data.find('PID:')
    seppos2 = input_data.find('\n')
    if (seppos1 > -1):
        PID = input_data[seppos1+4:seppos2].strip()
        input_data = input_data[seppos2+1:]
            
    seppos1 = input_data.find('Flags:')
    seppos2 = input_data.find('\n')
    if (seppos1 > -1):
        Flags = input_data[seppos1+6:seppos2].strip()
        input_data = input_data[seppos2+1:]
                   
    seppos1 = input_data.find('Package:')
    seppos2 = input_data.find('\n')
    if (seppos1 > -1):
        Package = input_data[seppos1+8:seppos2].strip()
        input_data = input_data[seppos2+1:]
                        
    seppos1 = input_data.find('Foreground:')
    seppos2 = input_data.find('\n')
    if (seppos1 > -1):
        Foreground = input_data[seppos1+11:seppos2].strip()
        input_data = input_data[seppos2+1:]
                              
    seppos1 = input_data.find('Subject:')
    seppos2 = input_data.find('\n')
    if (seppos1 > -1):
        Executing = input_data[seppos1+8:seppos2].strip()
        input_data = input_data[seppos2+1:]
                                    
    seppos1 = input_data.find('Build:')
    seppos2 = input_data.find('\n')
    if (seppos1 > -1):
        Build = input_data[seppos1+6:seppos2].strip()
        input_data = input_data[seppos2+1:]
    
    #print("anr_process_name = %s &&& input_data=%s" %(anr_process_name,input_data))
    #print("anr_process_name=%s\nPID =%s\nFlags =%s\nPackage =%s\nForeground =%s\nExecuting =%s\nBuild =%s\n" %(anr_process_name,PID,Flags,Package,Foreground,Executing,Build))
    return anr_process_name,PID,Flags,Package,Foreground,Executing,Build
  
def check_es_index(): #Connect to ES, check if indexes exist, if not create, return ES point
    #Open Elastic Search database  
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if not es.ping():
        print("ElasticSearch Not running, start and rerun")
        exit(-1)
    #Check if Required Index exists, if not create
    if (es.indices.exists(index_name)):
        print("Index %s already exists .. append data" %(index_name))
        return(es)
    createindex_stb_crash_anr_v2(es)
    return(es)
    
def createindex_stb_crash_anr_v2(es):
    index_settings = {
    'mappings':{
     'properties' :{
     "STBID" :{"type" :"keyword"},
     #"EVENTDATE_TIME" : {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis" },
     #"EVENTDATE_TIME" : {"type": "date", "format": "DD-MM-YYYY HH:mm:ss" },
     "EVENTDATE_TIME" : {"type": "date", "format" : "dd-MM-yyyy HH:mm:ss||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis" },
     "EVENT" : {"type" : "keyword"},
     "APP_VER" : {"type" : "keyword"},
     "EVENT_TYPE" : {"type" : "keyword"},
     "PROCESS_NAME" : {"type" : "keyword"},
     "PROCESS_INFO" : {"type" : "keyword"},
     "CRASH_HEADER" : {"type" : "keyword"},
     "ANR_PROCESS_NAME" : {"type" : "keyword"},
     "PID" : {"type" : "keyword"},
     "FLAGS" :{"type" : "keyword"},
     "PACKAGE" :{"type" : "keyword"},
     "FOREGROUND" :{"type" : "keyword"},
     "SUBJECT" :{"type" : "keyword"},
     "BUILD" :{"type" : "keyword"}
      }
    }
    }# Ends Index Settings
    print("Creating index crashlog")
    es.indices.create(index = index_name,body=index_settings)
    return
    
def insert_into_es(es,outputstb,outputdatetime,outputevent,outputver,outeventdes,outprocessname,outcrashtype,outprocessinfo,outanr_process_name,outPID,outFlags,outPackage,outForeground,outExecuting,outBuild):
    #print("outputdatetime=%s" %(outputdatetime))
    writedetstr = {
    "STBID" :outputstb,
     "EVENTDATE_TIME" : outputdatetime,
     "EVENT" : outputevent,
     "APP_VER" : outputver,
     "EVENT_TYPE" : outeventdes,
     "PROCESS_NAME" : outprocessname,
     "PROCESS_INFO" : outprocessinfo,
     "CRASH_HEADER" : outcrashtype,
     "ANR_PROCESS_NAME" : outanr_process_name,
     #"PID" : float(outPID),
     "PID" : outPID,
     "FLAGS" :outFlags,
     "PACKAGE" :outPackage,
     "FOREGROUND" :outForeground,
     "SUBJECT" :outExecuting,
     "BUILD" :outBuild
     }
    es.index(index=index_name ,doc_type='_doc', body=writedetstr)
    return

## Main Program Starts from here  
import sys
import time
import requests
import re
import os
import csv
from datetime import datetime,date
from datetime import timedelta
from datetime import datetime
import ast
import json
import fnmatch
from os import path
import pandas as pd
from datetime import datetime, time as datetime_time, timedelta
from math import ceil
from elasticsearch import Elasticsearch


#process_anr_info("Process: com.jio.stb.tifextn\nPID: 20041\nFlags: 0x30c8be4d\nPackage: com.jio.stb.tifextn v1 (0.1)\nForeground: No\nSubject: executing service com.jio.stb.tifextn/.TIFExtnService\nBuild: Jio/curie/curie:9/1.6fact/20200507:user/release-keys\n\n")
#process_anr_info("Process: com.jio.stb.catv\nPID: 26872\nFlags: 0x38c8be45\nPackage: com.jio.stb.catv v145 (1.0.25.10)\nForeground: Yes\nSubject: Broadcast of Intent { act=com.jio.stb.catv.adc.action.F flg=0x14 cmp=com.jio.stb.catv/com.jio.adc.core.UR (has extras) }\nBuild: Jio/franklin/franklin:9/2.4.3/20200421:user/dev-keys\n\n")
#exit(-1)

#These parameters will be parmetrized after development over
stb,xdatetime,event,version,jsonstr = Read_Five_Column_File('stbdata.csv') #Parameterize later
outcsv = open("crashlog_V2.csv","w")  #parameterize
write_toES = 1 #Parameterize, Is writing to ES required, 1 means Yes
index_name = 'stb_crash_anr_v2_5' #Parameterize, this is the ES index data goes into


if (write_toES == 1): # Check if ES is running and need to create index
    esind = check_es_index()
i=0
eventcnt=0
failcnt=0
recordsuccess=0 #0 is succesful
outcsv.write("STBID,EventDateTime,Event,App_Ver,EventType,Process_name,Process_Info,Crash_Header,Anr_Process_Name,Pid,Flags,Package,Foreground,Subject,Build\n")


while (i<len(stb)):
    if (not (event[i].strip() == 'XPSE9' or event[i] == 'XPSE10')):# Need to process only two events
        print("Skipping event[%d]=%s" %(i,event[i]))
        i=i+1
        continue # Go To Next Record
        
    #Start Processinng XPSE9 & XPSE10 events
    outputstb = stb[i]
    outputdatetime = (datetime.strptime(xdatetime[i],'%d-%m-%Y %H:%M:%S')-timedelta(hours=5, minutes = 30)).strftime('%d-%m-%Y %H:%M:%S') #Subtract 5.30 hours, as ES is adding 5.30 hours
    outputevent = event[i].strip()
    outputver = version[i]
    # Remove First and Last characters of Json
    jsonstr[i] = jsonstr[i][1:]
    jsonstr[i] = jsonstr[i][:-1]
    outeventdes = 'NA'
    outprocessname = 'NA'
    outprocessinfo = 'NA'
    outcrashtype = 'NA'
    outanr_process_name = 'NA'
    outPID = 'NA'
    outFlags = 'NA'
    outPackage = ''
    outForeground = 'NA'
    outExecuting = 'NA' #name it as outSubject for uniformity?
    outBuild = 'NA'
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
                #outprocessinfo = outprocessinfo[seppos+1:].strip()
        except Exception as e:
            #print("Record Number =%d, Problem in getting data" %(i))
            failcnt += 1
            recordsuccess = 1
            pass
    else: #XPSE10
        outeventdes = data["XPS3"]
        outprocessname = data["PS31"].strip()
        outprocessinfo = data["PS32"].strip()
        outanr_process_name,outPID,outFlags,outPackage,outForeground,outExecuting,outBuild = process_anr_info(outprocessinfo)
        
    #Write Output
    if (recordsuccess == 0):
        outstr ="%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" %(outputstb,outputdatetime,outputevent,outputver,outeventdes,outprocessname.replace(',','!').replace('\n','@'),outprocessinfo.replace(',','!').replace('\n','@'),outcrashtype.replace(',','!').replace('\n','@'),outanr_process_name,outPID,outFlags,outPackage,outForeground,outExecuting,outBuild) #Replace Required only for csv, while inserting into ES, will remove
        outcsv.write(outstr)
        if (write_toES == 1): #Write record to Elastic Search
            insert_into_es(esind,outputstb,outputdatetime,outputevent,outputver,outeventdes,outprocessname,outcrashtype,outprocessinfo,outanr_process_name,outPID,outFlags,outPackage,outForeground,outExecuting,outBuild)
    else: #Reset Recordstatus to success for next record
        recordsuccess = 0
    #Increment, counters as required
    eventcnt += 1
    i += 1
#For loop Over
outcsv.close()
print("Total Processed=%d, eventcnt=%d,failcnt=%d" %(i,eventcnt,failcnt))
    