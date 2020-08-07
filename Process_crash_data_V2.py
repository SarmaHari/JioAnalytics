#05-Aug-2020: Replace leading Asteriks in case of Build Fingerprint Process_Info
#06-Jul-2020: For <1% of ANR data, Activity data coming - ignore
#02-Jul-2020 : Remove leading and trailing blanks from input AND add a new column dev_free_mem_percent
#29-Jun-2020 : Insert data of MI1 (Device Memory) inputs & outputs will have _V2_8
#12-Jun-2020: Fixed Time issue (convert to GMT and insert) AND Json string leading blank removed
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

def Read_Six_Column_File(file_name): #alternately use Pandas reading, when reading gets complicated with more columns or selection is required etc
    with open(file_name, 'r', encoding='utf-8') as f_input:
        csv_input = csv.reader(f_input, delimiter=',', skipinitialspace=True)
        x = []
        y = []
        p = []
        q = []
        r = []
        s = []
        i = 0

        for cols in csv_input:
            x.append(cols[0].strip())
            y.append(cols[1].strip())
            p.append(cols[2].strip())
            q.append(cols[3].strip())
            r.append(cols[4].strip())
            s.append(cols[5].strip())
            i = i + 1
        f_input.close()

    return x, y, p, q, r, s
    
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
        
    #<1%rows are having this extra information. Ignore, as it is <1% of Data. 06-Jul-2020  
    seppos1 = input_data.find('Activity:')
    seppos2 = input_data.find('\n')
    if (seppos1 > -1):
        #Foreground = input_data[seppos1+11:seppos2].strip()
        input_data = input_data[seppos2+1:]
        
    #print("input_data=%s" %(input_data))    
    seppos1 = input_data.find('Subject:')
    seppos2 = input_data.find('\n')
    if (seppos1 > -1):
        Executing = input_data[seppos1+8:seppos2].strip()
        input_data = input_data[seppos2+1:]
        #print("seppos1=%d,seppos2=%d,Executing = %s,input_data=%s" %(seppos1,seppos2,Executing,input_data))
    else:
        print("Subject Not found:%s" %(input_data))
                                    
    seppos1 = input_data.find('Build:')
    seppos2 = input_data.find('\n')
    if (seppos1 > -1):
        Build = input_data[seppos1+6:seppos2].strip()
        input_data = input_data[seppos2+1:]
        #print("Build=%s" %(Build))
    else:
        print("Build Not found:%s" %(input_data))
    
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
     "EVENTDATE_TIME" : {"type": "date", "format" : "dd-MM-yyyy HH:mm:ss" },#This is the one used to load however 30-Jul data does not have seconds, so below line
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
     "BUILD" :{"type" : "keyword"},
     "DEV_TOT_MEM" : {"type" : "float"},
     "DEV_FREE_MEM" : {"type" : "float"},
     "DEV_FREE_MEM_PERCENT" : {"type" : "float"},
     "CRASH_CAUSE" : {"type" : "keyword"},
     "CRASH_ORIGIN" : {"type": "keyword"}
      }
    }
    }# Ends Index Settings
    print("Creating index crashlog")
    es.indices.create(index = index_name,body=index_settings)
    return
    
def insert_into_es(es,outputstb,outputdatetime,outputevent,outputver,outeventdes,outprocessname,outcrashtype,outprocessinfo,outanr_process_name,outPID,outFlags,outPackage,outForeground,outExecuting,outBuild,outdev_free_mem, outdev_tot_mem,outcrash_cause,outcrash_origin):
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
     "BUILD" :outBuild,
     "DEV_TOT_MEM" :outdev_tot_mem,
     "DEV_FREE_MEM" :outdev_free_mem,
     "DEV_FREE_MEM_PERCENT":(outdev_free_mem/outdev_tot_mem)*100,
     "CRASH_CAUSE" : outcrash_cause,
     "CRASH_ORIGIN" : outcrash_origin
     }
    es.index(index=index_name ,doc_type='_doc', body=writedetstr)
    #exit(-1)
    return
    
def get_crashdet(inputstr):#Function to Get Cause and Origin of Crash, Refer Document shared with Jio
    #print("inputstr=%s" %(inputstr))
    if(inputstr.find(' more')>=0):
        if (inputstr.split('\n')[0].find('Sending non-protected broadcast')>=0):
            return(inputstr.split('\n')[0],'Sending non-protected broadcast')
        else:
            return(inputstr.split('\n')[0],"Incomplete")
    else:
        return(inputstr.split('\n')[0],inputstr.split('\n')[-1])

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




#These parameters will be parmetrized after development over
#stbdata_V2_8 is Mi1 (device memory processing) received on 29-Jun-2020
outcsv = open("crashlog_V2_8.csv","w")  #parameterize
write_toES = 1 #Parameterize, Is writing to ES required, 1 means Yes
index_name = 'jiostb_2020-07-30'  #Parameterize, this is the ES index data goes into
jsonstr,version,stb,xdatetime,memstr,event = Read_Six_Column_File('After_Removing_Duplicates.csv') #Parameterize later




if (write_toES == 1): # Check if ES is running and need to create index
    esind = check_es_index()
i=0
eventcnt=0
failcnt=0
recordsuccess=0 #0 is succesful
rectoes = 0
outcsv.write("STBID,EventDateTime,Event,App_Ver,EventType,Process_name,Process_Info,Crash_Header,Anr_Process_Name,Pid,Flags,Package,Foreground,Subject,Build,dev_free_mem,dev_tot_mem,dev_free_mem_percent,crash_cause,crash_origin\n")

print("====>Important: 30-Jul-2020 data does not have seconds, so adding seconds, remove After JioTeam fixes this issue")

while (i<len(stb)):
    recordsuccess=0 #0 is succesful
    if (not (event[i].strip() == 'XPSE9' or event[i] == 'XPSE10')):# Need to process only two events
        print("Skipping event[%d]=%s" %(i,event[i]))
        i=i+1
        continue # Go To Next Record
        
    #Start Processinng XPSE9 & XPSE10 events
    outputstb = stb[i]
    #print("xdatetime[%d]=" %(i))
    #print(xdatetime[i])
    #print(memstr[i])
    xdatetime[i] = xdatetime[i]+':00'
    outputdatetime = (datetime.strptime(xdatetime[i],'%d-%m-%Y %H:%M:%S')-timedelta(hours=5, minutes = 30)).strftime('%d-%m-%Y %H:%M:%S') #Subtract 5.30 hours, as ES is adding 5.30 hours
   
    #print("outputdatetime=%s" %(outputdatetime))
    #exit(-1)
    #outputdatetime = (datetime.strptime(xdatetime[i],'%D-%M-%Y %H:%M:%S')-timedelta(hours=5, minutes = 30)).strftime('%D-%M-%Y %H:%M:%S') #Subtract 5.30 hours, as ES is adding 5.30 hours
    #outputdatetime = xdatetime[i]
    outputevent = event[i].strip()
    outputver = version[i]
    # Remove First and Last characters of Json
    jsonstr[i] = jsonstr[i].strip()
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
    outdev_free_mem = 0
    outdev_tot_mem = 0
    outcrash_cause=''
    outcrash_origin=''
    #Get Memory data
    tempmemstr = memstr[i].replace('[','').replace(']','').replace('"','')
    outdev_free_mem = float(tempmemstr[0:tempmemstr.find(',')])
    outdev_tot_mem = float(tempmemstr[tempmemstr.find(',')+1:])
    #print("free=%f,tot=%f"%(outdev_free_mem,outdev_tot_mem))
    
    data = json.loads(jsonstr[i]) #Try strict=False in case Jio Python differs from our dev machine
    #If no of ifs, events increase, think of defining a function
    if (outputevent == "XPSE9"):
        try:
            outeventdes = data["XPS2"]
            outprocessname = data["PS21"].strip()
            outprocessinfo = data["PS22"].strip().replace('*** *** *** *** *** *** *** *** *** *** *** *** *** *** *** ***\n','')
            seppos =  outprocessinfo.find(':')
            if (seppos > -1):
                outcrashtype = outprocessinfo[0:seppos].strip()
                #outprocessinfo = outprocessinfo[seppos+1:].strip()
        except Exception as e:
            print("%s:Record Number =%d, Problem in getting data:%s" %(xdatetime[i],i,jsonstr[i]))
            failcnt += 1
            recordsuccess = 1
            pass
        outcrash_cause,outcrash_origin = get_crashdet(outprocessinfo)#If this statement in in try block, failure of this function will result flow to go to Exception part
    else: #XPSE10
        try:
            outeventdes = data["XPS3"]
            outprocessname = data["PS31"].strip()
            outprocessinfo = data["PS32"].strip()
            outanr_process_name,outPID,outFlags,outPackage,outForeground,outExecuting,outBuild = process_anr_info(outprocessinfo)
        except Exception as e:
            print("%s:Record Number =%d, Problem in getting data:%s" %(xdatetime[i],i,jsonstr[i]))
            failcnt += 1
            recordsuccess = 1
            pass
        outcrash_cause='NA'
        outcrash_origin='NA'
        
    #Write Output
    if (recordsuccess == 0):
        #As input is increasing, varaibles to written are increasing, break into multiplelines
        outstr ="%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%f,%f,%f,%s,%s\n" %(outputstb,outputdatetime,outputevent,outputver,outeventdes,outprocessname.replace(',','!').replace('\n','@'),outprocessinfo.replace(',','!').replace('\n','@'),outcrashtype.replace(',','!').replace('\n','@'),outanr_process_name,outPID,outFlags,outPackage,outForeground,outExecuting,outBuild,outdev_free_mem,outdev_tot_mem,outdev_free_mem/outdev_tot_mem*100,outcrash_cause.replace(',','!').replace('\n','@'),outcrash_origin.replace(',','!').replace('\n','@')) #Replace Required only for csv, while inserting into ES, will remove
        outcsv.write(outstr)
        if (write_toES == 1): #Write record to Elastic Search
            insert_into_es(esind,outputstb,outputdatetime,outputevent,outputver,outeventdes,outprocessname,outcrashtype,outprocessinfo,outanr_process_name,outPID,outFlags,outPackage,outForeground,outExecuting,outBuild,outdev_free_mem,outdev_tot_mem,outcrash_cause,outcrash_origin)
            rectoes += 1
    else: #Reset Recordstatus to success for next record
        recordsuccess = 0
    #Increment, counters as required
    eventcnt += 1
    i += 1
#For loop Over
outcsv.close()
print("Total Processed=%d, eventcnt=%d,failcnt=%d,Records written to ES=%d" %(i,eventcnt,failcnt,rectoes))