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

def append_year_toevent(input_dt): #Add year to event 
    input_date = input_dt[3:5]
    input_month = input_dt[0:2]
    current_month = datetime.now().month
    event_year = datetime.now().year
    if ( int(input_month) > current_month): #By Jun , Jul all log files of previous year, would have been processed
        event_year -= 1
    return("%s-%s-%s" %(event_year,input_month,input_date))
   


def get_tag_n_message(inputstr):
#Tag & Message are separated by : character
#However, Tag can itself contain :. So can't use simple locate function
#Eg: [    6.504] [33;1mAmp2@@[MODULE_SNDSRV:snd_pipeline_create():474]:[0m : SNDPIPELINE Create sound pipeline!
#Here Tag is [    6.504] [33;1mAmp2@@[MODULE_SNDSRV:snd_pipeline_create():474]:[0m 
#and message is SNDPIPELINE Create sound pipeline!
    
    #inputstr = inputstr.replace(",","!")#This is to be discussed when you write to CSV, these "," creating problem
    if(inputstr.find(chr(27)) == -1): # No Escape Character
        loc_sep = inputstr.find(":")
        #print("inputstr=%s,loc_sep=%d,mess=%s" %(inputstr,loc_sep,inputstr[-1*loc_sep:]))
        return inputstr[0:loc_sep], inputstr[loc_sep+1:len(inputstr)-1]
    
    lenstr = len(inputstr)
    cnt_braces = 0
    loc_sep=0
    initiated = 0
    for cnt in range(lenstr):
        #print("cnt=%d,char=%s,ord=%d,cnt_braces=%d" %(cnt,inputstr[cnt],ord(inputstr[cnt]),cnt_braces))
        if (ord(inputstr[cnt]) == 27 and initiated == 0): #First Escape Sequence
            cnt_braces += 1
            initiated = 1
            #print("Initated")
            #time.sleep(1)
        elif (ord(inputstr[cnt]) == 27 and initiated == 1 ): #Ending Escape Character
            cnt_braces -= 1
            #print("DeInitated")
            #time.sleep(1)
        elif (inputstr[cnt] == ":" and  cnt_braces == 0 and initiated == 1):
            loc_sep = cnt #Can use cnt itself?
            break
        else:
            continue
    return inputstr[0:loc_sep], inputstr[loc_sep+1:len(inputstr)-1]
    
def createesindexstbandroidlog(db_con): #Leave settings to default
    index_settings = {
    'mappings':{
     'properties' :{
     "STB_ID": {"type" : "text"},
     "TIME": {"type": "keyword"},
          "PID": {"type": "long"},
          "TID": {"type": "long"},
          "PRIORITY": {"type": "keyword"},
          "TAG": {"type": "keyword"},
          "MESSAGE": {"type": "keyword"}
          }
    }
    }# Ends Index Settings
    print("Creating index stb_androidlog")
    db_con.indices.create(index = "stb_androidlog",body=index_settings)
    return
    
    


#Main Program Starts Here
input_dir = "F:\\Jio\\Design\\phase2\\16-Mar-2020\\input" #This will be parametrized before checking

#Writing to outf is a temporary one for debug purpose, will be deleted before checking
outf = open('F:\\Jio\\Design\\phase2\\16-Mar-2020\\wrAndroid.csv','w')#For Writing Events
outf.write("stbid,Time,Pid,Tid,Priority,Tag,Message\n")

outcrf = open('F:\\Jio\\Design\\phase2\\16-Mar-2020\\crAndroid.csv','w') #For Writing Crash Detial
outcrf.write("STBID,Memory_Status,Memory_Status_At,Crash_Type,Crash_At,Build_Ver,Crash_Stack_Trace\n")

#Initialize Variables required for  Crash statistics collection
mmemstatus=''
mmemstatusat=''
mcrashtype=''
mcrashat=''
mbuildver=''
mcrashstacktrace=''

#Disable Elastic Search writing for now, as it is slowing down 
'''
#Open Elastic Search database  
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
if not es.ping():
    print("ElasticSearch Not running, start and rerun")
    exit(-1)
#Check if Required Index exists, if not create
if not(es.indices.exists('stb_androidlog')):
    print('Creating stb_androidlog')
    createesindexstbandroidlog(es)
else:
    print('Index stb_androidlog Already Existing')
    
'''

k=0
for inputfile_name in os.listdir(input_dir):
    inpf = open(input_dir+'\\'+inputfile_name,'r')
    mstbid=os.path.splitext(os.path.basename(inputfile_name))[0]
    print("Processing STB:%s" %(mstbid))
    #exit(-1)
    for input_line in inpf:
        k += 1
        if (input_line[0:2] == "--"):
            continue
        reststr= input_line[32:]
        tag = ""
        message = ""
        tag,message = get_tag_n_message(reststr)
        #print("reststr=%s,tag=%s,message=%s" %(reststr,tag,message))
        #str1="%s,%s,%s,%s,%s\n" %(x[0:21],x[21:27],x[27:31],x[31:32],reststrf)
        #Time = input_line[0:20]
        Time = append_year_toevent(input_line[0:5])+input_line[5:14]
        Seqno = input_line[15:20]#Just store, not sure what's the use of it
        #print("Time=%s,seq=%s" %(Time,Seqno))
        #exit(-1)
        pid = input_line[20:26]
        tid = input_line[26:31]
        priority = input_line[31:32]
        outstr="%s,%s,%s,%s,%s,%s,%s\n" %(mstbid,Time,pid,tid,priority,tag.replace(',','!'),message.replace(',','!'))
        outf.write(outstr)
        
        #Disable writing to ES, as it is taking long time
        '''
        #Write Data to ElasticSearch (ES)
        writestr={
        "STB_ID": mstbid,
        "TIME": Time,
        "PID": float(pid),
        "TID": float(tid),
        "PRIORITY": priority,
        "TAG": tag,
        "MESSAGE": message
        }
        es.index(index='stb_androidlog',doc_type='_doc', body=writestr)
        '''
        #Now Process Crash Details
        if not (tag.find( 'DataProvider')>0  or tag.find( 'Background')>0): #Neither Memory nor Crash Stack
            continue # Process next log entry
     
     # Process Memory Status at time of Crash
        if (tag.find( 'Background')>0): #Get Memory Status
            #print("%d:Processing Background:%s" %(k,message))
            if(message.find('"type":"process_memory_list_param"')<0):
                continue
            #print("Log,Message for Memory-->%d:%s" %(k,message))
            mmemstatusat = Time
            mmemstatus = message[72:][:-2]
            print("k=%d:mmemstatus=%s:mmemstatusat=%s" %(k,mmemstatus,mmemstatusat))
        
        #Now Process Crash Details
        if (tag.find( 'DataProvider')>0):
            if(message.find(' Name: \"crash_happened_event\"')<0): #Not Required
                continue
            print("Crash Message:%d:%s" %(k,message))
            print("mmemstatus=%s:mmemstatusat=%s" %(mmemstatus,mmemstatusat))
            
            mcrashat=Time
            #Find Crash Type
            if(message.find("ActivityManager: ANR in")>0):
                mcrashtype = "ANR"
            elif(message.find('ABI: \'') >0):
                mcrashtype = "TombStone"
            else:
                mcrashtype = "JVM"
            if (mcrashtype == "ANR" or mcrashtype == "TombStone"):
                mbuildver = "Unknown"
                mcrashstacktrace = "Unknown"
            else:
                #Get Build Version
                buildstr = 'ActivityManager\\nBuild:' #Where Build Version Starts
                trim1 = message.find(buildstr)+len(buildstr)
                #print(message[trim1:])
                mbuildver = message[trim1:]
                #print(mbuildver)
                buildstr = ':user' #This Time Where Build Version ends
                trim1 = mbuildver.find(buildstr)
                #print(trim1)
                mbuildver = mbuildver[0:trim1]
                #print(mbuildver)
                #Now Get Crash stack
                stackstr = "\"stack_trace\":"
                trim1 = message.find(stackstr)+len(stackstr)
                mcrashstacktrace = message[trim1:][:-2]
                #print(mcrashstacktrace)
            if (1==1): # Just Need Indentation
                #Write Details to CSV
                outstr = '%s,%s,%s,%s,%s,%s,%s\n' %(mstbid,mmemstatus,mmemstatusat,mcrashtype,mcrashat,mbuildver.replace('\n','').replace(',','!'),mcrashstacktrace.replace('\n','').replace(',','!') )
                #outstr = '%s,%s,%s,%s,%s,%s,%s\n' %(mstbid,mmemstatus,mmemstatusat,mcrashtype,mcrashat)
                outcrf.write(outstr)
                #Will not be replace while writing to ES
                mmemstatus=''
                mmemstatusat=''
                mcrashtype=''
                mcrashat=''
                mbuildver=''
                mcrashstacktrace=''
    inpf.close()

print("No of lines=%d" %(k))
outf.close()
outcrf.close()
