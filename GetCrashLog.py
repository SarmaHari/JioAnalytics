#09-Apr-2020: Incorporated Processing Date
#Python Program to develop a crash events CSV 
#Started on 02-APr-2020 after a call with SUreash on 1-Apr-2020

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
import shutil
import glob,os
import io


def check_es_index(): #Connect to ES, check if indexes exist, if not create, return ES point
    #Open Elastic Search database  
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if not es.ping():
        print("ElasticSearch Not running, start and rerun")
        exit(-1)
    #Check if Required Index exists, if not create
    if not(es.indices.exists('stb_androidlog')):
        createesindexAndroidlog(es)
    else:
        print('Index Androidlog Already Existing')
        
    if not(es.indices.exists('stb_devicecrashlog')):
        createesindexDeviceCrashlog(es)
    else:
        print('Index DeviceCrashLog Already Existing')
    return(es)
    
def createesindexAndroidlog(db_con): #Create Android log index if not existsing
    index_settings = {
    'mappings':{
     'properties' :{
     "STBID" :{"type" :"keyword"},
     "CRASH_TIME" : {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis" },
     "PID" : {"type" : "long"},
     "TID" : {"type" : "long"},
     "PRIORITY" :{"type" :"keyword"},
     "TAG" :{"type" :"keyword"},
     "MESSAGE" :{"type" :"keyword"}
      }
    }
    }# Ends Index Settings
    print("Creating index AndroidLog")
    db_con.indices.create(index = "stb_androidlog",body=index_settings)
    return
    
def createesindexDeviceCrashlog(db_con): #Create Android log index if not existsing
    index_settings = {
    'mappings':{
     'properties' :{
     "STBID" :{"type" :"keyword"},
     "PROCESSING_DATE" : {"type": "date", "format" : "YYYY-MM-DD"},
     "CRASH_TIME" : {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis" },
     "PID" : {"type" : "long"},
     "PROCESS" :{"type" : "keyword"},
     "FLAG" :{"type" : "keyword"},
     "PACKAGE" :{"type" : "keyword"},
     "FOREGROUND" :{"type" : "keyword"},
     "BUILD" :{"type" : "keyword"},
     "CRASH_TRACE" :{"type" : "keyword"}
      }
    }
    }# Ends Index Settings
    print("Creating index crashlog")
    db_con.indices.create(index = "stb_devicecrashlog",body=index_settings)
    return

def append_year_toevent(input_dt): #Add year to event 
    input_date = input_dt[3:5]
    input_month = input_dt[0:2]
    try:
        x=int(input_month)
    except Exception as e:
        print("Something Wrong input_dt=%s,input_date=%s,input_month=%s" %(input_dt,input_date,input_month))
        return("%s-%s-%s" %(-1,-1,-1))
      
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

def get_stb_ver(dirpath): #Get STB version from the list of ALL files in the directory
    os.chdir(dirpath)
    mbuildver=""
    #First uncompress all zipped files and then untar
    for gzfile in glob.glob("*.tgz"):
        #print("%s:Uncompressing in dropbox folder %s" %(mstbid,gzfile))
        systemcmd = "7z x "+gzfile
        os.system(systemcmd)
    for gzfile in glob.glob("*.tgz"):
        #print("%s:Uncompressing in dropbox folder %s" %(mstbid,gzfile))
        systemcmd = "7z x "+gzfile
        os.system(systemcmd)
    for gzfile in glob.glob("*.7z"):
        #print("%s:Uncompressing in dropbox folder %s" %(mstbid,gzfile))
        systemcmd = "7z x "+gzfile
        os.system(systemcmd)
    for tarfile in glob.glob("*.tar"):
        systemcmd = "tar -xvf "+gzfile
        os.system(systemcmd)
       
    for input_file in glob.glob("*.txt"):#Alternately use comamnd " grep "I TR069NativeService: Current version  of STB:" * | sed '1,$s/^.*I TR069NativeService: Current version  of STB: //g'|uniq
        inpf = open(input_file,"r",encoding="utf8")
        for input_line in inpf:
            if(input_line.find('I TR069NativeService:')>=0 and input_line.find('Current version  of STB:')>=0):
                reqstr='Current version  of STB:'
                mbuildver=input_line[input_line.find(reqstr)+len(reqstr)+1:len(input_line)]
                #print("mbuildver=%s" %(mbuildver))
    #return(mbuildver.replace(',','!').replace('\n','!')) # Remove this replacement before inserting into ES
    return(mbuildver) 

def process_logs(locdir,mstbid):
    mbuildver= get_stb_ver(locdir) # first get buildver
    print("%s:Version:%s>" %(mstbid,mbuildver))
    os.chdir(locdir)
    print("STBID:%s:Directory:%s" %(mstbid,locdir))
    wrotetoes=0 #Keep no. of records written to ES
    for txtfile in glob.glob("*.txt"): 
        inpf = open(txtfile,"r",encoding="utf8")
        #print("Processing %s" %(txtfile))
        #Initialize Variables
        crashstarted=0
        linenum=0
        mcrashat=""
        mprocess=""
        mpid=""
        mcrashtrace=""
        k=0
        for input_line in inpf:
            k += 1
            if (input_line[0:2] == "--"): #Comments
                continue
            try:
                x=int(input_line[0:2])
            except Exception as e:
                print("Something Wrong input_line=<%s>,k=%d,txtfile=%s>" %(input_line,k,txtfile))
                outstr="%s,%s,%s,%s,%s,%s,%s\n" %(mstbid,"NA","NA","NA","NA","NA",input_line.encode('utf-8'))
                outfcsv.write(outstr)
                #outfcsv.flush()
                if (write_android ==1): # Write to ES
                    writeesstr={
                        "STB_ID": mstbid,
                        "MESSAGE": "Something Wrong input_line=<%s>,k=%d,txtfile=%s>" %(input_line,k,txtfile)
                        }
                    esind.index(index='stb_androidlog',doc_type='_doc', body=writeesstr) #Not increasing wrotetoes as this is an exception                    
                continue #Go to Next Recrod
           
            
            Time = append_year_toevent(input_line[0:5])+input_line[5:14]
            if (append_year_toevent(input_line[0:5]) =="-1--1--1"):
                print("Input_line=%s, exiting" %(input_line))
                print("Input2_line=%s, exiting" %(input2_line))
                exit(-1)
            Seqno = input_line[15:20]#Just store, not sure what's the use of it
            pid = input_line[20:26]
            tid = input_line[26:31]
            priority = input_line[31:32]
            reststr= input_line[32:]
            tag = ""
            message = ""
            tag,message = get_tag_n_message(reststr)
            outstr="%s,%s,%s,%s,%s,%s,%s\n" %(mstbid,Time,pid,tid,priority,tag.replace(',','!'), message.replace(',','!').replace('ˋ','@'))
            outfcsv.write(outstr) #Just write SEquential events, no speicific processing
            if(pid == ''): #pid defined as long in ES
                pid=0
            if(tid == ''): #tid defined as long in ES
                tid=0
                
            if (write_android ==1): # Write to ES
                    mtime = Time
                    #print("writing to ES, time=%s" %(mtime))
                    writeesstr={
                        "STB_ID": mstbid,
                        "CRASH_TIME" : mtime,
                        "PID": float(pid),
                        "TID": float(tid),
                        "TAG": tag,
                        "MESSAGE": message
                        }
                    esind.index(index='stb_androidlog',doc_type='_doc', body=writeesstr) 
                    wrotetoes += 1
                    if(wrotetoes > 500): #During Development, not writing more than 500 records to ES, as it would slow down dev/testing
                        print("---->Logged more than 500 Android Entries to ES, quitting now, Disable this conidtion in production")
                        input("Press Any Key to continue")
                        exit(-1)
                    
            # try: Directly writing to CSV and ES (if required).
            
            
            if (priority.find('E')<0 ):#Except E no priroty is required at this stage
                #Verify if you need to write to csv
                if(crashstarted == 0 ):# Neither this log entry(record) needed for parsing NOR any crash trace on-hold
                    continue
                else: #Crash Trace is on hold, write to CSV, re-initialize variables except mstbid
                    #print("Writing to CSV1:priority=%s,k=%d,txtfile=%s" %(priority,k,txtfile))
                    if (write_crash ==1): #Should this record be written to ES?
                        #print("--->Writing to ES NOTE: Crash_time is NULL")
                        #time.sleep(2)
                        writecrstr={
                        "STBID" : mstbid,
                        "PROCESSING_DATE" : processing_date, 
                        "BUILD" : mbuildver,
                        "CRASH_TIME": mcrashat,
                        "PID" : float(mpid),
                        "PROCESS" :mprocess,
                        "FLAG" :"NA",
                        "PACKAGE" :"NA",
                        "FOREGROUND" :"NA",
                        "CRASH_TRACE" :mcrashtrace
                        }
                        esind.index(index='stb_devicecrashlog',doc_type='_doc', body=writecrstr)
                    
                    crstr = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" %(mstbid,processing_date,mcrashat,mpid,mprocess,"NA","NA","NA",mbuildver.replace(',','!').replace('\n','!'),mcrashtrace.replace(',','!').replace('\n','@'))
                    #print("crstr=%s" %(crstr))
                    outcrfcsv.write(crstr)
                    outcrfcsv.flush()
                    crashstarted=0
                    linenum=0
                    mcrashat=""
                    mprocess=""
                    mpid=""
                    mcrashtrace=""
                    
              
            if (tag.find('AndroidRuntime')<0): #Not Required
                if(crashstarted == 0 ):# Neither this log entry(record) needed for parsing NOR any crash trace on-hold
                    continue
                else: #Crash Trace is on hold, write to CSV, re-initialize variables except mstbid
                    if (write_crash ==1): #Should this record be written to ES?
                        #print("--->Writing to ES NOTE: Crash_time is NULL")
                        #time.sleep(2)
                        writecrstr={
                        "STBID" : mstbid,
                        "PROCESSING_DATE" : processing_date, 
                        "BUILD" : mbuildver,
                        "CRASH_TIME": mcrashat,
                        "PID" : float(mpid),
                        "PROCESS" :mprocess,
                        "FLAG" :"NA",
                        "PACKAGE" :"NA",
                        "FOREGROUND" :"NA",
                        "CRASH_TRACE" :mcrashtrace
                        }
                        esind.index(index='stb_devicecrashlog',doc_type='_doc', body=writecrstr)
                    #print("Writing to CSV1:tag=%s,k=%d,txtfile=%s" %(tag,k,txtfile))
                    crstr = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" %(mstbid,processing_date,mcrashat,mpid,mprocess,"NA","NA","NA",mbuildver.replace(',','!').replace('\n','!'),mcrashtrace.replace(',','!').replace('\n','@'))
                    #print("crstr=%s" %(crstr))
                    outcrfcsv.write(crstr)
                    outcrfcsv.flush()
                    crashstarted=0
                    linenum=0
                    mcrashat=""
                    mprocess=""
                    mpid=""
                    mcrashtrace=""
            if (priority.find('E')>=0 and tag.find('AndroidRuntime')>=0 ): #Process Crash sequence set of records
                
                if(message.find('FATAL EXCEPTION')>=0):
                    if(crashstarted ==0 ): #This is the first 
                        print("Crash Entry Started:k=%d,txtfile=%s,priority=%s,tag=%s" %(k,txtfile,priority,tag))
                        #input("Crash Entry Started.. Press any key ")
                        crashstarted=1
                        linenum=1
                        mcrashat=Time
                        continue
                    else: #Some WTF issue
                        continue
                if(crashstarted ==1 ): #Crash started AND crash log continuing
                    linenum += 1
                    if (linenum == 2): #Get PID & Process
                        mprocess = message[10:message.find(', PID:')]
                        mpid = message[message.find(', PID:')+6:len(message)]
                        #print("mprocess=%s,mpid=%s" %(mprocess,mpid))
                    else: #Rest of the entries are Crash traces
                        mcrashtrace = "%s\n%s" %(mcrashtrace,message)
                        linenum += 1 #Increasing does not matter
        
        #return #After first file processing Return
    return #Main Return
    
def process_dropbox_logs(locdir,mstbid):
    os.chdir(locdir)
    print(" -->processing dropbox files for STBID:%s:Directory:%s" %(mstbid,locdir))
    #time.sleep(2)
    #First Unzip if there are any gz files
    for gzfile in glob.glob("*.gz"):
        #print("%s:Uncompressing in dropbox folder %s" %(mstbid,gzfile))
        systemcmd = "7z x "+gzfile
        os.system(systemcmd)
    for gzfile in glob.glob("*.tgz"):
        #print("%s:Uncompressing in dropbox folder %s" %(mstbid,gzfile))
        systemcmd = "7z x "+gzfile
        os.system(systemcmd)
    for gzfile in glob.glob("*.7z"):
        #print("%s:Uncompressing in dropbox folder %s" %(mstbid,gzfile))
        systemcmd = "7z x "+gzfile
        os.system(systemcmd)
    for tarfile in glob.glob("*.tar"):
        systemcmd = "tar -xvf "+gzfile
        os.system(systemcmd)
   
    for txtfile in glob.glob("*.txt"): #Wrong naming Convention. txtfile read as txtfileline
        if not (txtfile.upper().find("SYSTEM_APP_CRASH")>=0 or txtfile.upper().find("DATA_APP_CRASH")>=0):
            print("%s:Dropboxfile:%s being passed out" %(mstbid,txtfile))
            continue #Go to next file
        
        #Initialize Variables
        crashstarted=0
        linenum=0
        mbuildver=""
        mcrashat=""
        mprocess=""
        mpid=""
        mcrashtrace=""
        mpackage=""
        mflags=""
        mforeground=""
        inpf = open(txtfile,"r",encoding="utf8")
        for input_line in inpf:
            if (len(input_line) == 1): #Empty Line Crash Started here
                #print("%s:Crashstarted drobox" %(mstbid))
                crashstarted = 1
                continue #Nothing to do in this linen
            if(crashstarted == 1): #Append to Crash separated by \n Simple
                mcrashtrace = mcrashtrace + input_line #\n will compe from previous line and separates two lines
                continue #Just Acrrue and go to next line
            #===== EXTRA PRECAUTION
            #THis case may araise in future, for now even Build is working fine
            #if(input_line.count(':')>1): #Should not be as per examples, if so, report
                #print("%s:<%s> has more than one separator.. ignoring" %(mstbid,input_line))
                #continue
            #===== EXTRA PRECAUTION
            
            coln=input_line[0:input_line.find(':')]
            colv=input_line[input_line.find(':')+2:len(input_line)]
            #print("--->Coln:%s,ColV:%s>" %(coln,colv))
            if(coln == 'Package' or coln == "Subject"):
                mpackage = colv.replace('\n','')
            elif(coln == 'PID'):
                mpid = colv.replace('\n','')
            elif(coln == 'Flags'):
                mflags=colv.replace('\n','')
            elif(coln == 'Process'):
                mprocess = colv.replace('\n','')
            elif(coln == 'Foreground'):
                mforeground = colv.replace('\n','')
            elif(coln == 'Build'):
                mbuildver = colv.replace('\n','')
            else:
                continue
        #Processing one File is over. Write to csv
        if(mpid == ''): #if blank make it zero to avoid problem writing to ES as a float
            mpid=0
            
        if (write_crash ==1): #Should this record be written to ES?
            #print("--->Writing to ES NOTE: Crash_time is NULL")
            #time.sleep(2)
            writecrstr={
            "STBID" : mstbid,
            "PROCESSING_DATE" : processing_date, 
            "PID" : float(mpid),
            "PROCESS" :mprocess,
            "FLAG" :mflags,
            "PACKAGE" :mpackage,
            "FOREGROUND" :mforeground,
            "CRASH_TRACE" :mcrashtrace
            }
            esind.index(index='stb_devicecrashlog',doc_type='_doc', body=writecrstr)
        crstr="%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" %(mstbid,processing_date,"NA",mpid,mprocess.replace('\n',''),mflags.replace('\n',''),mpackage.replace('\n',''),mforeground.replace('\n',''),mbuildver.replace('\n','!'),mcrashtrace.replace(',','!').replace('\n','!').replace('ˈ','#'))
        outcrfcsv.write(crstr)
        outcrfcsv.flush()
           
        inpf.close()
        
    return
    
def change_filname(PresentSTBLogs): # Change file names in this directory, if there is a space character in name of the file, change to "_"  
    #for tarfile in glob.glob("*.tar"):
    for tarfile in glob.glob("*.*"):
        filn=tarfile.replace(' ','_')
        shutil.move(tarfile,filn)
    return


#Main Program Starts Here

#Speciy locations, which will be parameters before deploying
#MainDirectory="F://Jio//Design//phase2//HugeData_01-Apr-2020//"
MainDirectory="/home/ubuntu/jiodevicecrash/"
MainFile="STBLogs.zip"
write_android=0 #Make it 1, if Android log to be written to ES, by default NOT written
write_crash=1 #By Default, crash details appended into ES

#First filetimestamp to append to directories and files
filetimestamp = "%s" %(datetime.now())
filetimestamp = filetimestamp[0:16]
filetimestamp = filetimestamp.replace(' ','_').replace(':','_')
#print(filetimestamp)
### ===>NOTE: If program needs to restart from middle, assging filetimestamp value here
#filetimestamp = 

if(not path.isfile(MainDirectory+MainFile)):
    print("%s+%s: File Does not Exist .. Exiting" %(MainDirectory,MainDirectory))
    exit(-1)
if ( os.path.isdir(MainDirectory+"STBLogs") or os.path.isdir(MainDirectory+'STBLogs_'+filetimestamp)):
    print("STBLogs directories Existing.. Exit")
    exit(-1)
 
#Get Processing Date
processing_date=datetime.now().strftime('%Y-%m-%d')
print("---->Processing date=%s" %(processing_date))



if(write_android == 1 or write_crash ==1): # If either to be written then only care for ElasticSearch
    esind=check_es_index()


 
#Uncompress the main file 
systemcmd = '7z x '+MainDirectory+MainFile
#print("Command for uncompress is %s" %(systemcmd))
os.system(systemcmd)
#Rename STBLogs, so that, next run of this program will go thro'
PresentSTBLogs = MainDirectory+"STBLogs_"+filetimestamp
shutil.move(MainDirectory+"STBLogs",PresentSTBLogs)
#Open Required CSV files
outf=PresentSTBLogs+"/WrAndroidlog_"+filetimestamp+".csv" #This is just to write logs in sequence, an intermediate, will be discared in future
print("outf=%s" %(outf))
outfcsv=open(outf,'w',encoding='utf-8')
outfcsv.write("STBID,Time,Pid,Tid,Priority,Tag,Message\n")
outcrf=PresentSTBLogs+"/CrAndroidlog_"+filetimestamp+".csv" #This is to Write All crash events
outcrfcsv=open(outcrf,"w",encoding='utf-8')
outcrfcsv.write("STBID,processing_date,CrashAt,Pid,Process,Flag,Package,Foreground,Build,CrashTrace\n")
outcrfcsv.flush()

#Now gunzip all the files in 
os.chdir(PresentSTBLogs)
os.system('pwd')
#input("Before Change of file name... PRESS ANY KEY TO CONTINUE")
change_filname(PresentSTBLogs) # Change file names in this directory, if there is a space character in name of the file, change to "_"
#input("After Change of file name... PRESS ANY KEY TO CONTINUE")
#systemcmd = "7z x "+PresentSTBLogs+"/RARSBKE00063383_V4.11.tgz"
systemcmd = "7z x "+PresentSTBLogs+"/*.tgz"
print("systemcmd=%s" %(systemcmd))
os.system(systemcmd)
#input("After unzipping *.tgz.. PRESS ANY KEY TO CONTINUE")
for tgzfile in os.listdir(PresentSTBLogs):
    if tgzfile.endswith(".tgz"): # Process all .tgz files
        systemcmd =  "7z x \"%s\"" %(tgzfile)
        os.system(systemcmd)
#input("After unzipping *.tgz 2nd time.. PRESS ANY KEY TO CONTINUE")

#exit(-1)



processedtar=0
for tarfile in glob.glob("*.tar"): # Process all .tar files
#for tarfile  in glob.glob("RBLSBGF10000531.tar"): # Process all .tar files
    print("=====>Processing tar file:%s" %(tarfile))
    processedtar += 1
    #print(tarfile)
    mstbid=tarfile[:-4]
    print("Processing STB:%s" %(mstbid))
    if (not os.path.isdir(PresentSTBLogs+"/tempdir")):
        print("Creating Temporary Directory to process <%s>" %(tarfile))
        os.mkdir(PresentSTBLogs+"/tempdir") #Note Process is in PresentSTBLogs directory
        print("--->Trying to move:%s/%s,%s,%s" %(PresentSTBLogs,tarfile,PresentSTBLogs,"/tempdir"))
        #shutil.copy(PresentSTBLogs+"/"+"\""+tarfile+"\"",PresentSTBLogs+"/tempdir") # move the log file to temporary directory
        systemcmd = "mv \"%s\" tempdir" %(tarfile)
        print("systemcmd=%s" %(systemcmd))
        os.system(systemcmd)
        #systemcmd = "tar -xvf "+PresentSTBLogs+"/tempdir/"+"\"+tarfile+"\"
        systemcmd = "tar -xvf "+PresentSTBLogs+"/tempdir/*"
        os.chdir(PresentSTBLogs+"/tempdir")
        print("Untar command is:%s" %(systemcmd))
        os.system(systemcmd)
        
        #First process logfiles in SDCARD folder and beneth
        os.chdir(PresentSTBLogs+"/tempdir/data/data/insight.tr069.client/cache/")
        if( path.isfile('sdcardlog.tar.gz')): #Sometimes this file not found, then ignore
            systemcmd = '7z x sdcardlog.tar.gz'
            os.system(systemcmd)
            systemcmd = 'tar -xvf sdcardlog.tar'
            os.system(systemcmd)
            process_logs(PresentSTBLogs+"/tempdir/data/data/insight.tr069.client/cache/sdcard/log/",mstbid)
            #input("%s:Process Logs Over.. Press any key")
            
            #exit(-1)
        else:
            print("--->%s:sdcardlog.tar.gz Not Found" %(mstbid))
            
        #Now Process in logs in dropbox folder
        os.chdir(PresentSTBLogs+"/tempdir/data/data/insight.tr069.client/cache/")
        if( path.isfile('dropboxlog.tar.gz')): #if this file not found, then ignore
            systemcmd = '7z x dropboxlog.tar.gz'
            os.system(systemcmd)
            systemcmd = 'tar -xvf dropboxlog.tar'
            os.system(systemcmd)
            process_dropbox_logs(PresentSTBLogs+"/tempdir/data/data/insight.tr069.client/cache/data/system/dropbox/",mstbid)
        else:
            print("%s:--->%s:dropbox.tar.gz Not Found" %(os.getcwd(),mstbid))
        #exit(-1)
        
        #Processing of this stb is over
        os.chdir(PresentSTBLogs)
        #print("=====> Put Pause here")
        #input("Count No. of Ocurrances for STB:%s.. press any Key.. ENSURE SUBDIRECTORY REMOVABLE.. " %(mstbid))
        shutil.rmtree(PresentSTBLogs+"/tempdir/")#Keep Last one for sometime
        #exit(-1)
    else:
        print("tempdir existing..exit")
        exit(-1)
        
print("Total tar files processed=%d" %(processedtar))
    
