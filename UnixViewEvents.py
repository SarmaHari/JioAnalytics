#12-Feb-2020: Write to ES
#11-Feb-2020: WorkWeek implementation & NCCS
#03-Feb-2020: Input and Output directories added
#30-Jan-2020: Get TimeBand
#28-Jan-2020: NCCS data to ViewGenere changes started
#27-Jan-2020: STB 1.6.5 changes over
#23-Jan-2020: Changing to STB Version 1.6.5
#22-Jan-2020: If no XLTVE1,2,6 events for 30 minutes, consider TV Switched off - Implemented
#21-Jan-2020: If previous STB does not have any viewing events, the present STB first view is missing - Fixed
#23-Dec_2019: Putting Show start time, show end time and duration in ViewGenre & Print Unknown Channels dvbtemplate into a file
#18-Dec-2019: Printing minutes everywhere
#18-Dec-2019: Demographic Data being added to Genre
#17-Dec-2019: Duration in Genre is added



genrelogcnt=0 #This global variable to keep count of genrelog invocation


def week_of_the_day(dtx):
    #c = datetime.strptime(dtx, '%d-%m-%Y')
    #c = datetime.strptime(dtx, '%Y-%m-%d')
    c=dtx
    pmonth=c.strftime("%m") #Get month of the date
    first_day = c.replace(day=1)
    dom = c.day
    adjusted_dom = dom + first_day.weekday()
    weekno=int(ceil(adjusted_dom/7.0))
    return weekno
    

def time_diff(start, end):
    #print("In Time_diff")
    #print(start)
    #print(end)
    if isinstance(start, datetime_time): # convert to datetime
        assert isinstance(end, datetime_time)
        start, end = [datetime.combine(datetime.min, t) for t in [start, end]]
    if start <= end: # e.g., 10:33:26-11:15:49
        return end - start
    else: # end < start e.g., 23:55:00-00:25:00
        end += timedelta(1) # +day
        assert end > start
        return end - start

def Read_Two_Column_File(file_name):
    with open(file_name, 'r') as f_input:
        csv_input = csv.reader(f_input, delimiter=',', skipinitialspace=True)
        x = []
        y = []
        for cols in csv_input:
            x.append(cols[0])
            y.append(cols[1])
        f_input.close()

    return x, y


def decode_event(eventname):
    return{
        'AE6' :'App Crash',
        'DD1' : 'Flush/Upload statistics',
        'DD2' : 'Init & LifeCycle',
        'XLTVE1' : 'Channel Change',
        'XLTVE2' : 'Video Error',
        'XLTVE6' : 'Viewing Channel for the last 30 minutes',
        'XLTVE3' : 'Parental Pin set bu user',
        'XLTVE4' : 'Block Channel',
        'XLTVE5' : 'Favourite Channel',
        'XLTVE7' : 'Audio Language Selection',
        'XLTVE8' : 'Audio language Preference for all channels',
        'NE28' : 'MQQT Coonection Failure Information'
    }.get(eventname,'Unknown')

def Read_Five_Column_File(file_name):
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

def Read_Four_Column_File(file_name):
    with open(file_name, 'r', encoding='utf-8') as f_input:
        csv_input = csv.reader(f_input, delimiter=',', skipinitialspace=True)
        x = []
        y = []
        p = []
        q = []
        i = 0

        for cols in csv_input:
            x.append(cols[0])
            y.append(cols[1])
            p.append(cols[2])
            q.append(cols[3])
            i = i + 1
        f_input.close()

    return x, y, p, q

def channelidno(dbtemp , chanid, dbtempl): #Not required after STB-1.6.5
    retchanid=""
    i=0
    while(i<len(dbtemp)):
        if (dbtemp[i] == dbtempl) :
            return(chanid[i])
        i=i+1
    return (retchanid)

def getchancategory(channelid): #Get what is the category of the channelidno
    i=0
    #print("Channel2id length=%d\n" %(len(channel2id)))
    #print("Received channelid = %d" %(int(channelid)))
    while(i<len(channel2id)):
        #print("channel2id[%d]=%s and category = %s\n" %(i,channel2id[i],category2id[i]))
        #print("%d:%s,%s>" % (i, channel2id[i], channelid))
        #if (int(channelid) == 1116):
            #print("1116:channel2id[%d]=%s,%s" %(i,channel2id[i],channelid==channel2id[i]))
        #if(str(channel2id[i]) == str(channelid)):
        if(int(channel2id[i]) == int(channelid)):
            return category2id[i]
        i += 1
    print("channelid=%s did not find. Searched till %d\n" %(channelid,i))
    return(-1)


def parseparameters(str): #Separates parameters based on delimiter ","
    parm_list = str.split(",")
    return(parm_list[0],parm_list[1],parm_list[2],parm_list[3],parm_list[4],parm_list[5],parm_list[6])

def readepgfile(myear,mmonth,mday,channelid):
    #Get Json String from the directory where channels info is stored
    #Directory format is year_month_date
    #Channelid is the channel id
    retstr = ""
    syear = str(myear)
    smonth = str(mmonth)
    sday = str(mday)
    if(mday <=9):
        sday = "0"+sday
    if(len(smonth)<2):
        smonth = "0"+smonth
    #Do you have to do for month also

    directory = maindirectory+"/input/epg/year-month-day/"
    directory = directory.replace("year",syear).replace("month",smonth).replace("day",sday)
    #print("directory=%s\n" %(directory))
    if (not path.exists(directory)):
        print("Directory:%s Does not Exists CHeck" %(directory))
        return "NoDir"
    jsonfil = ""
    for file in os.listdir(directory):
        #print("file=%s" %(file))
        #if fnmatch.fnmatch(file, '*_' + str(channelid).strip() + '.json'):
        if fnmatch.fnmatch(file,'*'+ str(channelid).strip() + '.json'):
            jsonfil = directory + file
            break
    if (jsonfil == ""):
        return retstr
    #print("Jsonfile=%s\n",jsonfil)
    retstr = jsonfil
    f = open(jsonfil, "r")
    ln2 = f.readline()
    f.close()
    data2 = json.loads(ln2)
    retstr = data2['epg']
    return retstr

def old_getdemodata(stbid):
    area = ""
    gender = ""
    AgeGroup = ""
    Language = ""
    NCCS = ''
    df_res = df_csv[df_csv['STB'] == stbid]
    area = df_res['Area'].to_string(index=False, header=False)[1:]
    gender = df_res['Gender'].to_string(index=False, header=False)[1:]
    AgeGroup = df_res['AgeGroup'].to_string(index=False, header=False)[1:]
    Language = df_res['Language'].to_string(index=False, header=False)[1:]
    NCCS = df_res['NCCS'].to_string(index=False, header=False)[1:]
    return area,gender,AgeGroup,Language,NCCS
    
def getdemodata(stbid):
    area = ""
    gender = ""
    AgeGroup = ""
    Language = ""
    NCCS = ''
    df_res = df_csv[df_csv['STB'] == stbid]
    #print(df_res.count())
    area = df_res['Area'].to_string(index=False, header=False)[1:]
    if (area.find('ies(') > 1):
        print("No data found for STB:%s" %(stbid))
        area=""
        return area,gender,AgeGroup,Language,NCCS
    gender = df_res['Gender'].to_string(index=False, header=False)[1:]
    AgeGroup = df_res['AgeGroup'].to_string(index=False, header=False)[1:]
    Language = df_res['Language'].to_string(index=False, header=False)[1:]
    NCCS = df_res['NCCS'].to_string(index=False, header=False)[1:]
    return area,gender,AgeGroup,Language,NCCS
    

def createesindexviewhist(db_con): #Leave settings to default
    index_settings = {
    'mappings':{
     'properties' :{
      "AGE_GROUP": {"type": "long"},
          "CHANNEL_ID": {"type": "long"},
          "CHANNEL_NAME": {"type": "keyword"},
          "GENDER": {"type": "keyword"},
          "GENRE": {"type": "keyword"},
          "LOCATION": {"type": "keyword"},
          "NCCS": {"type": "keyword"},
          "PROGRAM_NAME": {"type": "keyword"},
          "PROCESSING_DATE": {"type": "date","format": "MM/dd/yyYY"},
          "SHOW_DURATION": {"type": "long"},
          "SHOW_ENDTIME": {"type": "keyword"},
          "SHOW_STARTTIME": {"type": "keyword"},
          "SPOKEN_LANGUAGE": {"type": "keyword"},
          "STB_ID": {"type": "keyword"},
          "TIMEBAND": {"type": "keyword"},
          "VIEW_DURATION": {"type": "double"},
          "VIEW_ENDTIME": {"type": "keyword"},
          "VIEW_STARTTIME": {"type": "keyword"},
          "WEEK_NO": {"type": "long"},
          "ATS": {"type": "long"},
          "IMPRESSION": {"type": "long"}
     }
    }
    }# Ends Index Settings
    print("Creating index viewhist")
    db_con.indices.create(index = "viewhist",body=index_settings)
    return

def writetoviewhist(viewstr,db_con,indexname):
    columns = viewstr.split(',')
    #print("Before insert")
    c = datetime.strptime(columns[1], '%Y-%m-%d')
    d= (datetime.strftime(c, "%m/%d/%Y"))
    #print(d)
    try: #Sometimes Channelid comes as unknown - If channel no. is not found in EPG
        columns[12] = int(columns[12])
    except Exception as e:
        columns[12] = -1
    if (viewstr.find('eries')>1):
        print("Exiting viewstr=>%s>" %(viewstr))
        exit(-1)
    #print("columns[11]=%s %.2f" %(columns[11],float(columns[11])))
        
    writestr={
    "AGE_GROUP": columns[19],
    "CHANNEL_ID":int(columns[12]),
    "CHANNEL_NAME":columns[6],
    "GENDER":columns[18],
    "GENRE": columns[8],
    "LOCATION": columns[17],
    "NCCS": columns[21],
    "PROGRAM_NAME": columns[7],
    "PROCESSING_DATE": d,
    "SHOW_DURATION": columns[15],
    "SHOW_ENDTIME": columns[14],
    "SHOW_STARTTIME": columns[13],
    "SPOKEN_LANGUAGE": columns[20],
    "STB_ID": columns[0],
    "TIMEBAND":columns[3],
    "VIEW_DURATION": columns[11],
    "ATS" : float(columns[11])/STB_COUNT,
    "IMPRESSION" : float(columns[11])/(STB_COUNT*24*60),
    "VIEW_ENDTIME": columns[5],
    "VIEW_STARTTIME": columns[4],
    "WEEK_NO": columns[2]
    }
    res = db_con.index(index=indexname,doc_type='_doc', body=writestr)
    return

def genrelog(detailsstr,jioepgid, csv): # Function for listing Program & Genre wise  data

    csvx=open(csv,"a")
    #genrelogcnt += 1
    #detailsstr = detailsstr.replace('\\n','')
    #print("record no:%d:<%s>" %(i,detailsstr))

    
    stb, fromx, tox, channel, duration, dvbtemplate, unnec = parseparameters(detailsstr)
    #28-Jan-2020: STB Data to be written to ViewGenere
    stbarea,stbGender,stbAgeGroup,stbLanguage,stbNCCS = getdemodata(stb)
    stbstr = "%s,%s,%s,%s,%s" % (stbarea, stbGender, stbAgeGroup, stbLanguage, stbNCCS) #Get ALl STB Data
    
    if(jioepgid == "Unknown" or jioepgid == 'Unknown2'):#STB 1.6.5. 
        csvnochannel.write("%s,%s\n" %(jioepgid,dvbtemplate))
        #return 0
        
    duration=float(duration)*60 #Comes in minutes from 18-Dec-2019
  
    try:
        fromxist = datetime.strptime(fromx, '%b %d %Y :%H:%M:%S.%f') + timedelta(hours=5.5)
    except Exception as e:
        print("==>Time=%s\n" %(fromx))
        print("--->Entrie input string is:%s\n" %(detailsstr))
    toxist = datetime.strptime(tox, '%b %d %Y :%H:%M:%S.%f') + timedelta(hours=5.5)

    presentdate = fromxist.date()
    #print("---->date=")
    #print(presentdate)
    result2 = readepgfile(presentdate.year, presentdate.month, presentdate.day, jioepgid)

    datechanged=0
    startedwatching = 0
    cumulativewatching=0

    while True:
        if (result2 == ""):
             outstr = "%s,%s,%s,%s,%s,%s,%s,%s,%s,Not Applicable,Not Applicable,%.2f,%s,,,,%s,%s\n" % (stb, presentdate,week_of_the_day(presentdate),gettimeband(datetime.strftime(fromxist, "%H:%M:%S")),datetime.strftime(fromxist,'%H:%M:%S'), datetime.strftime(toxist,'%H:%M:%S'), channel, "No Channel Info", "NotApplicable",(toxist-fromxist).total_seconds()/60,jioepgid,unnec,stbstr)
             if(outstr.find('\n')+1 < len(outstr)):
                print("--->1.%d,%d" %(outstr.find('\n'), len(outstr)))
                #print("<%s>" %(outstr))
                outstr=outstr.replace('\n','',1)
             csvx.write(outstr)
             writetoviewhist(outstr,es,'viewhist')
             return 0
             
        else:
            if(result2 =="NoDir"):
                outstr = "%s,%s,%s,%s,%s,%s,%s,%s,%s,Not Applicable,Not Applicable,%.2f,%s,,,,%s,%s\n" % (stb, presentdate,week_of_the_day(presentdate), gettimeband(datetime.strftime(fromxist, "%H:%M:%S")),fromxist, toxist, channel, "No Channel Info", "NotApplicable",(toxist-fromxist).total_seconds()/60,jioepgid,unnec,stbstr)
                if(outstr.find('\n')+1 < len(outstr)):
                    print("--->2.%d,%d" %(outstr.find('\n'), len(outstr)))
                    outstr=outstr.replace('\n','',1)
                csvx.write(outstr)
                writetoviewhist(outstr,es,'viewhist')
                print("No Directory:%s\n" %(detailsstr))
                return

        datechanged=0

        for value_dict2 in result2:
            showstarttime = value_dict2["showtime"] #Now showstart time is string
            showname = value_dict2["showname"].replace(",", "!")
            showendtime = datetime.strptime(value_dict2["endtime"], '%H:%M:%S').time()
            showendtime = value_dict2["endtime"]
            #xyz=value_dict2["showGenre"]
            
            #print("Value_dict2:%s and two is %s" %(xyz,xyz[2:-2]))
            genret = value_dict2["showGenre"]
            #convert genret from list to string and then remove leading [' and trailing ']
            genre=''
            for chara in genret:
                genre += chara
            genre=genre.replace('[\'','')
            genre=genre.replace('\']','')
            
            showstarttime=datetime.strptime(datetime.strftime(presentdate,'%Y-%m-%d')+" "+showstarttime, '%Y-%m-%d %H:%M:%S')
            showendtime = datetime.strptime(datetime.strftime(presentdate, '%Y-%m-%d') + " " + showendtime,'%Y-%m-%d %H:%M:%S')
            showdetstr="%s,%s,%s" %(value_dict2["showtime"],value_dict2["endtime"],value_dict2["duration"])


            if (value_dict2["endtime"] == "00:00:00"): # Check if Date Changed
                datechanged = 1
                showendtime = showendtime + timedelta(days=1)
            if(startedwatching == 0):
                if (fromxist >= showstarttime and fromxist <= showendtime):
                    startedwatching=1
                    if(showendtime<toxist): #User Continued to Watching
                        outstr = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%.2f,%s,%s,,%s\n" % (stb, presentdate, week_of_the_day(presentdate),gettimeband(datetime.strftime(fromxist, "%H:%M:%S")),datetime.strftime(fromxist,"%H:%M:%S"), datetime.strftime(showendtime,"%H:%M:%S"), channel, showname, genre, getchancategory(jioepgid),value_dict2["showCategory"],(showendtime-fromxist).total_seconds()/60,jioepgid,showdetstr,stbstr)
                        cumulativewatching = cumulativewatching + (showendtime-fromxist).total_seconds()
                        if(outstr.find('\n')+1 < len(outstr)):
                            print("--->3.%d,%d" %(outstr.find('\n'), len(outstr)))
                            outstr=outstr.replace('\n','',1)
                        csvx.write(outstr)
                        writetoviewhist(outstr,es,'viewhist')
                    else: #User Stopped Watching Here
                        outstr = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%.2f,%s,%s,%s,%s\n" % (stb, presentdate,week_of_the_day(presentdate), gettimeband(datetime.strftime(fromxist, "%H:%M:%S")),datetime.strftime(fromxist, "%H:%M:%S"),datetime.strftime(toxist,"%H:%M:%S"),channel,showname, genre,getchancategory(jioepgid), value_dict2[ "showCategory"],(toxist-fromxist).total_seconds()/60,jioepgid,showdetstr,unnec,stbstr)
                        if(outstr.find('\n')+1 < len(outstr)):
                            print("--->4.%d,%d" %(outstr.find('\n'), len(outstr)))
                            outstr=outstr.replace('\n','',1)
                        csvx.write(outstr)
                        writetoviewhist(outstr,es,'viewhist')
                        cumulativewatching = cumulativewatching + (toxist-fromxist).total_seconds()
                        if(int(cumulativewatching)-int(duration)>1): #One Second difference is fine, as we are getting in minutes and multiplyting with 60, there would be some gap
                            print("%s!=%s for %s\n" %(cumulativewatching,duration,detailsstr))
                        return 0
            else: #if(startedwatching == 0)
                if(showendtime>toxist ):#User Stopped Watching Here
                        outstr = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%.2f,%s,%s,%s,%s\n" % (stb, presentdate,week_of_the_day(presentdate), gettimeband(datetime.strftime(fromxist, "%H:%M:%S")),datetime.strftime(showstarttime,"%H:%M:%S"), datetime.strftime(toxist,"%H:%M:%S"), channel, showname, genre, getchancategory(jioepgid),value_dict2["showCategory"],(toxist-showstarttime).total_seconds()/60,jioepgid,showdetstr,unnec,stbstr)
                        if(outstr.find('\n')+1 < len(outstr)):
                            print("--->5.%d,%d" %(outstr.find('\n'), len(outstr)))
                            outstr=outstr.replace('\n','',1)
                        csvx.write(outstr)
                        writetoviewhist(outstr,es,'viewhist')
                        cumulativewatching = cumulativewatching +(toxist-showstarttime).total_seconds()
                        if(int(cumulativewatching)-int(duration)>1):
                            print("%s!=%s for %s\n" %(cumulativewatching,duration,detailsstr))
                        return 0
                else:
                    outstr = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%.2f,%s,%s,,%s\n" % (stb, presentdate,week_of_the_day(presentdate),gettimeband(datetime.strftime(fromxist, "%H:%M:%S")), datetime.strftime(showstarttime,"%H:%M:%S"), datetime.strftime(showendtime,"%H:%M:%S"), channel, showname, genre, getchancategory(jioepgid),value_dict2["showCategory"],(showendtime - showstarttime).total_seconds()/60,jioepgid,showdetstr,stbstr)
                    if(outstr.find('\n')+1 < len(outstr)):
                            print("--->6.%d,%d" %(outstr.find('\n'), len(outstr)))
                            outstr=outstr.replace('\n','',1)
                    csvx.write(outstr)
                    writetoviewhist(outstr,es,'viewhist')
                    cumulativewatching = cumulativewatching + (showendtime - showstarttime).total_seconds()

            if (datechanged == 1): # Check if Date Changed, This possibility will not be there if we are daily processing -- that in production
                presentdate = presentdate + timedelta(days=1)
                result2 = readepgfile(presentdate.year, presentdate.month, presentdate.day, jioepgid)
                if (result2 == "NoDir"):
                    print("No Directory:%s\n" % (detailsstr))
                datechanged = 0


def gettimeband(timestr): #based on Srinath's mail on 30-Dec
    rettimeband = ""
    hrstr = int(timestr[:2:])
    if (hrstr >=0 and hrstr <2): #Late Night
        timeband = "Late Night"
    elif (hrstr>=2 and hrstr <5): #Deadband
        timeband = "Deadband"
    elif (hrstr>=5 and hrstr < 9): #Early Morning
        timeband = "Early Morning"
    elif (hrstr>=9 and hrstr<12): #Morning
        timeband = "Morning"
    elif (hrstr >=12 and hrstr<17): #AFternoon
        timeband = "Afternoon"
    elif (hrstr>=17 and hrstr <20): #Evening
        timeband = "Evening"
    elif (hrstr>=20 and hrstr < 24): #Primetime
        timeband = "Primetime"
    else: #Will never come to this part
        timeband = "Unknown"
    return timeband

def getchannelsid_n_category() :
    #Get All channels id & Category and return both
    #Assumed Channel Category Does not differ much so put it a fixed location and read
    #In future, if required pass it a parameter for this function
    f=open(maindirectory+"/input/Channels.json","r")
    ln = f.readline()
    f.close()
    data = json.loads(ln)
    result = data['result']
    channelid =  []
    categoryid =  []
    for value_dict in result:
        channelid.append(value_dict["channel_id"])
        categoryid.append(value_dict["channelCategoryId"])
    return channelid , categoryid



def getamsmapping(): #get Channel name, amschannelid, Jiochannelid into a dataframe
    f = open(maindirectory+'/input/amsChannelMapping.json','r') #File location to be parameterized
    ln2 = f.read()
    f.close()
    #print(ln2)
    data2 = json.loads(ln2)
    amsmapping = pd.DataFrame(columns=['channelName','amsChannelId','jioTvChannelId'])
    for value_dict2 in data2['channels']:
        list1 = []
        #print("%s,%s,%s" % (value_dict2['channelName'], value_dict2['amsChannelId'], value_dict2['jioTvChannelId']))
        list1.append(value_dict2['channelName'])
        list1.append(value_dict2['amsChannelId'])
        list1.append(value_dict2['jioTvChannelId'])
        amsmapping = amsmapping.append(pd.Series(list1,index=['channelName','amsChannelId','jioTvChannelId']),ignore_index=True)

    return amsmapping
    
def getname_jioid(amschannelid): #Given Amschannelid return channel name and jioTvChannelId
    list1=''
    list1=amsmapping.loc[amsmapping['amsChannelId'] == amschannelid]
    if(not list1.empty):
        if (int(list1['jioTvChannelId'].to_string(index=False)) <=0):
            return list1['channelName'].to_string(index=False),'Unknown2'
        else:
            return list1['channelName'].to_string(index=False),list1['jioTvChannelId'].to_string(index=False)
    else:
        return 'Unknown','Unknown'

def check_directories(parentdir):
    if(not os.path.isdir(parentdir)):
        print("Directory(%s) does not exists.. Exit" %(parentdir))
        exit(-1)


## Main Program Starts from here

import sys
if (len(sys.argv) < 2):
    print("Usage: %s <Path>" %(sys.argv[0]))
    exit(-1)
maindirectory=sys.argv[1]


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

#Check if required directories are existing
check_directories(maindirectory)
check_directories(maindirectory+'/libraries/')
check_directories(maindirectory+'/input/')
check_directories(maindirectory+'/input/epg/')
check_directories(maindirectory+'/input/staging/')
check_directories(maindirectory+'/output/')

#Import from default libraries OR from provided as part of program
sys.path.append(maindirectory+'/libraries/') 
from elasticsearch import Elasticsearch


today = date.today()
yesterday = today - timedelta(days=1)
#SourceDir=sys.argv[1]
#targetfile="test1python.csv"
yesterday = "%s" %(yesterday)
try:
    os.remove(maindirectory+"/ouptuput/viewlog_"+yesterday+".csv")
    os.remove(maindirectory+"/output/eventslog_"+yesterday+".csv")
    os.remove(maindirectory+"/output/errorlog_"+yesterday+".csv")
    os.remove(maindirectory+"/output/viewgenre_"+yesterday+".csv")
    #os.remove(maindirectory+"output/nochannel.csv")
except OSError:
    pass

#Change  source and destination files  as required
try:
    csv1 = open(maindirectory+"/output/viewlog_"+yesterday+".csv", "w")
    csv2 = open(maindirectory+"/output/eventslog_"+yesterday+".csv", "w")
    csv3 = open(maindirectory+"/output/errorslog_"+yesterday+".csv", "w")
    generecsv = maindirectory+"/output/viewgenre_"+yesterday+".csv"
    csvnochannel = open(maindirectory+"/output/nochannel.csv","a")
    datafil = maindirectory+"/input/staging/stage_"+yesterday+".csv"
    csv4 = open(generecsv, "w")
except Exception as e:
    print("One or more required resources not available.. please check exit with -1")
    exit(-1)

if( not path.isfile(maindirectory+'/input/Demo_Loc_ncsdata.csv')):
    print("Demo_Loc_ncsdata.csv not available, exiting")
    exit(-1)
        
if( not path.isfile(datafil)):
    print("Main Datafile (%s) missing not available, exiting" %(datafil))
    exit(-1)
        
if( not path.isfile(maindirectory+'/input/Channels.json')):
    print("Channels.json not available, exiting")
    exit(-1)
if( not path.isfile(maindirectory+'/input/amsChannelMapping.json')):
    print("amsChannelMapping.json not available, exiting")
    exit(-1)
        
if( not path.isfile(maindirectory+'/input/Demo_Loc_ncsdata.csv')):
    print("Demo_Loc_ncsdata.csv not available, exiting")
    exit(-1)


#Open Elastic Search database  
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
if not es.ping():
    print("ElasticSearch Not running, start and rerun")
    exit(-1)
#Check if Required Index exists, if not create
if not(es.indices.exists('viewhist')):
    print('Creating Viewhist')
    createesindexviewhist(es)
else:
    print('Index Already Existing')

    #Read DVBTemplate& Channelids and Keep in memory
    #dvbtemp,chanid = Read_Two_Column_File("F:\\Jio\\02dec\\dvbtemplate_channelid.csv") # Not Required for STB 1.6.5

amsmapping = getamsmapping()
channel2id ,category2id = getchannelsid_n_category() #Get Channelid and its category. Keep in memory One time work

df_csv = pd.read_csv(maindirectory+'/input/Demo_Loc_ncsdata.csv') #Read ncsdata and keep data ready in arrays

#Get Active STB Count - Active means, watched atleast once on that day duration does not matter
df_stb_cnt = pd.read_csv(datafil, header=None, usecols=[0],names=['STB_ID'])
STB_COUNT = len(df_stb_cnt['STB_ID'].unique())
print("Unique STB_COUNT = %d" %(STB_COUNT))
if (STB_COUNT <= 0): # this happens only in case No data
    print("STB COUNT IS Zero or less..exiting")
    exit(-1)

stb,dt,event,jsonvar = Read_Four_Column_File(datafil)







i = 0
ochannel = " "
ostb = 0
odt = ""
otime = ""
oevent = ""
csv1stb = ""
csv1from = ""
csv1to = ""
csv1channel = "-1"
csv1channeltype = ""
viewingstarted = 0
errflg=0
prevevent=""
prevchan="cannotbe"
dvbtemplate=""
preventtime=""
jioepgid=""
csv1jioepgid=""
lastswitchedoff=1

#print("Global Variable genrelogcnt=%d" %(genrelogcnt))

datetype=0
while (i < len(event)):
    #if(i==19):
        #print("Exiting after 19 records processing")
        #exit(-1)
    if (i % 1000 == 0):
        print("Processing %dth record" %(i))
    #print("i=%d\n" %(i))
    if(i==0): #Date format is differing, bring date to standard format
        #print("About to try:\n")
        try:
           # print("in try dt=%s\n" %(dt[i]))
            dtjunk=datetime.strptime(dt[i],'%d-%m-%Y %H:%M:%S')
            datetype=1
        except Exception as e:
            pass
        print("datettype=%d\n" %(datetype))
    if (datetype == 1): #Convert to Regular format
        dt[i]=datetime.strftime(datetime.strptime(dt[i],'%d-%m-%Y %H:%M:%S'),'%b %d %Y @ %H:%M:%S.%f')


    #First Get date & time
    #print("1.dt[i]=%s\n" %(dt[i]))
    dt[i] = dt[i].replace(",", "", 1)
    #print("2.dt[i]=%s\n" % (dt[i]))
    if (len(dt[i])== 25): # This is required because date is coming as 1,2,3,.. not as 01,02,03..
        dt[i] = dt[i][:4] + '0' + dt[i][4:]
    x = dt[i]
    dt[i] = x[0:12]
    time = x[14:26]
    
    #print("3.dt[i]=%s time = %s\n" %(dt[i],time))
    #exit(-1)

    #Remove First and last character of JSON Variable
    jsonvar[i] = jsonvar[i][1:]
    jsonvar[i] = jsonvar[i][:-1]
    channelstr = ""

#First process Events and then Viewing patterns
    if (event[i]=="XLTVE5" or event[i] == "XLTVE4") : #Favorite and blocked channels, There can be multiple channels
        data = ast.literal_eval(jsonvar[i])
        for item in data["XLTV7"]:#STB 1.6.5 changes, now channel will come in XLTV1
            #channelstr = channelstr + "#" + "{XLTV5}".format(**item).replace("\n","").replace("\r","")
            channelstrX,jioepgid = getname_jioid("{XLTV9}".format(**item).replace("\n","").replace("\r","")) #STB 1.6.5 only channelid
            channelstr = channelstr + "#" +channelstrX
            
    
    if (event[i]=="XLTVE1" or event[i] =="XLTVE2" or event[i]=="XLTVE6") :#Change Channel & Video Error only one channel will be there
        try:
            data = json.loads(jsonvar[i])
            #print(data)
        except Exception as e:
            print("Probelm with jsondata:%s,event=%s,i=%d,data=%s\n" %(jsonvar[i],event[i],i,data))
            pass
        try:
            channelstr,jioepgid = getname_jioid(str(data["XLTV9"])) #STB 1.6.5 only channelid
            #print("%s:%s:%s" %(data["XLTV9"],channelstr,jioepgid))
            #exit(-1)
            #print("Channelstr=%s" %(channelstr))
        except Exception as e:
            print("XLTV9 Not Found,i=%d\n" %(i))
            pass
            
    if (event[i] =="XLTVE7") : #Setting Language for a channel
        data = json.loads(jsonvar[i])
        try:
            channelstrX,jioepgid = getname_jioid(data["XLTV9"])#STB 1.6.5
            #channelstr = data["XLTV5"]
            channelstr = channelstrX+":"+data["XLTV6"]
        except Exception as e:
            print("XLTV9 Not Found,i=%d\n" %(i))
            pass
        
    if (event[i] =="XLTVE3"):#Parental Key
        data = json.loads(jsonvar[i])
        channelstr = data["XLTV3"] #Parental key user set
        
    if (event[i] == "XLTVE8") :# Language Selection
            data = json.loads(jsonvar[i])
            channelstr = data["XLTV6"] # Language

    outstr="%s,%s,%s,%s%s,%s\n" %(stb[i],event[i],decode_event(event[i]),dt[i],time[:-4],channelstr)
    try:
     csv2.write(outstr) #write to Events log
    except Exception as e:
        print("Outstr=%s\n"%(outstr))

    # Now process for VIewing
    if (csv1stb == ""):  # First time
        csv1stb = stb[i]
        #stbarea, stbGender, stbAgeGroup, stbLanguage, stbNCCS_A, stbNCCS_B, stbNCCS_C, stbNCCS_D, stbNCCS_E, stbOccupation = getdemodata(stb[i])
    elif (csv1stb != stb[i]):
        preventtime = ""
        ##if (event[i][0:5] == "XLTVE"): 
        if (1==2): # 
            outstr = "%s,%s,%s,%s,%d,%s,%s\n" % (csv1stb, csv1from, "Abrupt Closure", csv1channel, 0,dvbtemplate, "NA")
            print("Abrupt Closure:dvbtemplate=%s>\n" %(dvbtemplate))
        else:
            if (csv1to == ''): #This data case RBLSBGF10000173,Jan 31, 2020 @ 19:38:29.000,DD1
                csv1to = csv1from
            if (csv1from != csv1to):
                datetime_object1 = datetime.strptime(csv1from, '%b %d %Y :%H:%M:%S.%f')
                try:
                    datetime_object2 = datetime.strptime(csv1to, '%b %d %Y :%H:%M:%S.%f')
                except Exception as e:
                    print("--->i=%d,csv1from=%s and csv1to=%s" %(i,csv1from,csv1to))
                    exit(-1)
                try:
                    outstr = "%s,%s,%s,%s,%.2f,%s,%s\n" % ( csv1stb, csv1from, csv1to, csv1channel, timedelta.total_seconds(datetime_object2 - datetime_object1)/60,dvbtemplate,"NA")
                except Exception as e:
                    print("csv1from=%s>csv1to=%s>%s\n" %(csv1from,csv1to,csv1channel))
                    print("%s,%s>\n" %(datetime_object1,datetime_object2))
                    exit(-1)
            else:  # Watched time is zero
                outstr = "%s,%s,%s,%s,%d,%s,%s\n" % (csv1stb, csv1from, csv1to, csv1channel, 0, dvbtemplate,"NA") #Why two same statements
                outstr = "%s,%s,%s,%s,%d,%s,%s\n" % (csv1stb, csv1from, csv1to, csv1channel, 0, dvbtemplate,"NA")
        csv1.write(outstr)

        if (csv1channel != "-1" ):  # User did not watch any channel,
            xy = genrelog(outstr, jioepgid,generecsv)
        csv1stb = stb[i]
        #stbarea, stbGender, stbAgeGroup, stbLanguage, stbNCCS_A, stbNCCS_B, stbNCCS_C, stbNCCS_D, stbNCCS_E, stbOccupation = getdemodata(stb[i])
        csv1from = ""
        csv1to = ""
        csv1channel = "-1"
        csv1channeltype = ""
        viewingstarted = 0
        errflg = 0

    #If XLTVE1,2,6 events, see previous event_time, if difference is more than 30 minutes, write that data - It is considered TV Switched off
    if (event[i] == "XLTVE1" or event[i] == "XLTVE2" or event[i] == "XLTVE6"):
        if (csv1from != ""): #Atleast one viewing happend and this one is second one
            #print("--->i=%d: Entered into 2nd if values %s,%s:%s and lastswitchedoff=%d" %(i,csv1from,dt[i] , time,lastswitchedoff))
            presenttime=""
            switchedoff=""
            presenttime = '%s %s' %(dt[i],time[:-4])
            try:
                presenttime = datetime.strptime(presenttime,'%b %d %Y %H:%M:%S')
            except Exception as e:
                print("Excption in present time conversion:i=%d, presenttime=%s-->%s:%s" %(i,presenttime,dt[i],time[:-4]))
                exit(-1)
            if (preventtime!=""): 
                eventtimediff = time_diff(preventtime,presenttime)
                #print("--->i=%d: Entered into 3rd if" %(i))
                #print(eventtimediff.days,eventtimediff.seconds)
                if(eventtimediff.days > 0 or eventtimediff.seconds >1800): #TV Switched off
                    #print("--->i=%d: Entered into 4th if csv1from=%s,csv1to=%s" %(i,csv1from,csv1to))
                    #print("Switched off at %d:%s,%s,%s,%s" %(i,csv1from,csv1to,csv1channel[:csv1channel.find("(")],dvbtemplate))
                    
                    #print("csv1to=%s,lastswitchedoff=%d" %(csv1to,lastswitchedoff))
                  
                    if(csv1to == ""): #if only one event of viewing csv1to not yet set
                        #print("setting csv1to to %s" %(csv1from))
                        csv1to = csv1from
                    datetime_object1 = datetime.strptime(csv1from, '%b %d %Y :%H:%M:%S.%f')
                    datetime_object2 = datetime.strptime(csv1to, '%b %d %Y :%H:%M:%S.%f')
                    if (csv1from == csv1to):  # if csv1from and csv1to are same sometimes timedelta.total_seconds NOT printing zero
                        #print("i=%d,printing zero duration" %(i))
                        outstr = "%s,%s,%s,%s,%d,%s,%s,\n" % (csv1stb, csv1from, csv1to, csv1channel, 0,dvbtemplate ,"Switched Off")
                    else: #if (csv1from == csv1to)
                            outstr = "%s,%s,%s,%s,%.2f,%s,%s,\n" % (csv1stb, csv1from, csv1to, csv1channel, timedelta.total_seconds(datetime_object2 - datetime_object1)/60,dvbtemplate,"Switched Off")
                    
                    csv1.write(outstr)
                    xy=genrelog(outstr, jioepgid,generecsv)
                    #As the TV switched off reinitialize all variables except STB number
                    csv1from = dt[i] + ":" + time
                    csv1to=csv1channel=dvbtemplate=""
                    viewingstarted=0
                    csv1channel = "-1"
                   #print("Switched off at %d:%s,%s,%s,%s" %(i,csv1from,csv1to,csv1channel,dvbtemplate))
                    switchedoff="1"
                    lastswitchedoff=1
                else:
                    lastswitchedoff=0
            else:
                lastswitchedoff=0
            if(switchedoff!=""):
                switchedoff =""
                preventtime = presenttime
            else:
                preventtime = presenttime
        else: #of if (csv1from != "")
            #lastswitchedoff=0 
            presenttime = '%s %s' %(dt[i],time[:-4])
            presenttime = datetime.strptime(presenttime,'%b %d %Y %H:%M:%S')
            preventtime = presenttime
    #Processing of TVSwitched off is over

#Start Processing Channel Change Event
    if (event[i] == "XLTVE1"):
        data = json.loads(jsonvar[i])
        try:
            channel,jioepgid = getname_jioid(str(data["XLTV9"])) #Channelid from STB 1.6.5
        except Exception as e:
            print("i=%d:XLTVE1:XLTV9 Data Not found" %(i) )
            channel = "NODATAFOUND"
            pass
        dvbtemplate = data["XLTV1"]
        if (viewingstarted == 0):
            viewingstarted = 1
        if (csv1channel == channel):  # Some exception in the data NOTE: No Exception XLTVE1 can result in going to same channel
            csv1to = dt[i] + ":" + time
        elif (csv1channel == "-1"):  # First channel change after switching on
            csv1channel = channel
            csv1jioepgid =  jioepgid
            csv1from = dt[i] + ":" + time
        elif (csv1channel != channel):  # Channel Changed to a new channel
            csv1to = dt[i] + ":" + time
            datetime_object1 = datetime.strptime(csv1from, '%b %d %Y :%H:%M:%S.%f')
            datetime_object2 = datetime.strptime(csv1to, '%b %d %Y :%H:%M:%S.%f')
            if (csv1from == csv1to):  # if csv1from and csv1to are same sometimes timedelta.total_seconds NOT printing zero
                outstr = "%s,%s,%s,%s,%d,%s,%s,\n" % (csv1stb, csv1from, csv1to, csv1channel, 0,dvbtemplate ,"NA")
            else: #if from and to have some time gap
                    outstr = "%s,%s,%s,%s,%.2f,%s,%s,\n" % (csv1stb, csv1from, csv1to, csv1channel, timedelta.total_seconds(datetime_object2 - datetime_object1)/60,dvbtemplate,"NA")
                    #print("1.%d\n" %(timedelta.total_seconds(datetime_object2 - datetime_object1)))
                    #print("2.%.2f\n" %(timedelta.total_seconds(datetime_object2 - datetime_object1)/60))
            csv1.write(outstr)
            xy=genrelog(outstr,csv1jioepgid, generecsv)
            csv1channel = channel
            csv1jioepgid=jioepgid
            # print("Assigned Channel %s\n" %(csv1channel))
            csv1from = dt[i] + ":" + time
#Processing Channel Change Event is over

#once program stablized, think of combining XLTVE1 & XLTVE2 processing, for now leave it
    if (event[i] == "XLTVE2"):  # Viewing Error Still considered user viewing the channel
        #print("--->i=%d:%s" %(i,dt[i] + ":" + time))
        if (viewingstarted == 0):  # Landing Channel has viewing Error, handle later
            # outstr = "%s at %s:%s Landing CHannel has viewing error<===> handle this in program \n" %(csv1stb,dt[i],time[i])
            # csv3.write(outstr)
            viewingstarted = 1
            data = json.loads(jsonvar[i])
            channel,jioepgid = getname_jioid(str(data["XLTV9"])) #Channelid from STB 1.6.5
            dvbtemplate = data["XLTV1"]
        if (csv1channel == "-1"):
            csv1from = dt[i] + ":" + time
            #print("Entered %s from is %s\n" % (csv1stb, csv1from))
            csv1channel = channel
            csv1jioepgid =  jioepgid
        elif (channel != csv1channel):  # If they are same, nothing to do
            csv1to = dt[i] + ":" + time
            #print("3. %s,%s\n" % (csv1from, csv1to))
            datetime_object1 = datetime.strptime(csv1from, '%b %d %Y :%H:%M:%S.%f')
            datetime_object2 = datetime.strptime(csv1to, '%b %d %Y :%H:%M:%S.%f')
            if (csv1from == csv1to):  # if csv1from and csv1to are same sometimes timedelta.total_seconds NOT printing zero
                outstr = "%s,%s,%s,%s,%d,%s,%s,\n" % (csv1stb, csv1from, csv1to, csv1channel, 0, dvbtemplate,"NA")
            else:
                outstr = "%s,%s,%s,%s,%.2f,%s,%s\n" % (csv1stb, csv1from, csv1to, csv1channel, timedelta.total_seconds(datetime_object2 - datetime_object1)/60,dvbtemplate, "NA")
            csv1.write(outstr)
            xy = genrelog(outstr, csv1jioepgid,generecsv)
            csv1channel = channel
            csv1jioepgid=jioepgid
            csv1from = dt[i] + ":" + time
        else:
            csv1to = dt[i] + ":" + time
            #print("--->csv1to=%s" %(csv1to))
#Processing XLTVE2 is over 

#Process XLTVE6 events
    if (event[i] == "XLTVE6"):  # More than thirty minutes of viewing

        data = json.loads(jsonvar[i])
        try:
            #channel = data["XLTV5"]
            channel,jioepgid = getname_jioid(str(data["XLTV9"])) #Channelid from STB 1.6.5
            dvbtemplate = data["XLTV1"]
            timetosubtract = data["XLTV4"]
        except Exception as e:
            channel = "Not Found"
            dvbtemplate = "NA"
            timetosubtract = 1800

        #print("prevchannel=%s, csv1channel=%s,channel=%s,%s" % (prevchan, csv1channel, channel,dt[i] + ":" + time))
        if(csv1channel == channel ) :
                csv1to = dt[i] + ":" + time
            #print("---> channel=%s, csv1to = %s\n" %(channel,csv1to))
        else : ## Channel Changed
            if (errflg == 0 ) : #Event XLTVE6, channels not same and first time a new channel came==>#Without user changing suddenly XLTVE6 came for first time
                if( viewingstarted == 0): # This happens, for that STB if the first event itself is XLTVE6 without XLTVE1 Refer Data of RBLSBJF20006698 on Jan 6 2020 at 15:01
                    #print("First Viewing Event itself XLTVE6-- Process i = %d" %(i))
                    viewingstarted = 1
                    csv1channel = channel
                    csv1to = dt[i] + ":" + time
                    csv1from = datetime.strptime(csv1to, '%b %d %Y :%H:%M:%S.%f') - timedelta(milliseconds=timetosubtract)
                    csv1from = csv1from.strftime('%b %d %Y :%H:%M:%S.%f')
                else: #of  if( viewingstarted == 0)
                #First Print Previous data
                    try:
                        datetime_object1 = datetime.strptime(csv1from, '%b %d %Y :%H:%M:%S.%f')
                        datetime_object2 = datetime.strptime(csv1to, '%b %d %Y :%H:%M:%S.%f')
                        outstr = "%s,%s,%s,%s,%.2f,%s,%s\n" % (csv1stb, csv1from, csv1to, csv1channel,timedelta.total_seconds(datetime_object2 - datetime_object1)/60,
                                                             dvbtemplate, "NA")
                    except Exception as e:
                        print("In Cond4.1,i=%d,csv1from=%s,csv1to=%s,viewstarted=%d\n" %(i,csv1from,csv1to,viewingstarted))
                        continue

                    #print("===>outstr=%s\n" %(outstr))
                    csv1.write(outstr)
                    xy = genrelog(outstr, jioepgid,generecsv)
                
                csv1to = dt[i] + ":" + time
                csv1from = datetime.strptime(csv1to, '%b %d %Y :%H:%M:%S.%f') - timedelta(milliseconds=timetosubtract)
                csv1from = csv1from.strftime('%b %d %Y :%H:%M:%S.%f')
                #print("In Cond3\n")
                errflg=1  # Now no longer sudden
                outstr=("%s User must be watching Channel %s but XLTVE6 at %s shows as %s \n" %(csv1stb,csv1channel,csv1to,channel))
                csv3.write(outstr)
                #2-Dec-2019 as per Suresh in such case ignore previous channel and consider 30 minute event chanel as the mainone
                prevchan = channel
                csv1channel = channel
            else: #OF if (errflg == 0 )
                if(viewingstarted !=0): #THis condition came because we introduced 30 minutes consider switch off case
                    #print("In Cond4\n")
                    #print("---> else errflg=0,%s,%s" %(csv1from, csv1to))
                    try:
                        datetime_object1 = datetime.strptime(csv1from, '%b %d %Y :%H:%M:%S.%f')
                        datetime_object2 = datetime.strptime(csv1to, '%b %d %Y :%H:%M:%S.%f')
                        outstr = "%s,%s,%s,%s,%.2f,%s,%s\n" % (csv1stb, csv1from, csv1to, csv1channel,timedelta.total_seconds(datetime_object2 - datetime_object1)/60,
                                                             dvbtemplate, "NA")
                    except Exception as e:
                        print("In Cond4.2,i=%d,csv1from=%s,csv1to=%s,viewingstarted=%d\n" %(i,csv1from,csv1to,viewingstarted))
                        continue

                    #print("===>outstr=%s\n" %(outstr))
                    csv1.write(outstr)
                    xy = genrelog(outstr, jioepgid,generecsv)
                    csv1to = dt[i] + ":" + time
                    outstr = ("%s:User must be watching Channel %s but XLTVE6 at %s shows as %s \n" % (csv1stb,csv1channel, csv1to, channel))
                    csv3.write(outstr)
                    prevchan = csv1channel
                    csv1channel = channel
                    errflg=1
                    csv1from = datetime.strptime(csv1to, '%b %d %Y :%H:%M:%S.%f') - timedelta(milliseconds=timetosubtract)
                    csv1from = csv1from.strftime('%b %d %Y :%H:%M:%S.%f')
                    #print("In Cond4 reset variables errflg=%d,csv1from=%s,csv1to=%s\n" %(errflg,csv1from,csv1to))
                else: #errflg!=0 and viewinstarted=0
                    viewingstarted = 1
                    csv1channel = channel
                    csv1to = dt[i] + ":" + time
                    csv1from = datetime.strptime(csv1to, '%b %d %Y :%H:%M:%S.%f') - timedelta(milliseconds=timetosubtract)
                    csv1from = csv1from.strftime('%b %d %Y :%H:%M:%S.%f')
                    
        #input("Press Enter to continue...")

    prevevent=event[i]
    timetosubtract = 0
    i = i + 1  # move increment as the last step


if (csv1to !=""):
    if (csv1from != csv1to):
        #print("===>%s,%s\n" % (csv1from, csv1to))
        datetime_object1 = datetime.strptime(csv1from, '%b %d %Y :%H:%M:%S.%f')
        datetime_object2 = datetime.strptime(csv1to, '%b %d %Y :%H:%M:%S.%f')
        outstr = "%s,%s,%s,%s,%.2f,%s,%s\n" % ( csv1stb, csv1from, csv1to,csv1channel , timedelta.total_seconds(datetime_object2 - datetime_object1)/60, dvbtemplate,"NA")
    else:  # Watched time is zero
        outstr = "%s,%s,%s,%s,%d,%s,%s\n" % (csv1stb, csv1from, csv1to, csv1channel, 0, dvbtemplate,"NA")
    csv1.write(outstr)
    xy = genrelog(outstr,jioepgid,generecsv)
else:#if (csv1to !="") this happens when last record is XLTVE1,2,6
    csv1to = '%s :%s.000'  %(dt[i-1],time[:-4])#i-1 the last record processsed
    if (csv1from != csv1to):
            #print("===>%s,%s\n" % (csv1from, csv1to))
            datetime_object1 = datetime.strptime(csv1from, '%b %d %Y :%H:%M:%S.%f')
            datetime_object2 = datetime.strptime(csv1to, '%b %d %Y :%H:%M:%S.%f')
            outstr = "%s,%s,%s,%s,%.2f,%s,%s\n" % ( csv1stb, csv1from, csv1to,csv1channel , timedelta.total_seconds(datetime_object2 - datetime_object1)/60, dvbtemplate,"NA")
    else:  # Watched time is zero
        outstr = "%s,%s,%s,%s,%d,%s,%s\n" % (csv1stb, csv1from, csv1to, csv1channel, 0, dvbtemplate,"NA")

    csv1.write(outstr)
    xy = genrelog(outstr,jioepgid,generecsv)
csv1.close()
csv2.close()
csv3.close()
csv4.close()
