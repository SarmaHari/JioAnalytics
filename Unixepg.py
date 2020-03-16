def check_directories(parentdir):
    if(not os.path.isdir(parentdir)):
        print("Directory(%s) does not exists.. Exit" %(parentdir))
        exit(-1)



import sys
if (len(sys.argv) < 2):
    print("Usage: %s <Path>" %(sys.argv[0]))
    exit(-1)


import os
from os import path
import pandas as pd
from datetime import datetime
from datetime import date
from datetime import timedelta
import json
import urllib
import urllib.request
from urllib.error import HTTPError

Sourcedir = sys.argv[1]
check_directories(Sourcedir)
check_directories(Sourcedir+'/libraries/')
check_directories(Sourcedir+'/input/')
check_directories(Sourcedir+'/input/epg/')
#Import from default libraries OR from provided as part of program
sys.path.append(Sourcedir +'/libraries/') 

#First Get Channels list

file = urllib.request.urlopen('http://jiotv.data.cdn.jio.com/apis/v1.3/getMobileChannelList/get/?langId=2&os=android&devicetype=tv')
string = file.read().decode('utf-8')
json_obj = json.loads(string)

e=[]
for i in range(len(json_obj["result"])):
    e.append(json_obj["result"][i]["channel_id"])

f=pd.DataFrame(e)
f.to_csv(Sourcedir+'/input/'+"channel_ids.csv", index=False)

#print("Now reading csv from %s" %(os.getcwd()))
if( not path.isfile(Sourcedir+'/input/'+'channelids.csv')):
    print("File (%s) not found: Exiting" %(Sourcedir+'/input/'+'channelids.csv'))
    exit(-1)
a = pd.read_csv(Sourcedir+'/input/'+'channelids.csv',names = ["Channel_ID"])
newpath = 'input/epg' 
if not os.path.exists(newpath):
    os.makedirs(newpath)

from datetime import date
today = date.today()
today = str(today)

os.chdir(newpath)


# to specified directory  
newpath = today
if not os.path.exists(newpath):
    os.makedirs(newpath)

os.chdir(newpath)





b=[]
for i in range(len(a)):
  b.append("http://snoidcdnems06.cdnsrv.jio.com/jiotv.data.cdn.jio.com/apis/v1.3/getepg/get/?offset=0&channel_id="+str(a['Channel_ID'][i]))
d=pd.DataFrame(b)

e=[]
import json
import urllib
import urllib.request
from urllib.error import HTTPError
for i in range(len(d)):
    try:
        file = urllib.request.urlopen(d[0][i])
        c_id = a['Channel_ID'][i]
        string = file.read().decode('utf-8')
        json_obj = json.loads(string)
        with open(str(c_id)+'.json','w') as f:
            json.dump(json_obj, f)
    except HTTPError as err:
         c_id = a['Channel_ID'][i]
         e.append(c_id)
         
f=pd.DataFrame(e)
f =f.rename(columns={0: "Channel_ID"})
f.to_csv("channels_not_found.csv")