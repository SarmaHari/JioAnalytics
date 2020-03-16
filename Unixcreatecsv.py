def check_directories(parentdir):
    if(not os.path.isdir(parentdir)):
        print("Directory(%s) does not exists.. Exit" %(parentdir))
        exit(-1)

import sys
if (len(sys.argv) < 2):
    print("Usage: %s <Path>" %(sys.argv[0]))
    exit(-1)

import numpy as np
import pandas as pd
import xlrd
import fnmatch
import os
from os import path
import operator
import csv
from datetime import datetime
from datetime import date
import sys
from datetime import timedelta

SourceDir=sys.argv[1]
check_directories(SourceDir)
check_directories(SourceDir+'/libraries/')
check_directories(SourceDir+'/input/')
check_directories(SourceDir+'/input/staging/')


sys.path.append(SourceDir +'/libraries/') 

today = date.today()
#print(today)
yesterday = today - timedelta(days=1)
print(yesterday)
print("yesterday=%s" %(yesterday))

#targetfile="test1python.csv"
yesterday = "%s" %(yesterday)
if (not path.exists(SourceDir+'/input/'+yesterday)):
    print("Source Directory(%s) does not exist" %(SourceDir+'/input/'+yesterday))
    exit(-1)
firsttime=0
#SourceDir = SourceDir+"*.csv"
for file in os.listdir(SourceDir+'/input/'+yesterday):
    print("File=%s\n" %(file))
    data=pd.read_csv(SourceDir+'/input/'+yesterday+"/"+file)

    data=data[['DI8','DT','Name','AS5','AS7']]
    if (firsttime == 0 ):
        data.to_csv(SourceDir+'/input/staging/'+"stage1_"+yesterday+".csv", index=False)
        firsttime = 1
    else:
        data.to_csv(SourceDir+'/input/staging/'+"stage1_"+yesterday+".csv", mode='a', header=False,index=False)
        
data=pd.read_csv(SourceDir+'/input/staging/'+"stage1_"+yesterday+".csv")
data['converted_date']=''

for i in range(len(data)):
    if(data['AS7'][i][:6]=='1.0.17'):
        data['AS7'][i]='1.0.17'
    data['converted_date'][i] = datetime.strptime(data['DT'][i], '%b %d, %Y @ %H:%M:%S.000')

data.drop(data.loc[data['AS7'] =='1.0.17'].index, inplace=True)
data1=data.sort_values(by=['DI8','converted_date'])
data1.to_csv(SourceDir+'/input/staging/'+"stage_"+yesterday+".csv", mode='w', header=False,index=False)