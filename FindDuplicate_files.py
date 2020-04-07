#Program to find duplicate files in a directory
#This program does not delete automatically
#Ensure to verify once again before deleting duplicate


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
import subprocess

Maindirectory ="F:\\Jio\\Design\\phase2\\HugeData_01-Apr-2020\\STBLogs_2020-04-06_23_12\\tarfils\\" # Which could be a parameter
os.chdir(Maindirectory)
#if possible/need be write to a different directory, ofcourse no issue
matchcsv=open("filematch.csv","w")
matchcsv.write("file1,file2\n")
#Writing Unmatched files, if required put here

processed_files = []

for file1 in os.listdir(Maindirectory):
    if (file1 == 'filematch.csv'): #Do not do anything, just go to next file, it is the csv that keeps matching files, it keeps changing
        continue 
    file1siz = os.stat(file1).st_size
    #print("Processing:%s, size:%d" %(file1,file1siz))
    for file2 in os.listdir(Maindirectory):
        if(file1 == file2  or file2 == 'filematch.csv' or file2 in processed_files or file1siz != os.stat(file2).st_size): # Do not compare same files, nor if already processed, nor size is NOT same
            continue
        
        systemcmd = 'diff -b %s %s' %(file1 , file2)
        try:
            psoutput = subprocess.check_output(systemcmd,shell='True')
            matchcsv.write("%s,%s\n" %(file1,file2))
        except Exception as e:
        #Writing Unmatched files, if required put here
           pass
    
    processed_files.append(file1)
 
matchcsv.close()