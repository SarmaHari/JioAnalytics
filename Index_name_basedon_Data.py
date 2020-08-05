#04-05 Aug 2020 Data template changed so change program
#Arrive at index name based on data

import pandas as pd
import datetime
from datetime import datetime

print("Started at " , (datetime.now()))
input_data = pd.read_csv('XPSE9nXPSE10_30_July_20-Part-1.csv')
#input_data = pd.read_csv('Input.csv')
print("Excel Read")
#input_col_Required = input_data[['dc_stb_event_po.Event Date']]
#30-Jul-2020 data sent on 4-Aug-2020 has a different column Name
input_col_Required = input_data[['dc_stb_event_po.event_date']]
print("Required Column Extracted. Number of rows=%d" %(input_col_Required.count()))
input_col_Required.sort_values('dc_stb_event_po.event_date',inplace = True)
print("Sorted Now Rows=%d" %(input_col_Required.count()))
input_col_Required.drop_duplicates(subset = 'dc_stb_event_po.event_date', inplace = True)
print("Number of rows after dropping duplicates:%d" %(input_col_Required.count()))
print(input_col_Required['dc_stb_event_po.event_date'].iloc[0].replace('T00:00:00',''))
index_name = "jiostb_"+input_col_Required['dc_stb_event_po.event_date'].iloc[0].replace('T00:00:00','')
print("Index_Name = %s" %(index_name))
print("Completed at finding index name" , (datetime.now()))
#04-Aug-2020, Extract required columns
input_data = pd.read_csv('XPSE9nXPSE10_30_July_20-Part-1.csv')
#input_data = pd.read_csv('Input.csv')
input_cols_Required = input_data[['dc_stb_event_po.as5','dc_stb_event_po.as7','dc_stb_event_po.di8','dc_stb_event_po.dt_raw','dc_stb_event_po.mi1','dc_stb_event_po.name']]
input_cols_Required .to_csv('Before_Removing_Duplicates.csv',index = False)
print("Completed Selecting Required Columns" , (datetime.now()))
input_cols_Required.sort_values(['dc_stb_event_po.di8','dc_stb_event_po.dt_raw','dc_stb_event_po.name','dc_stb_event_po.as5'],inplace = True)
input_cols_Required .to_csv('After_Sorting.csv',index = False)
input_cols_Required2 = input_cols_Required.drop_duplicates(['dc_stb_event_po.di8','dc_stb_event_po.dt_raw','dc_stb_event_po.name','dc_stb_event_po.as5'])
input_cols_Required2 .to_csv('After_Removing_Duplicates.csv',index = False, sep = ',')
print("Removed Duplicate COlumns" , (datetime.now()))