#Arrive at index name based on data

import pandas as pd
import datetime
from datetime import datetime

print("Started at " , (datetime.now()))
input_data = pd.read_excel('STB_ANR_CR_DATA_25000.xlsx')
print("Excel Read")
input_col_Required = input_data[['Event Date']]
print("Required Column Extracted. Number of rows=%d" %(input_col_Required.count()))
input_col_Required.sort_values('Event Date',inplace = True)
print("Sorted Now Rows=%d" %(input_col_Required.count()))
#input_col_Required.drop_duplicates(subset = 'Event Date',keep = first, inplace = True)
input_col_Required.drop_duplicates(subset = 'Event Date', inplace = True)
print("Number of rows after dropping duplicates:%d" %(input_col_Required.count()))
print(input_col_Required['Event Date'].iloc[0].replace('T00:00:00',''))
index_name = "jiostb_"+input_col_Required['Event Date'].iloc[0].replace('T00:00:00','')
print("Index_Name = %s" %(index_name))
print("Completed at " , (datetime.now()))
#print(input_col_Required)