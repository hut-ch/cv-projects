from datetime import datetime
import pandas as pd
import boto3
import codecs

## pass in environment variables from source system into python script
s3-bucket = v_s3_bucket
s3-key = v_s3_folderpath +v_filename

client = boto3.client('s3')
response = client.get_object(Bucket=s3-bucket, Key=s3-key) 

csv_headers = pd.read_csv(response.get("Body"), nrows=0).columns.tolist()
csv_headers_grid=[]
csv_headers_print=''
table=[]
new_column=[]

for col in csv_headers: 
  header = [col]
  if col == 'price' or col == 'sale_price':
    new_column = [col,'Numeric','38','2','','','']
  else:
    new_column = [col,'Text','20000','','','',''] 
    
  table += [new_column]
  csv_headers_grid += [header]
  csv_headers_print += header[0]+','
  

# check that csv file has the required columns if not add them to the table so that it still loads
# some of the older files don't contain the sale_price
# if new columns are required that get added then add to this check

missing_cols = ''
missing_flag = 0

if 'product_id' not in csv_headers:
  missing_cols +='product_id,'
  missing_flag = 1
  table += [['product_id','Text','2000','','','','']]
if 'price' not in csv_headers:
  missing_cols +='price,'
  missing_flag = 1
  table += [['price','Numeric','38','2','','','']] 
if 'sale_price' not in csv_headers:
  missing_cols +='sale_price,'
  missing_flag = 1
  table += [['sale_price','Numeric','38','2','','','']]


#update source system variables to be used in later job steps
context.updateGridVariable('gv_csv_headers', csv_headers_grid)
context.updateGridVariable('gv_load_table', table)
context.updateVariable('v_missing_cols_flag', missing_flag)
context.updateVariable('v_missing_cols', missing_cols)

print('File Columns: '+csv_headers_print[:-1])
if missing_flag >0:
  print('\nThe following columns are missing so they will be added to table by default: '+ missing_cols[:-1])
