from datetime import datetime
import pandas as pd
import numpy as np
import boto3

####################################################################################################
# Declare and Initialise Variables
####################################################################################################

# Set Bucket, Folder and File details for source and output files from environment variables
v_bucket = v_s3_location

v_source_key = v_s3_folder_source+'/'+v_source_file

v_xml_key = v_s3_folder_working+'/' + v_out_xml_file
v_dcn_key = v_s3_folder_working+'/' + v_out_dcn_file
v_dcn_assign_key = v_s3_folder_working+'/' + v_out_dcn_file_assign

# Create XML header data before each employee is added
xmldata = (
  '<?xml version="1.0" encoding="utf-8"?>\n'
  '<employees>\n' 
)

# Create DCN header data before each employee is added
dcndata = (
    '; Destination store\r\n'
    '[RUNTIME]\r\n'
    'DATE='+v_date_now.strftime("%Y-%m-%d")+'\r\n'
    'TIME='+v_date_now.strftime("%H:%M:%S")+'\r\n'
    'ALLOWERRORS=FALSE\r\n'
    '[TASK.1]\r\n'
    'SERVER=EMPLOYEE\r\n'
    'OPERATION=AddUpdate\r\n'
    'MODULE=EMPLOYEE\r\n'
    '[DATA.1]'
)

dcndata_assign = (
    '; Destination store\r\n'
    '[RUNTIME]\r\n'
    'DATE='+v_date_now.strftime("%Y-%m-%d")+'\r\n'
    'TIME='+v_date_now.strftime("%H:%M:%S")+'\r\n'
    'ALLOWERRORS=FALSE\r\n'
    '[TASK.1]\r\n'
    'SERVER=EMPLOYEE\r\n'
    'OPERATION=AddUpdate\r\n'
    'MODULE=StoreAssignment\r\n'
    '[DATA.1]'
)

assign_count = 0
day_new_starter = 10
####################################################################################################
# Read the source file into a dataframe and rename columns
####################################################################################################
client = boto3.client('s3')
response = client.get_object(Bucket=v_bucket, Key=v_source_key) 

df = pd.read_csv(response.get("Body")
                 ,converters={'Staff Number': str}
                 ,parse_dates=['Start Date']
                 ,dayfirst=True
                )
df = df.rename(columns={"Staff Number":"staffNo"
                        ,"Cost Centre":"storeNo"
                        ,"Known As":"fstName"
                        ,"Surname":"lstName"
                        ,"Job Title":"jobTitl"
                        ,"Start Date":"startDate"})
df = df.replace(np.nan, '', regex=True)


####################################################################################################
# Calculate fields and add to dataframe
####################################################################################################

################ Security Level calculations ################
# Use the job variable to determine the job title value
jobs = [
    (df['jobTitl'] == 'Seasonal Sales Assistant'),
    (df['jobTitl'] == 'Sales Assistant'),
    (df['jobTitl'] == 'Team Leader'),
    (df['jobTitl'] == 'Brand Lead'),
    (df['jobTitl'] == 'Store Manager')
    ]

# Set values for each job level 
backOffice = ['20', '20', '30', '35','40']
frontOffice = ['20', '20', '30', '35','40']

# Assign value based on value in job
df['boSecLvl'] = np.select(jobs, backOffice, default='20')
df['foSecLvl'] = np.select(jobs, frontOffice, default='20')


################ Other Calculations ################
# Calculate the number of days between today and persons start date 
df['start_date_diff'] = (v_date_now - df['startDate']).dt.days

# Store Number calculation
df['storeNo'] = df['storeNo'].str.replace('S00','',1)

#Staff Number calculation
df['staffNo'] = ('000000'+df['staffNo']).str[-6:]

################ Add other non-calculated columns ################
df['passwd'] = '12345678' 
df['passchg'] = '2000-01-01T00:00:00.0000000' 
df['passchg_dcn'] = '2000-01-01 00:00:00' 
df['divNo']   = '1'
df['deptNo']  = '2'
df['stat']    = '2'
df['accblty'] = 'WKSTN'

####################################################################################################
# Loop through each employee in the data to create manual files
####################################################################################################

for index, row in df.iterrows():
  # Create formatted employee record and add to existing data in loop
  xmldata = xmldata + (
    '<employee number="'+str(row['staffNo'])+'" action="addupdate">\n'
    +'<employmentStatusCode>HIRE</employmentStatusCode>\n'
    +'<primaryLocationNumber>'+str(row['storeNo'])+'</primaryLocationNumber>\n'
    +'<firstName>'+str(row['fstName']) +'</firstName>\n'
    +'<lastName>'+str(row['lstName']) +'</lastName>\n'
    +'<active>true</active>\n'
    +'</employee>\n'
  )
  
  dcndata = dcndata + ( 
    '\r\n"Employee","'
    + str(row['staffNo']) 
    +'","'+ str(row['storeNo']) 
    +'","'+ str(row['divNo']) 
    +'","'+ str(row['deptNo']) 
    +'","'
    +'","'+ str(row['fstName']) 
    +'"," ' # the space is intentional
    +'","'+ str(row['lstName']) 
    +'","'+ str(row['jobTitl']) 
    +'","'+ str(row['stat']) 
    +'","","","","","","","","","","","'
    +'","'+ str(row['boSecLvl']) 
    +'","'+ str(row['foSecLvl']) 
    +'","'+ str(row['passwd']) 
    +'","'+ str(row['passchg_dcn']) 
    +'","'+ str(row['accblty']) 
    +'","","","","","","","","","","","","","","","","","","","",""'
  )
    
  if row['start_date_diff'] <= day_new_starter:
    assign_count = assign_count +1
    dcndata_assign = dcndata_assign + (
      '\r\n"StoreAssignment","'
      + str(row['staffNo']) 
      +'","'+ str(row['storeNo'])  
      +'","'+ str(row['boSecLvl']) 
      +'","'+ str(row['foSecLvl']) 
      +'","'+ str(row['passwd']) 
      +'","'+ str(row['passchg_dcn']) 
      +'","'+ str(row['accblty']) 
      +'","","","","",""'
    )
  
  
# Add tail to XML file 
xmldata = xmldata + '</employees>'

####################################################################################################
# Output files to S3 ready to be moved to final location
####################################################################################################

#print(xmldata)
#print('---------')
#print(dcndata)
#print('---------')
#print(xmldata_tree)
#print('---------')
#print(dcndata_assign)
#print('---------')

client.put_object(Body=xmldata, Bucket=v_bucket, Key=v_xml_key)
client.put_object(Body=dcndata, Bucket=v_bucket, Key=v_dcn_key)
if assign_count > 0:
  client.put_object(Body=dcndata_assign, Bucket=v_bucket, Key=v_dcn_assign_key)
  context.updateVariable('v_assign_file_generated','Yes')
else:
  #this is so that they get ignored by the failure check
  context.updateVariable('v_send_status_dcn_assign_EU64','SUCCESS')
  context.updateVariable('v_send_status_dcn_assign_US','SUCCESS')