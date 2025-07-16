import boto3
from botocore.errorfactory import ClientError

# get s3 location form system envrionment variables passed in to job
bucket = v_s3_location
source_key = v_s3_folder_source+'/'+v_source_prefix

client = boto3.client('s3')

# get buckect contents
files = client.list_objects_v2(Bucket=v_bucket,Prefix=v_source_key)

# iterate over content of s3 bucket for specific file and pass result back up to calling job
if 'Contents' in files:
  for item in files['Contents']:
    file_name = list(reversed(item['Key'].split('/')))[0]
    if file_name.endswith('.csv'):
      context.updateVariable('v_source_file', file_name)
      context.updateVariable('v_source_file_found', 'Yes')
      print('Job Continuing - File Found: '+ file_name)
      exit()
    else:  
      context.updateVariable('v_source_file_found', 'No')
      print('File Found is not a CSV File: '+ file_name)      
else:  
  context.updateVariable('v_source_file_found', 'No')
  print('Job Ending - File Not Found Matching: '+ v_source_key)
