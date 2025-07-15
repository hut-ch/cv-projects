###
# Establish PLM connection
# Extract endpoint and write to file
###

import requests
import json
import re
import datetime
#import boto3
import zlib
#################################################
#define testing VARIABLES

v_job_table = "styles"
v_table_column_prc = "id"
v_timestamp = "1900-01-01T00:00:00Z"
v_count_record = 0
#################################################


# Expected environment variable content
# v_job_table        :  The endpoint
# v_metadata         :  Additional filters to be applied
# v_job_table_type   :  The object name of the endpoint
# v_job_iteration    :  The limit to be applied, blank if default
# v_table_column_prc : The object identifier field (used to count objects)
# v_timestamp        :  The 'modified after' filter timestamp to apply
v_job_iteration_default = "10"
# Set object identifier field to default value if not populated
if str(v_table_column_prc) == "None" :
  v_table_column_prc = "id"
  context.updateVariable("v_table_column_prc", str(v_table_column_prc))

def debug_message(message):
    print datetime.datetime.now().strftime("%H:%M:%S.%f")+"> "+message

debug_message("Endpoint: "+str(v_job_table))
debug_message("Object identifier field: "+str(v_table_column_prc))

# Endpoints
v_endpoint_name  = v_job_table
v_url_endpoint     = "http://plm.superdry.com/csi-requesthandler/api/v2/"
v_url_session      = v_url_endpoint+"session"

# PLM credentials, will need to be stored
v_auth_json = {
  "username" : "BI.Admin",
  "password" : "7-$Y`)3G{hFv5D:>"
  }
# Specify json format for information in and out
v_headers_json = {
    "content-type" : "application/json",
    "accept"       : "application/json"
    }


# Establish session
# The Session object allows you to persist certain parameters across requests.
# It also persists cookies across all requests made from the Session instance.
# A Session object has all the methods of the main Requests API.
debug_message("Post session")
v_api_session = requests.Session()
v_response = v_api_session.post(url=v_url_session, headers=v_headers_json, data=json.dumps(v_auth_json))
debug_message("API session open response: " + unicode(v_response))
debug_message("API session cookie: " + unicode(v_response.text))


# Extract Endpoint Object
# By default, when you make a request, the body of the response is downloaded immediately.
# You can override this behavior with the stream parameter
# to defer downloading the response body until you access the Response.content attribute
# e.g. v_api_session.get(url=v_url_style, stream=true)
# If you set stream to True when making a request,
# Requests cannot release the connection back to the pool unless you consume all the data or call Response.close.
debug_message("Get "+v_endpoint_name)

# Maximum number of objects to include in the results
if (str(v_job_iteration) == "" or str(v_job_iteration) == "None") :
  v_job_iteration = v_job_iteration_default
v_get_limit = int(v_job_iteration)
# Save to numeric environment variable for subsequent test if more data
context.updateVariable("v_loop", str(v_get_limit))

# Object filtering to be applied
if str(v_metadata) == "None" :
  v_metadata = ""
# Define bucket resource
#v_boto_s3    = boto3.resource("s3")


# Set Get filter parameters
v_get_parameter = "?limit="+str(v_get_limit)+"&-sort&skip="+str(v_count_record)+"&modified_after="+str(v_timestamp)+str(v_metadata)
#+"&sd_style_style_status_enum=Complete"
debug_message("Number of objects to paginate: "+str(v_get_limit))
debug_message("Specified Object filtering to be applied: "+str(v_metadata))
debug_message("API GET parameter: "+v_get_parameter)
v_response = v_api_session.get(url=v_url_endpoint+v_endpoint_name+v_get_parameter, stream=False)
debug_message(u"API endpoint response: " + unicode(v_response))
if unicode(v_response) != u"<Response [200]>" :
  debug_message(u"API error:\n" + unicode(v_response.text))
  raise Exception(u"API error:\n" + unicode(v_response) + "\n" + unicode(v_response.text))
  
debug_message("Data length = " + str(len(v_response.text)))
v_count_update = int(str(v_response.text.count('{"'+v_table_column_prc+'":')))
if v_count_update == 0 :
  # If not found then perhaps not first listed attribute, search again with leading comma
  v_count_update = int(str(v_response.text.count(',"'+v_table_column_prc+'":')))
debug_message("Number of objects: " + str(v_count_update))
if v_count_update == 0:
  debug_message("No further data, halting/n")
else :  
  # Convert maps (nested structures of pairs) to array of delimited string
  debug_message("Convert nested structure to array")
  # Define regular expression for a nested structure
  v_regex_replace = re.compile(u':{([^}]*)}', re.VERBOSE)
  # Function defined to do the replace on a matching group
  # 1) replace element separator commas with escaped commas
  # 2) strip quotes
  # 3) replace escaped commas with quoted commas
  # 4) replace struct curly brace with array square brace and quote
  # 5) replace empty quote array with empty array
  def escape_quote(match):
    return match.group().replace(u'","',u'\,').replace(u'"',u'').replace(u'\,',u'","').replace(u'{',u'["').replace(u'}',u'"]').replace(u'[\"\"]',u'[]')
  v_file_body = v_regex_replace.sub(escape_quote,v_response.text)
  # Make array if just a single item
  if v_file_body[0] == u"{" :
    v_file_body = u"["+v_file_body+u"]"

  # Compress to gzip format
  debug_message("Compress to gz")
  v_compression_level = 9
  v_file_body = zlib.compress(str(bytearray(v_file_body,"utf_8")), v_compression_level)  

  # Write to file
  v_json_filename = v_endpoint_name+"_page"+str(v_count_file).rjust(3, "0")+"_" + str(v_file_date) + ".json.gz"
  debug_message("Write to file: " + v_json_filename)
  #v_boto_s3.Bucket(v_s3_bucket).put_object(Key=v_plm_dir+"/"+v_plm_dir_extract+"/"+v_endpoint_name+"/"+v_json_filename, Body=v_file_body)  
  f = open("C:/test/"+v_json_filename, "w")
  f.write(v_file_body)
  f.close()  
  debug_message("File output completed")
  
# Update count of fetched objects
debug_message("Save object count")
v_count_record += v_count_update
context.updateVariable("v_count_record", str(v_count_record))
context.updateVariable("v_count_update", str(v_count_update))

if v_endpoint_name == "styles" :
  debug_message("styles endpoint")

# Close session
debug_message("Delete session")
v_response = v_api_session.delete(url=v_url_session)
debug_message("API session close response: " + str(v_response) + "\n")

