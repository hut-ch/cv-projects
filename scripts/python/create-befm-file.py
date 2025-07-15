########################################################################
## this script is used to create a befm file that is used manifest file
## to ensure only the expected files are loaded as part of each load
## this is intended for det and uat so that we can choose any production 
## files to test with and still have the files load 
## ensure the ART_BEFM_ID is set to the current_id +1 to ensure the 
## required sequence is maintinad otherwise the job will error
########################################################################
import os,sys, zipfile 
from datetime import datetime

path = '/source-data/'
files = os.listdir(path)

# create output filenames
befm_filename = 'bi_befm_'+datetime.now().strftime("%Y%m%d%H%M%S")
befm_filename_csv = befm_filename+'.csv'
befm_file_path_csv = path+'/'+befm_filename_csv
befm_filename_zip = befm_filename+'_1.zip'
befm_file_path_zip = path+'/'+befm_filename_zip 
file_counter = 0

# set up default column values
ART_BEFM_ID = '0001'
CRTN_DATE_TIME = datetime.now().strftime("%d/%m/%Y %H:%M")
PROCD = '1'
BR_ART_BR_ID = '123456789'
RENAME_DATE_TIME = CRTN_DATE_TIME
MESS = ''
FILE_SIZE_BYTES = '123'
FILENAME_ZIP = ''
FILE_TYPE = 'E'
CRTN_UPD_DATE_TIME = '41:57.3'
BI_FILENAME = befm_filename_csv

# open the output file to write to
print('opening file : '+ befm_file_path_csv)
data_output= open(befm_file_path_csv, "a")

# add the required file header
print('writing header')
data_output.write('ART_BEFM_ID,FILENAME,CRTN_DATE_TIME,PROCD,BR_ART_BR_ID,RENAME_DATE_TIME,MESS,FILE_SIZE_BYTES,FILENAME_ZIP,FILE_TYPE,CRTN_UPD_DATE_TIME,BI_FILENAME\n')
data_output.write(ART_BEFM_ID+','+befm_filename_csv+','+CRTN_DATE_TIME+','+PROCD+','+BR_ART_BR_ID+','+RENAME_DATE_TIME+','+MESS+','+FILE_SIZE_BYTES+','+befm_filename_zip+','+FILE_TYPE+','+CRTN_UPD_DATE_TIME+','+BI_FILENAME+'\n')

# loop through the files extrascted from the source system these are a mix of csv files and zipped csv files
# and create an entry to be added to output file
print('writing file entries')
for file in files:
    filepath = path+'/'+file
    if zipfile.is_zipfile(filepath):
        data_output.write(ART_BEFM_ID+','+file+','+CRTN_DATE_TIME+','+PROCD+','+BR_ART_BR_ID+','+RENAME_DATE_TIME+','+MESS+','+FILE_SIZE_BYTES+','+FILENAME_ZIP+','+'Z'+','+CRTN_UPD_DATE_TIME+','+BI_FILENAME+'\n')
        
        zip = zipfile.ZipFile(filepath).namelist()
        for csvfile in zip:
            data_output.write(ART_BEFM_ID+','+csvfile+','+CRTN_DATE_TIME+','+PROCD+','+BR_ART_BR_ID+','+RENAME_DATE_TIME+','+MESS+','+FILE_SIZE_BYTES+','+file+','+FILE_TYPE+','+CRTN_UPD_DATE_TIME+','+BI_FILENAME+'\n')
            file_counter = file_counter+1

# write the set of files to the output file
print(str(file_counter)+' file entries written')    
data_output.write(ART_BEFM_ID+','+befm_filename_zip+','+CRTN_DATE_TIME+','+PROCD+','+BR_ART_BR_ID+','+RENAME_DATE_TIME+','+MESS+','+FILE_SIZE_BYTES+','+FILENAME_ZIP+','+'Z'+','+CRTN_UPD_DATE_TIME+','+BI_FILENAME+'\n')    

print('saving file: '+befm_file_path_csv )
data_output.close()

## zip the output file as the etl job expecs the file to be in a zip with the matching name
print('zipping file: '+befm_filename_csv+' to '+befm_file_path_zip )
zf = zipfile.ZipFile(befm_file_path_zip, mode='w')
zf.write(befm_file_path_csv,befm_filename_csv)
zf.close()
print('file zipped')
