#######################################################################################################
# POC script to check that the expected format can be generated based on inital busainess requirements
#######################################################################################################

import io
import pandas as pd
import numpy as np
from datetime import datetime

now = datetime.now()
df = pd.read_excel(r"Employees Example.xlsx", converters={'Staff Number': str})
df = df.replace(np.nan, '')

df2 = pd.show_versions
ddd = io.FileIO.read

print (df)

# Initialise variables - not really needed but makes it easier to test
empNo   = ''
storeNo = ''
divNo   = ''
deptNo  = ''
fstName = ''
lstName = ''
jobTitl = ''
stat    = ''
accblty = ''
bOffSl  = ''
fOffSl  = ''
passwd  = ''
passchg = ''

xmldata = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
    '<ns1:InputEntity xmlns:ns1="http://BTE.ESB.DataModel.InputEntity" xmlns:ns2="http://BTE.ESB.InputModel.InputEmployeeData">\n'
    '<InputAction When="2000-01-01T00:00:00.0000000" Verb="AddUpdate"/>\n'
    '<Payload>\n' 
)

dcndata = (
    '; Destination Store\n'
    '[RUNTIME]\n'
    'DATE='+now.strftime("%Y-%m-%d")+'\n'
    'TIME='+now.strftime("%H:%M:%S")+'\n'
    'ALLOWERRORS=FALSE\n'
    '[TASK.1]\n'
    'SERVER=EMPLOYEE\n'
    'OPERATION=AddUpdate\n'
    'MODULE=EMPLOYEE\n'
    '[DATA.1]\n'
)

for index, row in df.iterrows():
    empNo   = str(row['Staff Number'])
    storeNo = str(row['Cost Centre']).replace('S00','',1)
    divNo   = '1'
    deptNo  = '2'
    fstName = str(row['Known As'])
    lstName = str(row['Surname'])
    jobTitl = str(row['Job Title'])
    stat    = '2'
    accblty = 'WKSTN'    
    passwd  = '12345678'
    passchg = '2000-01-01T00:00:00.0000000'
    
    if str(row['Job Title']) == 'Seasonal Sales Assistant':
        bOffSl = '20'
    elif str(row['Job Title']) == 'Sales Assistant':
        bOffSl = '20'    
    elif str(row['Job Title']) == 'Team Leader':
        bOffSl = '30'     
    elif str(row['Job Title']) == 'Brand Lead':
        bOffSl = '35'  
    elif str(row['Job Title']) == 'Store Manager':
        bOffSl = '40'  

    if str(row['Job Title']) == 'Seasonal Sales Assistant':
        fOffSl = '20'
    elif str(row['Job Title']) == 'Sales Assistant':
        fOffSl = '20'    
    elif str(row['Job Title']) == 'Team Leader':
        fOffSl = '30'     
    elif str(row['Job Title']) == 'Brand Lead':
        fOffSl = '35'  
    elif str(row['Job Title']) == 'Store Manager':
        fOffSl = '40'          
            
    xmldata = xmldata + '<ns2:InputEmployeeData Header="Employee" Number="'+ empNo +'" StoreNumber="'+storeNo +'" DivisionNumber="'+ divNo +'" DepartmentNumber="'+ deptNo +'" FirstName="'+ fstName +'" LastName="'+ lstName +'" JobTitle="'+ jobTitl+'" Status="'+ stat +'" BackOfficeSecurityLevel="'+bOffSl+'" FrontOfficeSecurityLevel="'+fOffSl+'" Password="'+passwd+'" Accountability="'+accblty+'" PasswordChanged="'+passchg+'" /> \n' 
    dcndata = dcndata + '"Employee","'+ empNo +'","'+ storeNo +'","'+ divNo +'","'+ deptNo +'","","'+ fstName +'","","'+ lstName +'","'+ jobTitl +'","'+ stat +'","","","","","","","","","","","","'+ bOffSl +'","'+ fOffSl +'","'+ passwd +'","'+ passchg +'","'+ accblty +'","","","","","","","","","","","","","","","","","","","","" \n'
    
xmldata = xmldata + '</Payload>'

print('\n-------------------------------------------------------------------------------------')
print('XML DATA')
print('-------------------------------------------------------------------------------------')
print(xmldata)
print('\n-------------------------------------------------------------------------------------')
print('DCN DATA')
print('-------------------------------------------------------------------------------------')
print(dcndata)

xmlFile = open( 'retail_employees_example.xml', 'w' )
xmlFile.write(xmldata)
xmlFile.close()

dcnFile = open( 'retail_employees_example.dcn', 'w' )
dcnFile.write(dcndata)

dcnFile.close()