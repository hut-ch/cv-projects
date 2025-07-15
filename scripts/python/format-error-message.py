#########################################################################
## Iterate over the messages in the automatic variable 'detailed_error'
## by splitting them out and then combine all error messages together
## removing exrta message entries. Also create a list of failed 
## components and a total row count from all jobs then update 
## environemt variables ready to send message
#########################################################################

detailed_error_test = detailed_error

v_message_detail = detailed_error_test.split("\n")
v_components = ''
v_full_message = '\n'
v_total_row_count = 0
v_status = 'FAILED'

# get total number or rows inserts/updated for whole run
v_gridvar = context.getGridVariable('v_run_log')
for v_row in v_gridvar:
    v_total_row_count = v_total_row_count+v_row[2]

# calculate duration in seconds of overall job based on login anf logoff jobs
v_duration = v_end - v_start
v_duration_sec = v_duration.seconds  
    
# format error message and remove extra lines   
for v_error in v_message_detail:
  v_err_det = v_error.split(": ",5)
 
  if (v_err_det[0] != v_err_det[2] and v_err_det[0] != v_err_det[3] and 'Variable [v_column_name] expected [default]' not in v_error and 'End Failure 0'not in v_error ):
    v_path_mess = v_error.split("failed: ",2)
    v_path = v_path_mess[0].split(": ",3)
    v_mess = v_path_mess[1].split(": ",2)
    print(v_path)
    print(len(v_path))
    print(v_mess)
    if len(v_path) > 2:
      v_job = v_path[1]
      v_child = v_path[2]
    elif len(v_path) == 2:
      v_job = v_path[1]
      v_child = ''
    else : 
      v_job = ''
      v_child = ''   
        
    v_components = v_components + v_mess[0] + ', '
    v_full_message = v_full_message +'   Job: '+ v_job +'\n   Child Job: '+ v_child +'\n   Component: '+ v_mess[0]  +'\n   Message: '+ v_mess[1].replace('\\n','\n    ') + '\n\n'

#remove extra comma at end of list
v_components = v_components[:-2]

# update environment variables ready to send message using shared job    
context.updateVariable('ev_audit_started_at',v_start)   
context.updateVariable('ev_audit_completed_at',v_end)   
context.updateVariable('ev_audit_duration',v_duration_sec)   
context.updateVariable('ev_audit_component',v_components)    
context.updateVariable('ev_audit_status',v_status )
context.updateVariable('ev_audit_message', v_full_message)
context.updateVariable('ev_audit_row_count', v_total_row_count)



print('component: ' +str(v_components))
print('message: ' + str(v_full_message))

#print(detailed_error)