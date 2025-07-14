<#  
    .Synopsis  
    Export Report Details.  
    .Description  
    Logs onto BI Platform, retrieves a list of documents then gets the details of each document and saves to specified file. 
    Next retrieves a list of data providers in the document and gets the details for each and saves to specified file. 
    #>   
     
    $logonInfo = @{}  
    $logonInfo.userName = "user" 
    $logonInfo.password = "password"
    $logonInfo.auth     = "secEnterprise"     
    $folderPath         = "C:\data"   
      
    $hostUrl = "http://"+$ServerName.ToLower()+":6405/biprws"  
  
    $Folder__SourceName_Universes  

# Check there is an output folder for the universes
if (-not (Test-Path $Folder__SourceName_Universes) ) { 
            New-Item -ItemType Directory -Path $Folder__SourceName_Universes}
     
    ############################################################################ 
    # Logon and retrieve the logon token 
    ############################################################################
    $headers = @{"Accept"       = "application/json" ;   
                    "Content-Type" = "application/json"   
                }  
    $result = Invoke-RestMethod -Method Post -Uri ($hostUrl + "/logon/long") -Headers $headers -Body (ConvertTo-Json($logonInfo))  
    $logonToken =  "`"" + $result.logonToken + "`""  # The logon token must be delimited by double-quotes.  
 
########################
#Universe Data
########################
    ############################################################################
    # Get a list of Unx Universes - currently only the first 50 are returned if 
    #we go over 50 universes then this will need to loop as well
    ############################################################################
    $headers = @{"X-SAP-LogonToken" = $logonToken ;
                 "Accept"           = "application/json" ;   
                 "Content-Type"     = "application/json"   
                }  
    $result = Invoke-RestMethod -Method Get -Uri ($hostUrl + "/sl/v1/universes?type=unx&limit=50") -Headers $headers 
    $universeList = $result.universes.universe
    
    Write-Host "Universe Data Export Start"
    
    #Loop through Universe List to get the universe details and output to file
    foreach ($universe in $universeList){
            
        $filePath = $Folder__SourceName_Universes + "/" + $universe.id + "_universe.xml"
        
        if(Test-Path $filePath -isValid) {
            $headers = @{"X-SAP-LogonToken" = $logonToken ;
                        "Accept"           = "application/xml" ;   
                        "Content-Type"     = "application/json"   
                        }  
            $result = Invoke-RestMethod -Method Get -Uri ($hostUrl + "/sl/v1/universes/"+ $universe.id) -Headers $headers  -OutFile $filePath 

        } else {  
            Write-Error "Invalid file path " + $filePath  
        } 
    }

    Write-Host "Universe Data Export End"
……….
