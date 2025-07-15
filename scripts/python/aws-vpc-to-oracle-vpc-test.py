######################################################################################################
## POC script to prove that a direct connection can be made between the etl tool within the local VPC
## and the oracle vpc that is hosting the source database
## if successful then file extracts will not be required and a direct connection beween both cloud
## environments can be setupo instead
######################################################################################################

import paramiko 
import sshtunnel
import cx_Oracle

publicHost = '1.1.1.1'
publicHostPort = 22
publicHostBindPort = 11520
privateHost = '2.2.2.2'
privateHostBindPort= 1522
sshUser   = 'oracle'
# key needs to be valid openssh key
sshKey   = '/key/oracle_open.ppk'

# establish ssh tunnel context
with sshtunnel.open_tunnel(
    (publicHost, publicHostPort),
    ssh_username=sshUser,
    ssh_pkey=sshKey,
 #   ssh_private_key_password="secret",
    remote_bind_address=(privateHost, privateHostBindPort),
    local_bind_address=('0.0.0.0', publicHostBindPort)
) as tunnel:
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(publicHost, username=sshUser, key_filename=sshKey)
    
    print('connected to SSH tunnel')
    # try connecting to oracle db using wallet file   
    try:
        # need to download and unzip the oracle instant client files and add the wallet content into network/admin folder
        # https://www.oracle.com/database/technologies/instant-client.html
        
        cx_Oracle.init_oracle_client(lib_dir=r"\oracle_ic\instantclient_21_3")
    
        db_dsn = "dprod_high"
        db_user = 'dprod'
        db_passowrd = '1234567'
    
        connection = cx_Oracle.connect(user=db_user, password=db_passowrd, dsn=db_dsn)
    
        print('connected to db')
        
        # select some data from a table
        try:
            cursor = connection.cursor()
            result = cursor.execute('select * from ITEM_LOC fetch next 100 rows only')
                  
            for row in result:
                print(row)
        except:
            print('error selecting data')  
    
        connection.close()
    except:
        print('issue connecting to db')
    else:
        print('disconnected from db')
    
    client.close()
    
    print('disconnected from SSH tunnel')