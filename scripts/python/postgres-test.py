import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
import pandas as pd

# get db credentials from environment file for a bit of security
load_dotenv('db.env')

# get all the enviroenemt variable
host = os.getenv('host_name')
port = os.getenv('port_number')
db = os.getenv('database_name')
user = os.getenv('user_name')
pw = os.getenv('user_pass')

#construct connection string
db_url ='postgresql+psycopg2://'+user+':'+pw+'@'+host+':'+port+'/'+db

# create connection
try:
    engine = create_engine(db_url)
except Exception as e:
    print('Unable to access postgresql database', repr(e))

# create connection and open a connection
engine = create_engine(db_url)
con = engine.connect() 


#create table
query = text(
    """DROP TABLE IF EXISTS public.people;

    CREATE TABLE IF NOT EXISTS public.people
    (
        id bigint GENERATED ALWAYS AS IDENTITY,
        first_name varchar(255) ,
        last_name varchar(255) ,
        CONSTRAINT people_pkey PRIMARY KEY (id)
    );"""
)

try:
    con.execute(query)
    print('People table created successfully')
except Exception as e:
    con.close()
    raise Exception('Unable to drop table', repr(e))

# insert data
query = text(
   """
   INSERT INTO public.people (first_name, last_name)
   VALUES ('Alice','Poulin')
       ,('Fabíola','Solberg')
       ,('Kamala','Miazga')
       ,('Josip','Greenwood')
       ,('Dev','Raskop')
       ,('Ilhana ','McKenna')
       ;
   """ 
)

try:
    con.execute(query)
    print('Data inserted into people table')
except Exception as e:
    con.close()
    raise Exception('Unable to insert into table', repr(e))


# select date and store in DataFrame
query = text("SELECT * FROM public.people")
rs = con.execute(query)
df = pd.DataFrame(rs.fetchall())

# Close connection
con.close()

# Print head of DataFrame df
print(df.head(10))