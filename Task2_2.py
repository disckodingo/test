import pandas as pd
import hashlib
import warnings
from calendar import day_name

from sqlalchemy import create_engine

warnings.filterwarnings("ignore")

# Postgres username, password, and database name
POSTGRES_ADDRESS='127.0.0.1'
POSTGRES_PORT='5432'
POSTGRES_USERNAME='postgres'
POSTGRES_PASSWORD='jcnruad3'
POSTGRES_DBNAME='postgres'

# Postgres login information
postgres_str=('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=POSTGRES_USERNAME,
                                                                                      password=POSTGRES_PASSWORD,
                                                                                      ipaddress=POSTGRES_ADDRESS,
                                                                                      port=POSTGRES_PORT,
                                                                                      dbname=POSTGRES_DBNAME))
cnx = create_engine(postgres_str)
dfsql=pd.read_sql_query(
    'select * from  public.agregated_data ad2 left join transaction_db_raw.transaction_log t on cast(t.transaction_uid as text) = ad2.uid;',cnx)
dfsql.to_csv("df.csv",index=False,header=True)
df=pd.read_csv('df.csv')
df.columns=['uid','csv_hash','tbl_hash','id_num_row','transaction_uid','account_uid','transaction_date','type_deal',
            'transaction_amount']

df['transaction_date']=pd.to_datetime(df['transaction_date'])
df['WeekDay']=df.transaction_date.apply(lambda x:x.strftime("%A"))
df['Month']=df.transaction_date.apply(lambda x:x.strftime("%B"))
df['Year']=df.transaction_date.apply(lambda x:x.strftime("%Y"))
day=df.groupby(['account_uid','WeekDay'],as_index=False)['transaction_amount'].max().to_csv('max.csv',index=False)
day1=df.groupby(['account_uid','WeekDay'],as_index=False)['transaction_amount'].min().to_csv('min.csv',index=False)
day2=df.groupby(['account_uid','WeekDay'],as_index=False)['transaction_amount'].mean().to_csv('avg.csv',index=False)
mon=df.groupby(['account_uid','Month'],as_index=False)['transaction_amount'].max().to_csv('maxm.csv',index=False)
mon1=df.groupby(['account_uid','Month'],as_index=False)['transaction_amount'].min().to_csv('minm.csv',index=False)
mon2=df.groupby(['account_uid','Month'],as_index=False)['transaction_amount'].mean().to_csv('avgm.csv',index=False)
year=df.groupby(['account_uid','Year'],as_index=False)['transaction_amount'].max().to_csv('maxy.csv',index=False)
year1=df.groupby(['account_uid','Year'],as_index=False)['transaction_amount'].min().to_csv('miny.csv',index=False)
year2=df.groupby(['account_uid','Year'],as_index=False)['transaction_amount'].mean().to_csv('avgy.csv',index=False)
