import pandas as pd
import hashlib
import warnings
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

# Connectyion
cnx=create_engine(postgres_str)

# PostgresTable
dfsql=pd.read_sql_query('select * from transaction_db_raw.transaction_log;',cnx)
dfsql=dfsql.drop(columns='id_num_row')
dfsql.to_csv('dfsql',index=False,header=False)
h=hashlib.new("md5")
with open("dfsql","rb") as f:
    for line in f:
        h.update(line)
        c=str(line[0:36])
        g="sql,"+c+","+h.hexdigest()+"\n"
        j=g.replace(",b'",",")
        s=j.replace("',",",")
        file=open('dfsql1.csv','a')
        file.write(s)
dftodb=pd.read_csv("dfsql1.csv",error_bad_lines=False)
dftodb.to_csv("dfsql2.csv",header=["type","uid","hash"],index=False)
dftodb1=pd.read_csv("dfsql2.csv",error_bad_lines=False)
df2=pd.read_csv('dfsql2.csv')
df2.to_csv('raw_dfsql.csv')
dftodb1.to_sql("raw_dfsql.csv",cnx,schema='transaction_db_clean',index=False)

# CSV df
df2=pd.read_csv("transaction_data.csv",sep='	',engine='python',
                names=['transaction_uid','account_uid','transaction_date','type_deal','transaction_amount'])
a=hashlib.new("md5")
with open("transaction_data.csv","rb") as t:
    for lin in t:
        a.update(lin)
        b=str(lin[0:36])
        d="csv,"+b+","+a.hexdigest()+"\n"
        k=d.replace(",b'",",")
        x=k.replace("',",",")
        file=open('dfcsv_todb.csv','a')
        file.write(x)

csvtodb=pd.read_csv("dfcsv_todb.csv",error_bad_lines=False)
csvtodb.to_csv("dfcsv1.csv",header=["type","uid","hash"],index=False)
csvtodb1=pd.read_csv("dfcsv1.csv",error_bad_lines=False)
csv2=pd.read_csv('dfcsv1.csv')
csv2.to_csv('dfcsv2.csv',index=False)
csvtodb1.to_sql("dfcsv2.csv",cnx,schema='transaction_db_clean',index=False)
