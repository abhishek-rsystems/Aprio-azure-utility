"""
This script will fetch and print the data from SQL SERVER
"""

import pyodbc

config = configparser.ConfigParser()
config.read('config.ini')

TMP_DIR = config['main']['TMP_DIR']
DBHOST = config['main']['DBHOST']
DBNAME = config['main']['DBNAME']
DBUSER = config['main']['DBUSER']
DBPASS = config['main']['DBPASS']

try:
    cnxn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};SERVER='+DBHOST+';DATABASE='+DBNAME+';UID='+DBUSER+';PWD='+ DBPASS)
except Exception as e:
    print("[-] Failed to connect to MSSQL => %s" % (DBHOST))
    exit()

cur = cnxn.cursor()
## Get Columns of Table ##
# qry = 'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N\'tOrganizations\''

## Get Org ID & org Name ##
global SQLDATA
SQLDATA = ''
qry = 'SELECT OrgName,OrgAlias FROM dbo.tOrganizations'
cur.execute(qry)
row = cur.fetchone()
while row: 
    str1 = str(row).replace('(','').replace(')',''). replace('\'','')
    l = str1.replace(',',' :')
    SQLDATA = SQLDATA + " , " + l
    row = cur.fetchone()
cur.close() 

test_str = SQLDATA[1:]

res = [] 
for sub in test_str.split(', '): 
    if ':' in sub: 
        res.append(map(str.strip, sub.split(':', 1))) 
res = dict(res) 
 
print("Fetched Following Dict => " + str(res)) 