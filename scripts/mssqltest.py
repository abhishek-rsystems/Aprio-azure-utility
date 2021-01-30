"""
This script will fetch and print the data from SQL SERVER
"""

import pyodbc, configparser

config = configparser.ConfigParser()
config.read('config.ini')

TMP_DIR = config['main']['TMP_DIR']

DBHOST = config['DB_AprioUSDB']['DBHOST']
DBNAME = config['DB_AprioUSDB']['DBNAME']
DBUSER = config['DB_AprioUSDB']['DBUSER']
DBPASS = config['DB_AprioUSDB']['DBPASS']

try:
    cnxn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};SERVER='+DBHOST+';DATABASE='+DBNAME+';UID='+DBUSER+';PWD='+ DBPASS)
except Exception as e:
    print("[-] Failed to connect to DB @ %s/%s" % (DBHOST,DBNAME))
    exit()

cur = cnxn.cursor()
print("[+] Connected to DB @ %s/%s" % (DBHOST,DBNAME))

def run_query(query):
    print("\nQUERY => %s\n" %(query))
    global SQLDATA
    SQLDATA = ''
    cur.execute(qry)
    row = cur.fetchone()
    while row: 
        str1 = str(row).replace('(','').replace(')',''). replace('\'','')
        l = str1.replace(',',' :')
        SQLDATA = SQLDATA + " , " + l
        row = cur.fetchone()
    return SQLDATA[3:-3]

## Queries ##
# qry =  "select NAME from AprioBoardPortal.Tenant where Code = '1000581'"
qry =  "select id from AprioBoardPortal.UploadedDoc where FileName = '01 Agenda.pdf' and TenantId = '3'"
# qry = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'Tenant'"


## Output ##
print(run_query(qry))
cur.close()
