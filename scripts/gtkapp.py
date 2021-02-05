"""
This is the Main GUI app file which pulls the detaisl from Database & then render the same to Tkinter app
On selecting the button, itsync the same,
"""

import pyodbc, configparser, os
from tkinter import *
from tkinter.ttk import *

from az_sync_single import *
import DataMigrationUtility as dm

config = configparser.ConfigParser()
config.read('config.ini')

TMP_DIR = config['main']['TMP_DIR']
DBHOST = config['sourcedb']['src_server']
DBNAME = config['sourcedb']['src_db']
DBUSER = config['sourcedb']['src_user']
DBPASS = config['sourcedb']['src_pwd']

try:
    cnxn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};SERVER='+DBHOST+';DATABASE='+DBNAME+';UID='+DBUSER+';PWD='+ DBPASS)
except Exception as e:
    print("[-] Failed to connect to DB @ %s" % (DBHOST))
    exit()
cur = cnxn.cursor()
print("[+] Connected to DB @ %s/%s" % (DBHOST,DBNAME))
qry = 'SELECT OrgName,OrgAlias FROM dbo.tOrganizations'
global SQLDATA
SQLDATA = ''
cur.execute(qry)
row = cur.fetchone()
while row:
    # print(row)
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
values = res

## Define Geometry ##
GEOMETRY = "400x200"

def clicksel():
    ORG_NAME = str(clicked.get())
    ORG_ID = values.get(ORG_NAME)
    label.config(text = "Syncing ["+ ORG_ID +"]",width=300)
    dm.MigrateData(ORG_ID) #data migration with password encryption
    #MAINS(ORG_ID) #file migration with pdf conversion

def close():
    root.destroy

## Initiate the Box  ##
root = Tk()

## banner & Logo ##
root.wm_title("Aprio Data Sync Utility")
FAVICON = os.path.join(TMP_DIR,"favicon.png")
root.call('wm', 'iconphoto', root._w, PhotoImage(file=FAVICON))
root.geometry(GEOMETRY)

## Header Text ##
Label(root, text = 'Select Organization to Synchronize', font =('Verdana', 15)).pack(side = TOP, pady = 20) 

clicked = StringVar() 
v = StringVar(root, "1") 

# Create Dropdown Menu ##
drop = OptionMenu( root , clicked , *res ) 
drop.pack()

## Sync and Exit Buttons ##
ICON1PATH = os.path.join(TMP_DIR,"sync.png")
icon1 = PhotoImage(file = r"%s" % ICON1PATH) 
pim1 = icon1.subsample(x=15,y=15) 
Button(root, text = '[ SYNC ]', image = pim1, compound = RIGHT, command = clicksel ).pack(side = LEFT, padx=20) 

ICON2PATH = os.path.join(TMP_DIR,"exit.png")
icon2 = PhotoImage(file = r"%s" % ICON2PATH) 
pim2 = icon2.subsample(x=27,y=27)
Button(root, text = '[ EXIT ]', image = pim2, compound = RIGHT, command = close).pack(pady=20, padx=20, side = RIGHT)
 
label = Label( root , text = " " ) 

label.pack()

## End construction of Box ##
root.mainloop()
