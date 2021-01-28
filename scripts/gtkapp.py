"""
This is the Main GUI app file which pulls the detaisl from Database & then render the same to Tkinter app
On selecting the button, itsync the same,
"""

import pyodbc, configparser, os
from tkinter import *
from tkinter.ttk import *

from az_sync_single import *

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

qry = 'SELECT OrgName,OrgAlias FROM dbo.tOrganizations'

global SQLDATA
SQLDATA = ''
cur.execute(qry)
row = cur.fetchone()
while row:
    #print(row)
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

GEOMETRY = "500x500"
global ORG_ID

def msgbox(GUIMSG):
    label.config(text = GUIMSG ).pack(side = BOTTOM,pady = 500)

def radsel():
    ORG_ID = str(v.get())
    label.config(text = "[ Selected " + ORG_ID + " ]", command = MAINS(ORG_ID)).pack(side = BOTTOM,pady = 500)
    MAINS(ORG_ID)

def radsync():
    ## TODOS ##
    try:
        msgbox("Syncing "+ ORG_ID + ".")
    except Exception as e:
        msgbox("Select ORG before syncing .")

root = Tk()
root.wm_title("Aprio Data Sync Utility")
FAVICON = os.path.join(TMP_DIR,"favicon.png")
root.call('wm', 'iconphoto', root._w, PhotoImage(file=FAVICON))
root.geometry(GEOMETRY)

Label(root, text = 'Select organization to synchronize.', 
    font =('Verdana', 15)
    ).pack(side = TOP, pady = 20) 

v = StringVar(root, "1") 

ORG_LIST = {}

for (text, value) in values.items(): 
    Radiobutton(root, text = text, variable = v, 
        value = value, command = radsel ).pack( ipady = 5) 

## Sync and Exit Buttons ##

ICON1PATH = os.path.join(TMP_DIR,"sync.png")
icon1 = PhotoImage(file = r"%s" % ICON1PATH) 
pim1 = icon1.subsample(x=15,y=15) 
Button(root, text = '[ SYNC ]', image = pim1, compound = RIGHT, command = radsync ).pack(side = LEFT) 

ICON2PATH = os.path.join(TMP_DIR,"exit.png")
icon2 = PhotoImage(file = r"%s" % ICON2PATH) 
pim2 = icon2.subsample(x=27,y=27)
Button(root, text = '[ EXIT ]', image = pim2, compound = RIGHT, command = root.destroy).pack(pady=20, side = RIGHT)

label = Label(root)
label.pack()

root.mainloop()
