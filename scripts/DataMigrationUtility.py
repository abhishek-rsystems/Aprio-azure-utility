'''
1. "config.ini" file would be created in the same folder
where
    a) scriptExecutorUtility
    b) config.ini will have info about 
        1) Host file
        2) SQL Scripts
        3) Migration Log
        4) Error log

        
    : "Host" to contain following
        1) Source Server address
        2) Source Server Username
        3) Source Server Password
        4) Destination Server address
        2) Destination Server Username
        3) Destination Server Password

    : Migration will contain
        1) Execution for OrgAlias
        2) Execution timings for each script
        3) Rows affected for each table / Script
        3) Status for each script : Success or Failure

    : Log will contain
        1) Script name and Error message

    : Script sheet has name of all scripts that are to be executed

'''

import os
import configparser
import sys
import pandas
from pandas import DataFrame
import pyodbc   #for SQL connection
from openpyxl import load_workbook
import datetime
from sqlalchemy import create_engine
import urllib
import RecentDocumentMigration as rcd
import requests

#Check if Host.xlsx exists in parent folder, If not then ask user to put it there and re click the button

#azuredb = 'ABP'


def MigrateData(orgAlias):
    excelHost = "config.ini"
    bit_error = 0
    error_message = ""
    sqlError = ""

    scriptName = ""
    tableName = ""
    sql = ""
    executionOn = 0
    params = ""
    startDateTime = datetime.datetime.now()
    pk = ''
    pk_alias = ''   

         
    config = configparser.ConfigParser()
    config.read('config.ini')

    src_server = config['sourcedb']['src_server']
    src_db = config['sourcedb']['src_db']
    src_user = config['sourcedb']['src_user']
    src_pwd = config['sourcedb']['src_pwd']

    stage_db = config['sourcedb']['stage_db']

    dst_server = config['destdb']['dst_server']
    dst_db = config['destdb']['dst_db']
    dst_user = config['destdb']['dst_user']
    dst_pwd = config['destdb']['dst_pwd']

    ACCESS_DB_PARENT_FOLDER = config['accessdb']['ACCESS_DB_PARENT_FOLDER']

    script_folder = config['script']['script_folder']

    API_HOST = config['PWD_API']['API_HOST']
    API_KEY = config['PWD_API']['API_KEY']

    logFileLoc = config['main']['LOCAL_LOG_FILE']

    logFile = open(logFileLoc+"\\"+orgAlias+".txt", "a")
    logFile.write(str(datetime.datetime.now())+"\n")
    
    try:
        #try connecting to Source server, if successful then try connecting to destination server. If successful then start executing SQL script
        src_cnxn = pyodbc.connect(driver="{SQL Server Native Client 11.0}",Server=src_server,Database=stage_db,uid=src_user, pwd=src_pwd,autocommit=False,CHARSET='Latin-1')
        cursor = src_cnxn.cursor()
        
        fd = open(script_folder+'\\'+'0_MigrationScript.sql', 'r')
        sqlCommandString = fd.read()
        fd.close()
        cursor.execute(sqlCommandString)
        src_cnxn.commit()
        
        dataFrame_Script = pandas.read_sql_query('select * from AprioBoardPortal.Migration_Scripts order by sno', src_cnxn)
        print("Connection to Source db successful")
        logFile.write("Connection to Source db successful \n")
    except pyodbc.Error as ex:
        bit_error = 1
        error_message="Not able to connect to Source Server"
        sqlError = ex.args[1]
        sqlError = sqlError.split(";")
        sqlError = sqlError[0]
    if bit_error == 0:
                      
        
        #take input from

        #global orgAlias
        
        #orgAlias = input("OrgAlias : ")
        region = input("Region: ")
        
        try :
            
                          
            ctr = 0
            
            
            for ctr in range(0,dataFrame_Script.shape[0]): #shape[0] returns rows
                
                scriptName = script_folder+"\\"+dataFrame_Script.iloc[ctr][1]
                tableName = dataFrame_Script.iloc[ctr][2]
                #executionOn = dataFrame_Script.iloc[ctr][3] #whether to execute on dest server or use dataframe to insert data
                
                print(scriptName) #print this to log file
                
                fd = open(scriptName, 'r')
                sqlCommandString = fd.read()
                fd.close()

                sqlCommandString = sqlCommandString.replace('go;','')
                sqlCommandString = sqlCommandString.replace('Use AprioBoardPortal','')
                sqlCommandString = sqlCommandString.replace('use AprioBoardPortal','')

                
               
                cursor.execute(sqlCommandString)#cursor is connected to "Azuredb" on source server
                
                if tableName == 'Migration_OrgAlias':
                    src_cnxn.commit()
                    cursor.execute('Insert into AprioBoardPortal.Migration_OrgAlias values({0})'.format(orgAlias))
                #src_cnxn.commit()

                
                #for loop ends here
            
            fd = open(script_folder+"\\"+'62_Update_TimeZone.sql', 'r')
           
            sqlCommandString = fd.read()
            fd.close()
            cursor.execute(sqlCommandString)

            cursor.execute("Update AprioBoardPortal.Tenant set TenantLocation ='"+str(region)+"' where code = '"+str(orgAlias)+"'")
            
        except FileNotFoundError as e:
            error_message = "Script file "+scriptName+" not found"
            sqlError = "Script file "+scriptName+" not found"
            bit_error = 1

        except pyodbc.OperationalError as ex:
            bit_error = 1
            error_message = "OperationalError"
            sqlError = ex
        except pyodbc.DataError as ex:
            bit_error = 1
            error_message = "DataError"
            sqlError = ex
        except pyodbc.IntegrityError as ex:
            bit_error = 1
            error_message = "IntegrityError"
            sqlError = ex.args[1]

        except pyodbc.ProgrammingError as ex:
            bit_error = 1
            error_message = "ProgrammingError"
            sqlError = ex.args[1]
        except pyodbc.NotSupportedError as ex:
            bit_error = 1
            error_message = "NotSupportedError"
            sqlError = ex
        except pyodbc.DatabaseError as ex:
            bit_error = 1
            error_message = "DatabaseError"
            sqlError = ex
        except pyodbc.Error as ex:
            bit_error = 1
            error_message = "Error"
            sqlError = ex
        except Exception as exc:
            bit_error = 1
            error_message = exc.args





    if bit_error == 1:
        src_cnxn.rollback()
        
        print(error_message)
        print('Data Migration unsuccessfull rolling back....')
        
        print(sqlError)
        logFile.write(error_message+"\n")
        logFile.write(sqlError+"\n")
        logFile.write('Data Migration unsuccessfull rolling back....\n')
        logFile.close()
        return
    else:
          
        src_cnxn.commit()
        print("scripts executed successfully in staging db")
        logFile.write("scripts executed successfully in staging db \n")


    print("Migrating data to Azure Now...")


   
    try:
        dest_cnxn = pyodbc.connect(driver="{SQL Server Native Client 11.0}",Server=dst_server,Database=dst_db,uid=dst_user, pwd=dst_pwd,autocommit=False)              
        
        df = pandas.read_sql_query('select * from information_schema.tables', dest_cnxn)
        print("Connection to Azure db successful")

        params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER="+dst_server+";"
                                 "DATABASE="+dst_db+";"
                                 "UID="+dst_user+";"
                                 "PWD="+dst_pwd+";")

        engine = create_engine("mssql+pyodbc:///?odbc_connect={0}".format(params),fast_executemany=True)

        #engine = create_engine("mssql+pyodbc://"+dst_user+":"+dst_pwd+"@"+dst_server+"/"+dst_db+"?charset=utf8mb4")


        
        src_cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='Latin-1')
        src_cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='Latin-1')
        src_cnxn.setencoding(encoding='Latin-1')

        with engine.connect() as conn: 
            ctr = 1 #to exclue migration_orgalias table
            with conn.begin(): 
                for ctr in range(1,dataFrame_Script.shape[0]): #shape[0] returns rows
                    tableName = dataFrame_Script.iloc[ctr][2]
                    pk = dataFrame_Script.iloc[ctr][3]
                    pk_alias = dataFrame_Script.iloc[ctr][4]

                    
                    #print(pk)
                    #print(pk_alias)
                    
                    sqlCommandString = "Select * from AprioBoardPortal.["+tableName+"]"
                    sqlCommand = pandas.read_sql_query(sqlCommandString, src_cnxn)
                    df = pandas.DataFrame(sqlCommand) #get data into dataframe

                    if tableName =='MeetingInstance': #drop computed columns from dataframe
                        df.drop("StartDate", axis=1, inplace=True)
                        df.drop("EndDate", axis=1, inplace=True)

                    if pk != 'XXX':
                        sqlCommandString = "Select "+pk+" as "+pk_alias+" from AprioBoardPortal.["+tableName+"]"
                        sqlCommand = pandas.read_sql_query(sqlCommandString, dest_cnxn)
                        dfDest = pandas.DataFrame(sqlCommand) #get data into dataframe

                        
                        df.set_index(pk)
                        dfDest.set_index(pk_alias)
                        dfMerged = pandas.merge(df,dfDest,how='left', left_on = pk, right_on= pk_alias)
                        
                        df = pandas.DataFrame(dfMerged[dfMerged[pk_alias].isnull()])
                        
                        df.drop(pk_alias,axis = 1, inplace=True)
                    
                   
                        
                        conn.execute("if exists(select OBJECT_ID from sys.identity_columns where OBJECT_ID = OBJECT_ID('Aprioboardportal."
                                     ""+tableName+"')) SET IDENTITY_INSERT [AprioBoardPortal].["+tableName+"] ON")
                        df.to_sql(tableName, con=conn, if_exists='append',index = False,  schema = 'AprioBoardPortal',chunksize=2000)
                        conn.execute("if exists(select OBJECT_ID from sys.identity_columns where OBJECT_ID = OBJECT_ID('Aprioboardportal."
                                     ""+tableName+"')) SET IDENTITY_INSERT [AprioBoardPortal].["+tableName+"] OFF")
                        
                        print(tableName+':'+str(df.shape[0]))
                    
                    else:
                        if tableName == 'ApplicationModuleTenant':
                            
                            df.set_index(['TenantId', 'ApplicationModuleId'])
                            
                            sqlCommandString = "Select TenantId as A_TenantId, ApplicationModuleId as A_ApplicationModuleId from AprioBoardPortal.["+tableName+"]"
                            sqlCommand = pandas.read_sql_query(sqlCommandString, dest_cnxn)
                            
                            dfDest = pandas.DataFrame(sqlCommand) #get data into dataframe

                            dfDest.set_index(['A_TenantId', 'A_ApplicationModuleId'])
                            
                            dfMerged = pandas.merge(df,dfDest,how='left', left_on = ['TenantId', 'ApplicationModuleId'], right_on= ['A_TenantId', 'A_ApplicationModuleId'])
                            
                            df = pandas.DataFrame(dfMerged[dfMerged['A_TenantId'].isnull()])
                            df.drop("A_TenantId",axis = 1, inplace=True)
                            df.drop("A_ApplicationModuleId",axis = 1, inplace=True)
                           
                            df.to_sql(tableName, con=conn, if_exists='append',index = False,  schema = 'AprioBoardPortal',chunksize=2000)
                          
                            
    except pyodbc.Error as ex:
        bit_error = 1
        error_message="Not able to connect to Azure Server"
        sqlError = ex.args[1]
        sqlError = sqlError.split(";")
        sqlError = sqlError[0]
        
        print(error_message)
        print(sqlError)
        logFile.write(error_message+"\n")
        logFile.write(sqlError+"\n")
    except Exception as exc:
        bit_error = 1
        error_message = exc.args
        print(error_message)
        logFile.write(error_message+"\n")
    if bit_error == 1:
        
        print("Data not migrated to Azure db....rolling back from Azure")
        logFile.write("Data not migrated to Azure db....rolling back from Azure \n")
        logFile.close()
    else:
        
        print("Data migration executed successfully to Azure db")
        logFile.write("Data migration executed successfully to Azure db \n")
    #in any case wipe out data from AzureDb on source server
    cursor.execute("delete from AprioboardPortal.Migration_OrgAlias")
    src_cnxn.commit()

    
    def password_api(orgAlias):

        df = pandas.read_sql_query("select id from AprioboardPortal.Tenant where code = '"+orgAlias+"'", dest_cnxn)
        TenantID = df.iloc[0,0]
        print(TenantID)
        url = API_HOST

        querystring = {"tenantId":"1","migrationKey":"migration_key"}
        
        #querystring = {"tenantId":'"'+str(TenantID)+'"',"migrationKey":'"'+API_KEY+'"'}
        headers = {'cache-control': "no-cache"}
        try:
            response = requests.request("POST", url, headers=headers, params=querystring)
            print("API worked")
            print(response.text)
            logFile.write("Passwords encrypted through API \n")
        except Exception as e:
            print("[API] FAILED Password API @ %s" %(API_HOST))
            print(e.args)
            logFile.write("[API] FAILED Password API @ %s \n" %(API_HOST))
            logFile.close()
            # exit()
        
    if bit_error != 1:
        password_api(orgAlias)

    logFile.close()

    
def MigrateAccessData(orgAlias):
    print("Migrating RecentDocsAccess data now.....from Access db")

    rcd.fn_MigrateDataLocal(orgAlias)
    rcd.fn_MigrateAzure()


#orgAlias = input("OrgAlias: ")
#region = input("Region: ")
#MigrateData(orgAlias)








                    
                    
        
        
