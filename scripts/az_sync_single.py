
"""
This file sync all data from defined data directory to Azure blob storage 
& organize the data according to the schema defined in the config (as after discussion)
"""

import  os, glob, shutil, configparser, hashlib, requests, pyodbc, re, uuid
from azure.storage.blob import ContainerClient, BlobServiceClient, BlobClient, ContentSettings

config = configparser.ConfigParser()
config.read('config.ini')

AZ_CONN_STR = config['main']['AZ_CONNECTION_STRING']
AZ_CONT_STORAGE = config['main']['AZ_CONTAINER_STORAGE']
# LOC_LOG_FILE = config['main']['LOCAL_LOG_FILE']
LOC_DIR_STORAGE = config['main']['LOCAL_DIR_STORAGE']
AZ_CONTAINER_LINK = config['main']['AZ_CONTAINER_LINK']

DBHOST = config['destdb']['dst_server']
DBNAME = config['destdb']['dst_db']
DBUSER = config['destdb']['dst_user']
DBPASS = config['destdb']['dst_pwd']

API_HOST = config['PDF_API']['API_HOST']
API_KEY = config['PDF_API']['API_KEY']

Media = ["mp4"]
Document = ["csv","html","htm","pptx","potx","potm","txt","dotx","dot","docx","docm","doc","xltm","xlsx","xlsb","xls","rtf","pdf"]
Images = ["jpg","gif","bmp","png","jpeg"]

## DB Operations ##

## Query Function ##
try:
    cnxn2 = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};SERVER='+DBHOST+';DATABASE='+DBNAME+';UID='+DBUSER+';PWD='+ DBPASS)
except Exception as e:
    print("[-] Failed to connect to DB @ %s/%s" % (DBHOST,DBNAME))
    print(e)
    exit()

cur2 = cnxn2.cursor()
print("[+] Connected to DB @ %s/%s" % (DBHOST,DBNAME))

def select_query(qry):
    # print("\nQUERY => %s\n" %(query))
    global SQLDATA
    SQLDATA = ''
    cur2.execute(qry)
    row = cur2.fetchone()
    while row: 
        str1 = str(row). replace('\'','')
        l = str1.replace(',',' :')
        SQLDATA = SQLDATA + " , " + l
        row = cur2.fetchone()
    return SQLDATA

def update_query(qry):
    cur2.execute(qry)

def pdf_api(TENANT_CODE):
    url = API_HOST+"/api/command/MigrationCommand/BulkUpload"
    TENANT_ID = select_query("select Id from AprioBoardPortal.Tenant where Code = '"+TENANT_CODE+"'")
    TENANT_ID = TENANT_ID[4:-4]
    # print(TENANT_ID)
    # print(TENANT_CODE)
    querystring = {"tenantId":TENANT_ID,"tenantCode":TENANT_CODE,"migrationKey":API_KEY}
    headers = { 'cache-control': "no-cache" }
    try:
        response = requests.request("POST", url, headers=headers, params=querystring)
        print("    [API] PDF API STATUS = %s" %(response.status_code))
        print(response.text)
    except Exception as e:
        print(e)
        print("    [API] FAILED PDF API @ %s" %(API_HOST))
        # exit()

## Db Update Functions ##
global TENANT_ID
global TENANT_NAME

## Function to Update Db for All files ##
def db_update_all(TENANT_CODE,FILE_NAME,FILE_SIZE,AZURE_FILE,EXT, TENANT_ID):
    FILE_NAME = FILE_NAME.replace("'","''")
    AZURE_FILE = AZURE_FILE.replace("'","''")
    # TENANT_NAME = select_query("select NAME from AprioBoardPortal.Tenant where Code = '"+TENANT_CODE+"'")
    FILE_IDS = select_query("select id from AprioBoardPortal.UploadedDoc where FileName = '"+FILE_NAME+"' and TenantId = '"+TENANT_ID+"'")
    for i in FILE_IDS.split (","):
        FID = i[2:-4].replace(' ','')
        if FID == '':
            pass
        else:
            try:
                if EXT == '.pdf':
                    update_query("UPDATE AprioBoardPortal.UploadedDoc set PdfUrl = '"+AZURE_FILE+"' , PdfSize = '"+FILE_SIZE+"' where Id = '"+FID+"' and FileName = '"+FILE_NAME+"'")
                else:
                    update_query("UPDATE AprioBoardPortal.UploadedDoc set FileUrl = '"+AZURE_FILE+"' , FileSize = '"+FILE_SIZE+"' where Id = '"+FID+"' and FileName = '"+FILE_NAME+"'")
                cur2.commit()
            except Exception as e:
                print("    [LOCAL] EXCEPTION in DB update : %s" % (e))
                pass


## Function to Update Db for XFDF files ##
def  db_update_xfdf(XFDF_FILE,AZURE_FILE):
    XFDF_FILE = XFDF_FILE.replace("'","''")
    AZURE_FILE = AZURE_FILE.replace("'","''")
    try:
        update_query("Update AprioBoardPortal.Annotation set AnnotationLink = '"+AZURE_FILE+"' where AnnotationLink  = '"+XFDF_FILE+"'")
        cur2.commit()
    except Exception as e:
        print("    [LOCAL] EXCEPTION in DB update : %s" % (e))
        pass

## Function to Update Db for IMAGE files ##
def  db_update_profile_images(IMAGE_FILE,AZURE_FILE,MEETING_ID):
    IMAGE_FILE = IMAGE_FILE.replace("'","''")
    AZURE_FILE = AZURE_FILE.replace("'","''")
    try:
        update_query("Update AprioBoardPortal.Contact set ProfileImageUrl = '"+AZURE_FILE+"'  , ProfileImageThubUrl = '"+AZURE_FILE+"' where ProfileImageUrl = '"+IMAGE_FILE+"'")
        update_query("Update AprioBoardPortal.Tenant set Logo = '"+AZURE_FILE+"' where Code = '"+MEETING_ID+"' and Logo = '"+IMAGE_FILE+"'")
        cur2.commit()
    except Exception as e:
        print("    [LOCAL] EXCEPTION in DB update : %s" % (e))
        pass

## Signature Update #

def db_update_signatures(FILE_NAME,AZURE_FILE,TENANT_ID):
    FILE_NAME = FILE_NAME.replace("'","''")
    AZURE_FILE = AZURE_FILE.replace("'","''")
    try:
        update_query("Update AprioBoardPortal.DocSignature set SignatureProofDocId = '"+AZURE_FILE+"' where TenantId = '"+TENANT_ID+"' and SignatureProofDocId = '"+FILE_NAME+"'")
        cur2.commit()
    except Exception as e:
        print("    [LOCAL] EXCEPTION in DB update : %s" % (e))
    pass

def load_signatures_list(TENANT_ID):
    signature_file_list = []
    FILE_IDS = select_query("select SignatureProofDocId from AprioBoardPortal.DocSignature where TenantId = '"+TENANT_ID+"'")
    for i in FILE_IDS.split (","):
        FID = i[2:-4].replace(' ','')
        if FID == '':
            pass
        else:
            signature_file_list.append(FID)
            pass
    return signature_file_list


## Azure Upload ##
blob_service_client =  BlobServiceClient.from_connection_string(AZ_CONN_STR)
def azure_upload(LOCAL_FILE,AZURE_FILE):
    blob_client = blob_service_client.get_blob_client(container=AZ_CONT_STORAGE,blob=AZURE_FILE)
    content_setting = ContentSettings(content_type=None)

    with open(LOCAL_FILE, "rb") as data:
        try:
            blob_client.upload_blob(data,content_settings=content_setting,overwrite=True,validate_content=True)
            print("    [AZURE] Uploaded %s " % (AZURE_FILE), end='\r')
        except Exception as e:
            pass


## Structurize files in local, Meeting ID ##
def organize_local(MEETING_ID):
    TENANT_ID = select_query("select Id from AprioBoardPortal.Tenant where Code = '"+MEETING_ID+"'")
    TENANT_ID = TENANT_ID[4:-4]
    TENANT_CODE = str(MEETING_ID)
    signature_file_list = load_signatures_list(TENANT_ID)
    FILES_PATH = os.path.join(LOC_DIR_STORAGE, MEETING_ID)

    ## Move files according to extensions , into respective directories.
    def extensionwise_syncing(FILETYPEARRAY,FILETYPE):
        print("    [LOCAL] Processing %s Files" % (FILETYPE))
        filenum = 0
        for EXT in FILETYPEARRAY:
            fileset = [file for file in glob.glob(FILES_PATH + "**/*."+EXT, recursive=True)]
            for SRC_FILE in fileset:
                UUID = uuid.uuid4().hex                 ## Assign New UUID ##
                SRC_FILE_NAME = os.path.basename(SRC_FILE)
                SRC_FILE_HALF_NAME, SRC_FILE_EXT = os.path.splitext(SRC_FILE_NAME)
                SRC_FILE_PATH = os.path.join(FILES_PATH, SRC_FILE_NAME)
                AZURE_FILE_BLOB = str(MEETING_ID+'/'+FILETYPE+'/'+SRC_FILE_HALF_NAME+'_'+UUID+SRC_FILE_EXT)
                AZURE_FILE_URL = str(AZ_CONTAINER_LINK+AZURE_FILE_BLOB)
                FILE_SIZE = str(os.path.getsize(SRC_FILE))
                EXT = str(SRC_FILE_EXT)
                azure_upload(SRC_FILE_PATH,AZURE_FILE_BLOB)                                         ## Azure Upload ##
                db_update_all(TENANT_CODE,SRC_FILE_NAME,FILE_SIZE,AZURE_FILE_URL,EXT,TENANT_ID)     ## Db Update ##
                if SRC_FILE_NAME in signature_file_list: 
                    db_update_signatures(SRC_FILE_NAME,AZURE_FILE_URL,TENANT_ID)                    ## Db Update ##

                filenum += 1

            if len(fileset) == 0 :
                pass
            else :
                print("\n    [%s] Uploaded %d %s files. " % (MEETING_ID, len(fileset), EXT) )
                pass

        if filenum > 0:
            print("    [%s] TOTAL %d files Uploaded to [%s]" % (MEETING_ID, filenum, FILETYPE) )


    ## Move rest of the files & directores to Others ##
    def rest_to_others(SUBDIR):
        SRC_PATH = os.path.join(LOC_DIR_STORAGE, MEETING_ID)
        filenum = 0
        DST_PATH = os.path.join(FILES_PATH,SUBDIR)
        fileset = os.listdir(SRC_PATH)
        for file in fileset:
            if file in LOC_SUBDIRS:
                pass
            else:
                try:
                    shutil.copy(os.path.join(SRC_PATH, file), DST_PATH)
                except Exception as e:
                    # shutil.copytree(os.path.join(SRC_PATH, file), DST_PATH)
                    pass
                filenum += 1

        if filenum > 0:
            print("    [%s] TOTAL %d files Uploaded to [%s]" % (MEETING_ID,filenum, SUBDIR) )

    def xfdf_organize(MEETING_ID):
        print("    [LOCAL] Processing XFDF Files")
        LOCAL_TEMP_DIR = os.path.join(LOC_DIR_STORAGE,"Temp")
        filenum = 0
        for path, currentDirectory, files in os.walk(LOCAL_TEMP_DIR):
            for file in files:
                if file.startswith("anma"+MEETING_ID) or file.startswith("anev"+MEETING_ID) or file.startswith("ando"+MEETING_ID):
                    SRC_FILE_NAME = file
                    SRC_FILE_PATH = os.path.join(LOCAL_TEMP_DIR,SRC_FILE_NAME)
                    if os.path.splitext(SRC_FILE_NAME)[1] == ".xfdf":
                        AZURE_FILE_BLOB = str(MEETING_ID+'/'+"Others"+'/'+SRC_FILE_NAME)
                        AZURE_FILE_URL = str(AZ_CONTAINER_LINK+AZURE_FILE_BLOB)
                        azure_upload(SRC_FILE_PATH,AZURE_FILE_BLOB)
                        db_update_xfdf(SRC_FILE_NAME,AZURE_FILE_URL)
                        filenum += 1

        print("\n    [%s] Uploaded %d %s files. " % (MEETING_ID, filenum, ".xfdf") )


    def profile_images_organize(MEETING_ID):
        print("    [LOCAL] Processing Profile Images")
        SRC_PATH = os.path.join(FILES_PATH,"Images")
        filenum = 0
        for path, currentDirectory, files in os.walk(SRC_PATH):
            for file in files:
                UUID = uuid.uuid4().hex             ## Assign New UUID ##
                SRC_FILE_NAME = file
                SRC_FILE_PATH = os.path.join(SRC_PATH, SRC_FILE_NAME)
                SRC_FILE_HALF_NAME, SRC_FILE_EXT = os.path.splitext(SRC_FILE_NAME)
                AZURE_FILE_BLOB = str(MEETING_ID+'/'+"Images"+'/'+SRC_FILE_HALF_NAME+'_'+UUID+SRC_FILE_EXT)
                AZURE_FILE_URL = str(AZ_CONTAINER_LINK+AZURE_FILE_BLOB)
                azure_upload(SRC_FILE_PATH,AZURE_FILE_BLOB)                             ## Azure Upload ##
                db_update_profile_images(SRC_FILE_NAME,AZURE_FILE_URL,MEETING_ID)       ## Db Update ##
                filenum += 1

        print("\n    [%s] Uploaded %d Profile Images to [Images]. " % (MEETING_ID, filenum) )
        pass

    profile_images_organize(MEETING_ID)
    xfdf_organize(MEETING_ID)
    extensionwise_syncing(Document,"Document")
    extensionwise_syncing(Images,"Images")
    extensionwise_syncing(Media,"Media")


def MAINS(MEETING_ID):
    print("[+] Received Request to Sync [%s]" % (MEETING_ID))
    ## Connect AZ
    container_client = ContainerClient.from_connection_string(conn_str=AZ_CONN_STR, container_name=AZ_CONT_STORAGE)
    print("[+] Connected to Azure.")
    ## Check if container Exists, If not then create.
    try:
        container_properties = container_client.get_container_properties()
        print("[+] Container [%s] Already Exists on Azure." % (AZ_CONT_STORAGE))

    except Exception as e:
        print("[-] Container [%s] Not Found on Azure, Creating ..." % (AZ_CONT_STORAGE))
        container_client.create_container()

    ## Check if LOC_DIR_STORAGE Exists or NOT

    is_locdir = os.path.isdir(LOC_DIR_STORAGE)  

    if is_locdir == True :
            print("[+] Local Directory [%s] Exists" % (LOC_DIR_STORAGE))
    else:
        print("[-] Local Directory %s Not Found." % (LOC_DIR_STORAGE))
        exit

    FILES_PATH = os.path.join(LOC_DIR_STORAGE, MEETING_ID)
    if os.path.isdir(FILES_PATH) == True:
        print("[+] Processing ORGID [%s]" % (MEETING_ID))
        organize_local(MEETING_ID)
        pdf_api(MEETING_ID)
        print ("[+] SUCESSFULLY Synced Tenant [%s] to Azure" % (MEETING_ID))
    else:
        print("[-] Tenant [%s] doesn't exists in Local Data Directory" % (MEETING_ID))
        exit
