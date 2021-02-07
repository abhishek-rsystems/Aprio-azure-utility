
"""
This file sync all data from defined data directory to Azure blob storage 
& organize the data according to the schema defined in the config (as after discussion)
"""

import  os, glob, shutil, configparser, hashlib, requests, pyodbc, re
from azure.storage.blob import ContainerClient, BlobServiceClient, BlobClient, ContentSettings

config = configparser.ConfigParser()
config.read('config.ini')

AZ_CONN_STR = config['main']['AZ_CONNECTION_STRING']
AZ_CONT_STORAGE = config['main']['AZ_CONTAINER_STORAGE']
# LOC_LOG_FILE = config['main']['LOCAL_LOG_FILE']
LOC_DIR_STORAGE = config['main']['LOCAL_DIR_STORAGE']
TMP_DIR = config['main']['TMP_DIR']
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

## Calculate Hash of File ##

def calcmd5(filename):
    md5 = hashlib.md5()
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(128 * md5.block_size), b''):
            md5.update(chunk)
    return md5.hexdigest()

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
        str1 = str(row).replace('(','').replace(')',''). replace('\'','')
        l = str1.replace(',',' :')
        SQLDATA = SQLDATA + " , " + l
        row = cur2.fetchone()
    # test_str = SQLDATA[1:] ; print(test_str)
    return SQLDATA[3:-3]

def update_query(qry):
    cur2.execute(qry)

def pdf_api(TENANT_ID,TENANT_NAME,TENANT_CODE):
    url = API_HOST+"/api/command/MigrationCommand/BulkUpload"
    querystring = {"tenantId":TENANT_ID,"tenantName":TENANT_NAME,"tenantCode":TENANT_CODE,"migrationKey":API_KEY}
    headers = { 'cache-control': "no-cache" }
    try:
        response = requests.request("POST", url, headers=headers, params=querystring)
        print("    [API] PDF API STATUS = %s" %(response.status_code))
        # print(response.text)
    except Exception as e:
        print(e)
        print("    [API] FAILED PDF API @ %s" %(API_HOST))
        # exit()

## Db Update Functions ##
TENANT_ID = 0
TENANT_NAME = ''
## Function to Update Db for All files ##
def db_update_all(TENANT_CODE,FILE_NAME,FILE_SIZE,AZURE_FILE,EXT):
    TENANT_NAME = select_query("select NAME from AprioBoardPortal.Tenant where Code = '"+TENANT_CODE+"'")
    TENANT_ID = select_query("select Id from AprioBoardPortal.Tenant where Code = '"+TENANT_CODE+"'")
    FILE_IDS = select_query("select id from AprioBoardPortal.UploadedDoc where FileName = '"+FILE_NAME+"' and TenantId = '"+TENANT_ID+"'")
    for i in FILE_IDS.split (","):
        FID = i[1:-4]
        try:
            update_query("UPDATE AprioBoardPortal.UploadedDoc set FileName = '"+FILE_NAME+"' , FileUrl = '"+AZURE_FILE+"' , FileSize = '"+FILE_SIZE+"' where Id = '"+FID+"' and FileExtension = '"+EXT+"'")
            cur2.commit()
        except Exception as e:
            print("    [LOCAL] EXCEPTION in DB update : %s" % (e))
            pass

## Function to Update Db for XFDF files ##
def  db_update_xfdf(XFDF_FILE,AZURE_FILE):
    try:
        update_query("Update AprioBoardPortal.Annotation set AnnotationLink = '"+AZURE_FILE+"' where AnnotationLink  = '"+XFDF_FILE+"'")
        cur2.commit()
    except Exception as e:
        print("    [LOCAL] EXCEPTION in DB update : %s" % (e))
        pass

## Function to Update Db for IMAGE files ##
def  db_update_profile_images(IMAGE_FILE,AZURE_FILE,MEETING_ID):
    try:
        update_query("Update AprioBoardPortal.Contact set ProfileImageUrl = '"+AZURE_FILE+"'  , ProfileImageThubUrl = '"+AZURE_FILE+"' where ProfileImageUrl = '"+IMAGE_FILE+"'")
        update_query("Update AprioBoardPortal.Tenant set Logo = '"+AZURE_FILE+"' where Code = '"+MEETING_ID+"' and Logo = '"+IMAGE_FILE+"'")
        cur2.commit()
    except Exception as e:
        print("    [LOCAL] EXCEPTION in DB update : %s" % (e))
        pass

## Structurize files in local, Meeting ID ##
def organize_local(MEETING_ID):
    FILES_PATH = os.path.join(TMP_DIR, MEETING_ID)

    ## Check Subdirectories Inside Directory, if not then create.
    LOC_SUBDIRS = [ 'Document','Images','Media', 'Others' ]

    for subdir in LOC_SUBDIRS:
        subdir_path = os.path.join(FILES_PATH,subdir)
        if os.path.isdir(subdir_path) == False :
            os.makedirs(subdir_path,exist_ok=True)
            print("    [LOCAL] Created SubDir [%s]." % (subdir))
            pass
        else:
            print("    [LOCAL] SubDir [%s] already exists." % (subdir))
            pass

    ## Move files according to extensions , into respective directories.
    def extensionwise_organize(TYPEPATH,TYPEPATH2):
        filenum = 0
        for EXT in TYPEPATH:
            fileset = [file for file in glob.glob(os.path.join(LOC_DIR_STORAGE, MEETING_ID) + "**/*."+EXT, recursive=True)]
            for SRC_FILE in fileset:
                # print(file)
                SRC_FILE_NAME = os.path.basename(SRC_FILE)
                SRC_FILE_HALF_NAME, SRC_FILE_EXT = os.path.splitext(SRC_FILE_NAME)
                DST_FILE_STR = os.path.join(FILES_PATH,TYPEPATH2)
                DST_FILE = os.path.join(DST_FILE_STR,SRC_FILE_HALF_NAME+'_'+calcmd5(SRC_FILE)+SRC_FILE_EXT)   

                # DST_FILE = os.path.join(FILES_PATH,TYPEPATH2)+'/'+SRC_FILE_NAME+'_'+calcmd5(SRC_FILE)
                # print(file+" -> "+DST_FILE)
                
                shutil.copyfile(SRC_FILE, DST_FILE)
                filenum += 1
                
                ## DB Updations for all files ##
                FILE_SIZE = str(os.path.getsize(SRC_FILE))
                FILE_NAME = SRC_FILE_NAME
                TENANT_CODE = str(MEETING_ID)
                EXT = str(SRC_FILE_EXT)
                AZURE_FILE = str(AZ_CONTAINER_LINK+MEETING_ID+'/'+TYPEPATH2+'/'+SRC_FILE_HALF_NAME+'_'+calcmd5(SRC_FILE)+SRC_FILE_EXT)
                db_update_all(TENANT_CODE,FILE_NAME,FILE_SIZE,AZURE_FILE,EXT)

            if len(fileset) == 0 :
                pass
            else :
                print("    [LOCAL] Synced %d %s files to %s directory" % (len(fileset), EXT, TYPEPATH2) )
                pass

        if filenum > 0:
            print("    [LOCAL] TOTAL %d files Synced to [%s]" % (filenum, TYPEPATH2) )

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
            print("    [LOCAL] TOTAL %d files Synced to [%s]" % (filenum, SUBDIR) )


    def xfdf_organize(MEETING_ID):
        FILES_PATH = os.path.join(TMP_DIR, MEETING_ID)
        DST_PATH = os.path.join(FILES_PATH,"Others")
        LOCAL_TEMP_DIR = os.path.join(LOC_DIR_STORAGE,"Temp")
        filenum = 0
        for path, currentDirectory, files in os.walk(LOCAL_TEMP_DIR):
            for file in files:
                if file.startswith("anma"+MEETING_ID) or file.startswith("anev"+MEETING_ID) or file.startswith("ando"+MEETING_ID):
                    checkfile = file
                    if os.path.splitext(checkfile)[1] == ".xfdf":
                        # print(file)
                        filenum += 1
                        SRC_PATH = os.path.join(LOCAL_TEMP_DIR, checkfile)
                        shutil.copy(SRC_PATH, DST_PATH)
                        AZURE_FILE = str(AZ_CONTAINER_LINK+MEETING_ID+"/Others/"+checkfile)
                        db_update_xfdf(checkfile,AZURE_FILE)
        print("    [LOCAL] Synced %d %s files to [Others]" % (filenum, ".xfdf") )


    def profile_images_organize(MEETING_ID):

        LOCAL_DATA_DIR = os.path.join(LOC_DIR_STORAGE,MEETING_ID)
        SRC_PATH = os.path.join(LOCAL_DATA_DIR,"Images")

        LOCAL_TMP_DIR = os.path.join(TMP_DIR, MEETING_ID)
        DST_PATH = os.path.join(LOCAL_TMP_DIR,"Images")

        filenum = 0
        for path, currentDirectory, files in os.walk(SRC_PATH):
            for file in files:
                checkfile = file
                filenum += 1
                SRC_FILE = os.path.join(SRC_PATH, checkfile)
                filename, file_extension = os.path.splitext(checkfile)
                filehash = calcmd5(SRC_FILE)
                NEW_FILE_NAME = filename+'_'+filehash+file_extension
                DST_FILE = os.path.join(DST_PATH,NEW_FILE_NAME)
                shutil.copy(SRC_FILE, DST_FILE)

                AZURE_FILE = str(AZ_CONTAINER_LINK+MEETING_ID+"/Images/"+checkfile)
                db_update_profile_images(checkfile,AZURE_FILE,MEETING_ID)

        print("    [LOCAL] Synced %d Profile Images to [Images]" % (filenum) )
        pass

    profile_images_organize(MEETING_ID)
    xfdf_organize(MEETING_ID)

    extensionwise_organize(Document,"Document")
    extensionwise_organize(Images,"Images")
    extensionwise_organize(Media,"Media")
    # rest_to_others("Others")

def azure_upload(MEETING_ID):
    # FILES_PATH = os.path.join(LOC_DIR_STORAGE, MEETING_ID)
    FILES_PATH = os.path.join(TMP_DIR, MEETING_ID)

    blob_service_client =  BlobServiceClient.from_connection_string(AZ_CONN_STR)
    filenum = 0
    for r,d,f in os.walk(FILES_PATH):        
        if f:
            for file in f:
                file_path_on_azure = os.path.join(r,file).replace(TMP_DIR+'/','')
                file_path_on_local = os.path.join(r,file)
                # print(file_path_on_azure, file_path_on_local)
                blob_client = blob_service_client.get_blob_client(container=AZ_CONT_STORAGE,blob=file_path_on_azure)

                ## Check Md5 on Azure ##
                md5hash = calcmd5(file_path_on_local)
                local_hash = md5hash
                content_setting = ContentSettings(content_type=None)
                filenum += 1
                with open(file_path_on_local, "rb") as data:
                    try:
                        azure_prop = blob_client.get_blob_properties()
                        azure_hash = azure_prop.content_settings.content_md5.hex()
                        # print("%s == %s" %(azure_hash, local_hash))
                        if azure_hash == local_hash:
                            ## Skip upload if hash matched ##
                            print("    [AZURE] Checksum Matched, Skipping [%s]" % (file_path_on_azure))
                        else:
                            ## Reupload if hash is not matching ##
                            blob_client.upload_blob(data,content_settings=content_setting,overwrite=True)
                            print("    [AZURE] Blob ReUploaded @ [%s]" % (file_path_on_azure))
                            pass
                    ## if file is not there then upload fresh one ##       
                    except Exception as e:
                        blob_client.upload_blob(data,content_settings=content_setting,overwrite=True)
                        print("    [AZURE] Blob Uploaded @ [%s]" % (file_path_on_azure))
                        pass

    print("    [+] Parsed Total %d files from %s" % (filenum,MEETING_ID))


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

    # Check if LOC_DIR_STORAGE Exists or NOT

    is_locdir = os.path.isdir(LOC_DIR_STORAGE)  

    if is_locdir == True :
            print("[+] Local Directory [%s] Exists" % (LOC_DIR_STORAGE))
    else:
        print("[-] Local Directory %s Not Found." % (LOC_DIR_STORAGE))
        exit

    FILES_PATH = os.path.join(LOC_DIR_STORAGE, MEETING_ID)
    if os.path.isdir(FILES_PATH) == True:
        # print("[+] Congo its a directory")
        print("[+] Processing ORGID [%s]" % (MEETING_ID))
        organize_local(MEETING_ID)
        azure_upload(MEETING_ID)
        pdf_api(TENANT_ID,TENANT_NAME,MEETING_ID)
        print ("[+] SUCESSFULLY Synced ORGID [%s] to Azure" % (MEETING_ID))
    else:
        print("[-] ORGID [%s] doesn't exists in Local Data Directory" % (MEETING_ID))
        exit
