
"""
This file sync all data from defined data directory to Azure blob storage 
& organize the data according to the schema defined in the config (as after discussion)
"""

import  os, glob, shutil, configparser
from azure.storage.blob import ContainerClient, BlobServiceClient, BlobClient, ContentSettings

config = configparser.ConfigParser()
config.read('config.ini')

AZ_CONN_STR = config['main']['AZ_CONNECTION_STRING']
AZ_CONT_STORAGE = config['main']['AZ_CONTAINER_STORAGE']
# LOC_LOG_FILE = config['main']['LOCAL_LOG_FILE']
LOC_DIR_STORAGE = config['main']['LOCAL_DIR_STORAGE']

""" 
## List has issues with configparser, storing them directly.
Media = config['main']['Media']
Document = config['main']['Document']
Images = config['main']['Images']
"""

Media = ["mp4"]
Document = ["xml","xls","txt","xlsx","cxv","doc","docx","pdf","ppt","pptx"]
Images = ["jpg","gif","bmp","png"]

## Connect AZ
container_client = ContainerClient.from_connection_string(conn_str=AZ_CONN_STR, container_name=AZ_CONT_STORAGE)
print("[+] Connected to Azure.")

## Check if container Exists, If not then create.
try:
    container_properties = container_client.get_container_properties()
    print("[+] Container %s Exists." % (AZ_CONT_STORAGE))

except Exception as e:
    print("[-] Container %s Not Found, Creating ..." % (AZ_CONT_STORAGE))
    container_client.create_container()

# Check if LOC_DIR_STORAGE Exists or NOT

is_locdir = os.path.isdir(LOC_DIR_STORAGE)  

if is_locdir == True :
        print("[+] Local Directory %s Exists" % (LOC_DIR_STORAGE))
else:
    print("[-] Local Directory %s Not Found." % (LOC_DIR_STORAGE))
    exit

## Structurize files in local, Meeting ID

def organize_local(MEETING_ID):
    FILES_PATH = os.path.join(LOC_DIR_STORAGE, MEETING_ID)

    ## Check Subdirectories Inside Directory, if not then create.

    LOC_SUBDIRS = [ 'Document','Images','Media', 'Others' ]

    for subdir in LOC_SUBDIRS:
        subdir_path = os.path.join(FILES_PATH,subdir)
        if os.path.isdir(subdir_path) == False :
            os.mkdir(subdir_path)
            print("    [%s] Created SubDir [%s]." % (MEETING_ID,subdir))
        else:
            print("    [%s] SubDir [%s] already exists." % (MEETING_ID,subdir))
            pass

    ## Move files according to extensions , into respective directories.
    def extensionwise_organize(TYPEPATH,TYPEPATH2):
        filenum = 0
        for EXT in TYPEPATH:
            # print(EXT)
            fileset = [file for file in glob.glob(FILES_PATH + "**/*."+EXT, recursive=True)]
            for file in fileset:
                # print(file)
                shutil.move(file, os.path.join(FILES_PATH,TYPEPATH2))
                filenum += 1
            if len(fileset) == 0 :
                pass
            else :
                print("    [%s] Moved %d %s files to %s directory" % (MEETING_ID,len(fileset), EXT, TYPEPATH2) )
                pass

        if filenum > 0:
            print("    [%s] Moved %d files to %s directory" % (MEETING_ID,filenum, TYPEPATH2) )

    extensionwise_organize(Document,"Document")
    extensionwise_organize(Images,"Images")
    extensionwise_organize(Media,"Media")

    ## Move rest of the files & directores to Others.
    def rest_to_others(SUBDIR):
        filenum = 0
        target_dir = os.path.join(FILES_PATH,SUBDIR)
        fileset = os.listdir(FILES_PATH)
        for file in fileset:
            if file in LOC_SUBDIRS:
                pass
            else:
                shutil.move(os.path.join(FILES_PATH, file), target_dir)
                filenum += 1

        if filenum > 0:
            print("    [+] Moved %d files to %s directory" % (filenum, SUBDIR) )

    rest_to_others("Others")

def azure_upload(MEETING_ID):
    FILES_PATH = os.path.join(LOC_DIR_STORAGE, MEETING_ID)
    blob_service_client =  BlobServiceClient.from_connection_string(AZ_CONN_STR)
    filenum = 0
    for r,d,f in os.walk(FILES_PATH):        
        if f:
            for file in f:
                file_path_on_azure = os.path.join(r,file).replace(LOC_DIR_STORAGE,"")
                file_path_on_local = os.path.join(r,file)
                # print(file_path_on_azure, file_path_on_local)
                blob_client = blob_service_client.get_blob_client(container=AZ_CONT_STORAGE,blob=file_path_on_azure)
                content_setting = ContentSettings(content_type=None)
                filenum += 1
                with open(file_path_on_local, "rb") as data:
                    try:
                        blob_client.upload_blob(data,content_settings=content_setting)
                        print("      [/] Uploaded - %s" % (file_path_on_azure))

                    except Exception as e:
                        # print(e)
                        pass
    print("    [+] Parsed Total %d files from %s" % (filenum,MEETING_ID))


FOLDERS = os.listdir(LOC_DIR_STORAGE)
for MEETING_ID in FOLDERS:
    FILES_PATH = os.path.join(LOC_DIR_STORAGE, MEETING_ID)
    if os.path.isdir(FILES_PATH) == True:
        # print("Congo its a directory")
        print("[+] Processing [%s]" % (MEETING_ID))
        organize_local(MEETING_ID)
        azure_upload(MEETING_ID)
    else:
        pass
