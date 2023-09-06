#Get the librarys
import pyodbc
import pandas as pd
import os
import io
import sys
import unidecode
import unicodedata
import ftplib
import time
import zipfile
from datetime import datetime

#Set the important variables
server = 'server' 
database = 'database'
uid  ="user"
pwd = "password"
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';Trusted_Connection=no;UID=' + uid + ';PWD=' + pwd)

FTP_HOST = "ftp_server"
FTP_USER = "ftp_user"
FTP_PASS = "ftp_password"

cursor = cnxn.cursor()

#Get the Data from the server
try:
    cursor.execute("""SET LANGUAGE Spanish""")
    MAESTRO_VENTAS_FIJO_AMDOCS_ACT= pd.read_sql_query("""SELECT Columnms
  FROM table;""",cnxn)
except Exception as e:
    print("An error occurs: ", e)

#Clean the columns names
columnas_nec = MAESTRO_VENTAS_FIJO_AMDOCS_ACT.columns.values
columnas_nec = columnas_nec.split(sep=',')
column_mapping = {MAESTRO_VENTAS_FIJO_AMDOCS_ACT.columns[i]: columnas_nec[i] for i in range(len(MAESTRO_VENTAS_FIJO_AMDOCS_ACT.columns))}
MAESTRO_VENTAS_FIJO_AMDOCS_ACT.rename(columns=column_mapping, inplace=True)

#Set the path_of the file to be saved
file_path = os.path.join(os.getcwd(), 'MAESTRO_VENTAS_FIJO_AMDOCS_ACT.csv')

#Make sure to delete every unwanted character
def normalize_text(text):
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode()

MAESTRO_VENTAS_FIJO_AMDOCS_ACT = MAESTRO_VENTAS_FIJO_AMDOCS_ACT.applymap(lambda x: str(x)).applymap(normalize_text)


#Save the file making sure that the null values are save as ""
MAESTRO_VENTAS_FIJO_AMDOCS_ACT.to_csv(file_path, header = False, 
                    sep = ";", index = False, encoding = "utf-8")

with io.open(file_path, mode="r", encoding="latin-1") as fd:
    content = fd.read()
with io.open(file_path, mode="w", encoding="cp1252") as fd:
    fd.write(content)

#Wait untill the file is complete save before upload to the FTP_server
time.sleep(10)

#Upload the file to the FTP server
ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
ftp.encoding = "utf-8"
uploaded = False

while not uploaded:
    try:
        with open(file_path, "rb") as f:
            # use FTP's STOR command to upload the file
            ftp.storbinary(f"STOR {file_path}", f)
        uploaded = True
    except Exception as e:
        print(f"Error uploading {file_path}: {e}")
        print("Retrying in 5 seconds...")
        time.sleep(5)

ftp.quit()

#Wait until the file is complete upload to the FTP
time.sleep(10)

#Name the Files that will be saved locally
archivo_csv = file_path
nombre_csv = "nombre_csv.csv" + datetime.today.strptime("yyyy-MM-dd")
nombre_zip = os.path.join(os.getcwd(), "zip_file.zip")

with zipfile.ZipFile(nombre_zip, "a") as zip_file:
    zip_file.write(archivo_csv, nombre_csv)

#delete the CSV file after is saved in the zip
os.remove(archivo_csv)

#Delete old files from the zip
with zipfile.ZipFile(nombre_zip, 'r') as zip_ref:
    archivos_zip = zip_ref.namelist()

archivos_a_eliminar = archivos_zip[:-3]
for archivo in archivos_a_eliminar:
    zip_ref.extract(archivo)
    os.remove(archivo)