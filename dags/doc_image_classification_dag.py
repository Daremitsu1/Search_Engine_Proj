from datetime import datetime 
from datetime import timedelta
import pandas as pd
import numpy as np
import zipfile
from io import StringIO
import os
import shutil
import requests
import json

from pathlib import Path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
import csv
from datetime import datetime, timedelta
from PIL import Image
import cv2
import imageio
from io import StringIO

from matplotlib.image import imread
import matplotlib.pyplot as plt

from tensorflow.keras.preprocessing.image import img_to_array
from tensorflow.keras.preprocessing.image import load_img
from tensorflow.keras.models import load_model
import pickle
import json
import pytesseract
from PIL import Image

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import imageio
import fitz  # PyMuPDF


from datetime import datetime 
from datetime import timedelta
import pandas as pd
import numpy as np
import zipfile
from io import StringIO
import os
import shutil
import requests
import json

from pathlib import Path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
import csv
from datetime import datetime, timedelta
from PIL import Image
import cv2
import imageio
from io import StringIO

from matplotlib.image import imread
import matplotlib.pyplot as plt

from tensorflow.keras.preprocessing.image import img_to_array
from tensorflow.keras.preprocessing.image import load_img
from tensorflow.keras.models import load_model
import pickle
import json
import pytesseract
from PIL import Image

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import imageio
import fitz  # PyMuPDF



def doc_image_classification_dag(unzip_csv_file_path,unzip_csv_file_name,folder_path,file_destination_path):
    df_unzip = pd.read_csv(os.path.join(unzip_csv_file_path,unzip_csv_file_name))
    unzip_folder_file_type_list_old = list(df_unzip["FileType"].values)
    print("unzip_folder_file_type_list_old ",unzip_folder_file_type_list_old)
    shipment_name_list = list(np.unique(df_unzip[df_unzip["FileType"]=="Not Known"]["Shipment Name"].values))
    print("shipment_name_list : ",shipment_name_list)
    if len(shipment_name_list)>0:
        print("shipment_name_list ",shipment_name_list)
        # unzip_folder_file_type_list_updated = []
        for shipment_name in shipment_name_list:
            for file in os.listdir(folder_path+'/'+shipment_name):
                file_path = folder_path+'/'+shipment_name+'/'+file
                print("file_path ",file_path)
                if file_path.lower().endswith(('.pdf', '.tif','.tiff')):
                    # Check if it's a PDF or TIF text file
                    user_name = "NEXVAL123"
                    license_code = "0D06A12C-1134-484F-8F39-1D11F9B49281"
                    API_ENDPOINT = "http://147.135.15.63/restservices/processDocument?gettext=true&newline=1&getwords=true&ocroption=3&ocrtype=11&outputformat=txt"
                    
                    with open(file_path, 'rb') as image_file:
                        image_data = image_file.read()
                    try:
                        r = requests.post(url=API_ENDPOINT, data=image_data, auth=(user_name, license_code), timeout=300)
                        if r.status_code == 401:
                            # Please provide valid username and license code
                            print("Unauthorized request")
                            

                        # Decode Output response
                        jobj = json.loads(r.content)

                        ocrError = str(jobj["ErrorMessage"])

                        if ocrError != '':
                            # Error occurs during recognition
                            print("Recognition Error: " + ocrError)

                        content = ""
                        page = 0
                        for ocr_text in jobj["OCRText"][0]:
                            content = content + " " + ocr_text
                            page = page + 1

                    except:
                        import time
                        time.sleep(6)
                        
                    if bool(content.strip()):
                        print("It Is A Text File")

                        condition_file_type = (df_unzip["Shipment Name"]==shipment_name)&(df_unzip["FileType"]=="Not Known")&(df_unzip["UnZip Folder File Name"]==file)
                        df_unzip.loc[condition_file_type,"FileType"]="Doc-Indexing-Extract"
                        
                        if os.path.exists(file_destination_path+'/'+'DocumentIndexingExtraction'):
                            pass
                        else:
                            os.mkdir(file_destination_path+'/'+'DocumentIndexingExtraction')

                        if os.path.exists(file_destination_path+'/'+'DocumentIndexingExtraction'+'/'+'IngestedFiles'):
                            pass
                        else:
                            os.mkdir(file_destination_path+'/'+'DocumentIndexingExtraction'+'/'+'IngestedFiles')

                        if os.path.exists(file_destination_path+'/'+'DocumentIndexingExtraction'+'/'+'IngestedFiles'+'/'+shipment_name):
                            pass
                        else:
                            os.mkdir(file_destination_path+'/'+'DocumentIndexingExtraction'+'/'+'IngestedFiles'+'/'+shipment_name)
                            
                        shutil.copy(file_path, file_destination_path+'/'+'DocumentIndexingExtraction'+'/'+'IngestedFiles'+'/'+shipment_name)
                        
                    else:
                        print("It Is Not A Text File")

                elif file_path.lower().endswith(('.jpg', '.png','.jpeg','.JPEG','.PNG','.JPG','.Jpg','.Jpeg','.Png')):
                    try:
                        image = imageio.imread(file_path)
                        print("It Is A Image File")
                        condition_file_type = (df_unzip["Shipment Name"]==shipment_name)&(df_unzip["FileType"]=="Not Known")&(df_unzip["UnZip Folder File Name"]==file)
                        df_unzip.loc[condition_file_type,"FileType"]="Property-Preservation"
                        if os.path.exists(file_destination_path+'/'+'PropertyPreservation'):
                            pass
                        else:
                            os.mkdir(file_destination_path+'/'+'PropertyPreservation')

                        if os.path.exists(file_destination_path+'/'+'PropertyPreservation'+'/'+'IngestedFiles'):
                            pass
                        else:
                            os.mkdir(file_destination_path+'/'+'PropertyPreservation'+'/'+'IngestedFiles')

                        if os.path.exists(file_destination_path+'/'+'PropertyPreservation'+'/'+'IngestedFiles'+'/'+shipment_name):
                            pass
                        else:
                            os.mkdir(file_destination_path+'/'+'PropertyPreservation'+'/'+'IngestedFiles'+'/'+shipment_name)
                            
                        shutil.copy(file_path, file_destination_path+'/'+'PropertyPreservation'+'/'+'IngestedFiles'+'/'+shipment_name)



                    except Exception as e:
                        print("It Is A Not Image File")

                elif file_path.lower().endswith(('.csv', '.xlsx')):
                    print("It Is A Data File")
                    df_data = pd.read_csv(file_path)
                    data_columns = df_data.columns
                    condition_file_type = (df_unzip["Shipment Name"]==shipment_name)&(df_unzip["FileType"]=="Not Known")&(df_unzip["UnZip Folder File Name"]==file)
                    if "non-std-address" in data_columns:
                        df_unzip.loc[condition_file_type,"FileType"]="DataCorrection"
                        if os.path.exists(file_destination_path+'/'+'DataStandardization'):
                            pass
                        else:
                            os.mkdir(file_destination_path+'/'+'DataStandardization')
                        if os.path.exists(file_destination_path+'/'+'DataStandardization'+'/'+'DataCorrection'):
                            pass
                        else:
                            os.mkdir(file_destination_path+'/'+'DataStandardization'+'/'+'DataCorrection')

                        if os.path.exists(file_destination_path+'/'+'DataStandardization'+'/'+'DataCorrection'+'/'+'IngestedFiles'):
                            pass
                        else:
                            os.mkdir(file_destination_path+'/'+'DataStandardization'+'/'+'DataCorrection'+'/'+'IngestedFiles')

                        if os.path.exists(file_destination_path+'/'+'DataStandardization'+'/'+'DataCorrection'+'/'+'IngestedFiles'+'/'+shipment_name):
                            pass
                        else:
                            os.mkdir(file_destination_path+'/'+'DataStandardization'+'/'+'DataCorrection'+'/'+'IngestedFiles'+'/'+shipment_name)
                            
                        shutil.copy(file_path, file_destination_path+'/'+'DataStandardization'+'/'+'DataCorrection'+'/'+'IngestedFiles'+'/'+shipment_name)
                    elif "Address" in data_columns:
                        df_unzip.loc[condition_file_type,"FileType"]="DataCompletion"
                        if os.path.exists(file_destination_path+'/'+'DataStandardization'):
                            pass
                        else:
                            os.mkdir(file_destination_path+'/'+'DataStandardization')
                        
                        if os.path.exists(file_destination_path+'/'+'DataStandardization'+'/'+'DataCompletion'):
                            pass
                        else:
                            os.mkdir(file_destination_path+'/'+'DataStandardization'+'/'+'DataCompletion')

                        if os.path.exists(file_destination_path+'/'+'DataStandardization'+'/'+'DataCompletion'+'/'+'IngestedFiles'):
                            pass
                        else:
                            os.mkdir(file_destination_path+'/'+'DataStandardization'+'/'+'DataCompletion'+'/'+'IngestedFiles')

                        if os.path.exists(file_destination_path+'/'+'DataStandardization'+'/'+'DataCompletion'+'/'+'IngestedFiles'+'/'+shipment_name):
                            pass
                        else:
                            os.mkdir(file_destination_path+'/'+'DataStandardization'+'/'+'DataCompletion'+'/'+'IngestedFiles'+'/'+shipment_name)
                            
                        shutil.copy(file_path, file_destination_path+'/'+'DataStandardization'+'/'+'DataCompletion'+'/'+'IngestedFiles'+'/'+shipment_name)

        
        df_unzip.to_csv(os.path.join(unzip_csv_file_path, unzip_csv_file_name), index=False)

    else:
        print("No New Shipment Has Arrived.")
        pass


# Default parameters for the workflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG

with DAG(
        'doc_image_classification_dag', # Name of the DAG / workflow
        description='DAG to  classify the files into text document and image file.',
        default_args=default_args,
        catchup=False,
        schedule_interval=timedelta(minutes=2),  # Set the interval to run every 10 minutes
) as dag:
    # This operator does nothing. 
    start_task = EmptyOperator(
        task_id='start_task', # The name of the sub-task in the workflow.
        dag=dag # When using the "with Dag(...)" syntax you could leave this out
    )
    
    # With the PythonOperator you can run a python function.
    doc_image_classification_dag = PythonOperator(
        task_id='doc_image_classification_dag',
        python_callable=doc_image_classification_dag,
        op_args=["/home/python/Nexdeck/UnzipFolderShipment",
                     "unzipfile_status_table.csv",
                     "/home/python/Nexdeck/UnzipFolderShipment/TempFolder",
                     "/home/python/Nexdeck/UnzipFolderShipment"],  # Replace with your actual path
        dag=dag
    )

    # Define the order in which the tasks are supposed to run
    # You can also define paralell tasks by using an array 
    # I.e. task1 >> [task2a, task2b] >> task3
    start_task >> doc_image_classification_dag 
                
