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
import fitz  #



def ocr_extraction_dag(unzip_csv_file_path,unzip_csv_file_name,doc_file_stored_path,doc_file_ocr_destination_path):
    if os.path.exists(doc_file_ocr_destination_path+'/'+"OCRTextFiles"):
        doc_file_ocr_destination_path = doc_file_ocr_destination_path+'/'+"OCRTextFiles"
        df_unzip = pd.read_csv(os.path.join(unzip_csv_file_path,unzip_csv_file_name))
        all_shipment_name_list = list(np.unique(df_unzip[df_unzip["FileType"]=="Doc-Indexing-Extract"]["Shipment Name"].values))
        completed_doc_ocr_shipment_name_list = os.listdir(doc_file_ocr_destination_path)
        shipment_name_list = [item for item in all_shipment_name_list if item not in completed_doc_ocr_shipment_name_list]
        if len(shipment_name_list)>0:
            for shipment_name in shipment_name_list:
                files = os.listdir(doc_file_stored_path+'/'+shipment_name)
                for file in files:
                    file_path = doc_file_stored_path+'/'+shipment_name+'/'+file
                    if file_path.lower().endswith(('.pdf', '.tif','.tiff')):
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
                            
                            # Check if the extracted text is not empty
                            if bool(content.strip()):
                                print("It Is A Text File")
                            
                                                    
                            ocr_text_file_destination_path = doc_file_ocr_destination_path
                            if os.path.exists(ocr_text_file_destination_path+'/'+shipment_name):
                                pass
                            else:
                                os.mkdir(ocr_text_file_destination_path+'/'+shipment_name)

                            with open(ocr_text_file_destination_path+'/'+shipment_name+'/'+file.split('.')[0]+'.txt', 'w', encoding='utf-8') as docfile:
                                docfile.write(content)
                            print("OCR of the Text Document Task Completed")

                        except:
                            import time
                            time.sleep(6)
                    else:
                        pass
        else:
            print("No New Doc Shipment Has Arrived.")
            pass
    
    
    
    else:
        os.mkdir(doc_file_ocr_destination_path+'/'+"OCRTextFiles")
        doc_file_ocr_destination_path = doc_file_ocr_destination_path+'/'+"OCRTextFiles"
        df_unzip = pd.read_csv(os.path.join(unzip_csv_file_path,unzip_csv_file_name))
        all_shipment_name_list = list(np.unique(df_unzip[df_unzip["FileType"]=="Doc-Indexing-Extract"]["Shipment Name"].values))
        shipment_name_list = [item for item in all_shipment_name_list]
        if len(shipment_name_list)>0:
            for shipment_name in shipment_name_list:
                files = os.listdir(doc_file_stored_path+'/'+shipment_name)
                for file in files:
                    file_path = doc_file_stored_path+'/'+shipment_name+'/'+file
                    if file_path.lower().endswith(('.pdf', '.tif','.tiff')):
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
                            
                            # Check if the extracted text is not empty
                            if bool(content.strip()):
                                print("It Is A Text File")
                            
                                                    
                            ocr_text_file_destination_path = doc_file_ocr_destination_path
                            if os.path.exists(ocr_text_file_destination_path+'/'+shipment_name):
                                pass
                            else:
                                os.mkdir(ocr_text_file_destination_path+'/'+shipment_name)

                            with open(ocr_text_file_destination_path+'/'+shipment_name+'/'+file.split('.')[0]+'.txt', 'w', encoding='utf-8') as docfile:
                                docfile.write(content)
                            print("OCR of the Text Document Task Completed")

                        except:
                            import time
                            time.sleep(6)
                    else:
                        pass
        else:
            print("No New Doc Shipment Has Arrived.")
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
        'ocr_extraction_dag', # Name of the DAG / workflow
        description='DAG to generate ocr from the text document',
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
    ocr_extraction_dag = PythonOperator(
        task_id='ocr_extraction_dag',
        python_callable=ocr_extraction_dag,
        op_args=["/home/python/Nexdeck/UnzipFolderShipment",
                    "unzipfile_status_table.csv",
                    "/home/python/Nexdeck/UnzipFolderShipment/DocumentIndexingExtraction/IngestedFiles",
                    "/home/python/Nexdeck/UnzipFolderShipment/DocumentIndexingExtraction"],  # Replace with your actual path
        dag=dag,
    )


    # Define the order in which the tasks are supposed to run
    # You can also define paralell tasks by using an array 
    # I.e. task1 >> [task2a, task2b] >> task3
    start_task >> ocr_extraction_dag
                