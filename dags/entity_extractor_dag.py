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
import imagehash
import itertools

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

import arcgis
from arcgis.learn import prepare_data
from arcgis.learn.text import EntityRecognizer
import pickle
import pandas as pd

def entity_extractor_dag(unzip_csv_file_path,unzip_csv_file_name,ocr_text_file_stored_path,entity_extarctor_model_stored_path,entity_extracted_csv_stored_path):
    if os.path.exists(entity_extracted_csv_stored_path+'/'+"Results"):
        entity_extracted_csv_stored_path = entity_extracted_csv_stored_path+'/'+"Results"
        print("entity_extracted_csv_stored_path : ",entity_extracted_csv_stored_path)
        df_unzip = pd.read_csv(os.path.join(unzip_csv_file_path,unzip_csv_file_name))
        all_shipment_name_list = list(np.unique(df_unzip[df_unzip["FileType"]=="Doc-Indexing-Extract"]["Shipment Name"].values))
        print("all_shipment_name_list : ",all_shipment_name_list)
        completed_entity_extracted_doc_shipment_name_list = os.listdir(entity_extracted_csv_stored_path)
        print("completed_entity_extracted_doc_shipment_name_list : ",completed_entity_extracted_doc_shipment_name_list)
        shipment_name_list = [item for item in all_shipment_name_list if item not in completed_entity_extracted_doc_shipment_name_list]
        print("shipment_name_list : ",shipment_name_list)
        if len(shipment_name_list)>0:
            for shipment_name in shipment_name_list:
                os.mkdir(entity_extracted_csv_stored_path+'/'+shipment_name)
                try:
                    with open(entity_extarctor_model_stored_path+'/'+'doc_extract.pickle', 'rb') as f:
                        new_ner = pickle.load(f)
                        print(new_ner)
                    results = new_ner.extract_entities(ocr_text_file_stored_path+'/'+shipment_name)
                    df = pd.DataFrame(results)
                    df.to_csv(entity_extracted_csv_stored_path+'/'+shipment_name+'/'+'doc_extract_{}.csv'.format(shipment_name))
                except Exception as e:
                            print(e)

        else:
            print("No New OCR Document Shipment Has Arrived.")
            pass

    else:
         os.mkdir(entity_extracted_csv_stored_path+'/'+"Results")
         entity_extracted_csv_stored_path = entity_extracted_csv_stored_path+'/'+"Results"
         print("entity_extracted_csv_stored_path : ",entity_extracted_csv_stored_path)
         df_unzip = pd.read_csv(os.path.join(unzip_csv_file_path,unzip_csv_file_name))
         all_shipment_name_list = list(np.unique(df_unzip[df_unzip["FileType"]=="Doc-Indexing-Extract"]["Shipment Name"].values))
         print("all_shipment_name_list : ",all_shipment_name_list)
         shipment_name_list = [item for item in all_shipment_name_list]
         print("shipment_name_list : ",shipment_name_list)
         if len(shipment_name_list)>0:
            for shipment_name in shipment_name_list:
                os.mkdir(entity_extracted_csv_stored_path+'/'+shipment_name)
                try:
                    with open(entity_extarctor_model_stored_path+'/'+'doc_extract.pickle', 'rb') as f:
                        new_ner = pickle.load(f)
                        print(new_ner)
                    results = new_ner.extract_entities(ocr_text_file_stored_path+'/'+shipment_name)
                    df = pd.DataFrame(results)
                    df.to_csv(entity_extracted_csv_stored_path+'/'+shipment_name+'/'+'doc_extract_{}.csv'.format(shipment_name))
                except Exception as e:
                            print(e)

         else:
            print("No New OCR Document Shipment Has Arrived.")
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
        'entity_extractor_dag', # Name of the DAG / workflow
        description='DAG to check whethera zip file is present or not. If zip file is there then unzip files. Then classify the files into text document and image file. Then generate ocr from the text document and also segregate the images into different task and subtask',
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
    entity_extractor_dag = PythonOperator(
        task_id='entity_extractor_dag',
        python_callable=entity_extractor_dag,
        op_args=["/home/python/Nexdeck/UnzipFolderShipment",
                    "unzipfile_status_table.csv",
                    "/home/python/Nexdeck/UnzipFolderShipment/DocumentIndexingExtraction/OCRTextFiles",
                    "/home/python/Nexdeck/Models/Doc-Indexing-Extract/models",
                    "/home/python/Nexdeck/UnzipFolderShipment/DocumentIndexingExtraction"],  # Replace with your actual path
        dag=dag,
    )

    
    # Define the order in which the tasks are supposed to run
    # You can also define paralell tasks by using an array 
    # I.e. task1 >> [task2a, task2b] >> task3
    start_task >> entity_extractor_dag 
