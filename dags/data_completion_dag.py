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

import arcgis
from arcgis.learn import prepare_textdata
from arcgis.learn.text import TextClassifier, SequenceToSequence
import pickle
import pandas as pd
from pathlib import Path
import os
import pandas as pd


import arcgis
from arcgis.learn import prepare_textdata
from arcgis.learn.text import SequenceToSequence
import pandas as pd
from pathlib import Path
import os
import pandas as pd

def data_completion_dag(unzip_csv_file_path,unzip_csv_file_name,incomplete_data_file_stored_path,completed_data_file_destination_path,data_complete_model_path,completed_csv_file_name):
    if os.path.exists(completed_data_file_destination_path+'/'+"Results"):
        completed_data_file_destination_path = completed_data_file_destination_path+'/'+"Results"
        df_unzip = pd.read_csv(os.path.join(unzip_csv_file_path,unzip_csv_file_name))
        all_shipment_name_list = list(np.unique(df_unzip[df_unzip["FileType"]=="DataCompletion"]["Shipment Name"].values))
        completed_completed_data_shipment_name_list = os.listdir(completed_data_file_destination_path)
        shipment_name_list = [item for item in all_shipment_name_list if item not in completed_completed_data_shipment_name_list]
        if len(shipment_name_list)>0:
            for shipment_name in shipment_name_list:
                files = os.listdir(incomplete_data_file_stored_path+'/'+shipment_name)
                for file in files:
                    file_path = incomplete_data_file_stored_path+'/'+shipment_name+'/'+file
                    df = pd.read_csv(file_path)
                    #print(df)
                    model_path = os.path.join(data_complete_model_path,"data_classifier","data_classifier.emd")
                    model = TextClassifier.from_model(model_path)
                    #print(model)
                    text_list = df["Address"].values
                    data_predictions = model.predict(text_list)
                    results_df = pd.DataFrame(data_predictions, columns=['Address','Country','Confidence'])
                    print(results_df)
                    if os.path.exists(completed_data_file_destination_path + '/' + shipment_name):
                        pass
                    else:
                        os.mkdir(completed_data_file_destination_path + '/' + shipment_name)
                    results_df.to_csv(completed_data_file_destination_path + '/' + shipment_name + '/' + completed_csv_file_name,index=False)

    else:
        os.mkdir(completed_data_file_destination_path+'/'+"Results")
        completed_data_file_destination_path = completed_data_file_destination_path+'/'+"Results"
        df_unzip = pd.read_csv(os.path.join(unzip_csv_file_path,unzip_csv_file_name))
        all_shipment_name_list = list(np.unique(df_unzip[df_unzip["FileType"]=="DataCompletion"]["Shipment Name"].values))
        shipment_name_list = [item for item in all_shipment_name_list]
        if len(shipment_name_list)>0:
            for shipment_name in shipment_name_list:
                files = os.listdir(incomplete_data_file_stored_path+'/'+shipment_name)
                for file in files:
                    file_path = incomplete_data_file_stored_path+'/'+shipment_name+'/'+file
                    df = pd.read_csv(file_path)
                    #print(df)
                    model_path = os.path.join(data_complete_model_path,"data_classifier","data_classifier.emd")
                    model = TextClassifier.from_model(model_path)
                    #print(model)
                    text_list = df["Address"].values
                    data_predictions = model.predict(text_list)
                    results_df = pd.DataFrame(data_predictions, columns=['Address','Country','Confidence'])
                    print(results_df)
                    if os.path.exists(completed_data_file_destination_path + '/' + shipment_name):
                        pass
                    else:
                        os.mkdir(completed_data_file_destination_path + '/' + shipment_name)

                    results_df.to_csv(completed_data_file_destination_path + '/' + shipment_name + '/' + completed_csv_file_name,index=False)


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
        'data_completion_dag', # Name of the DAG / workflow
        description='DAG to complete data from csv and excel data',
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
    data_completion_dag = PythonOperator(
        task_id='data_completion_dag',
        python_callable=data_completion_dag,
        op_args=["/home/python/Nexdeck/UnzipFolderShipment",
                    "unzipfile_status_table.csv",
                    "/home/python/Nexdeck/UnzipFolderShipment/DataStandardization/DataCompletion/IngestedFiles",
                    "/home/python/Nexdeck/UnzipFolderShipment/DataStandardization/DataCompletion",
                    "/home/python/Nexdeck/Models/Doc-Standardization",
                    "results_df_completed.csv"],  # Replace with your actual path
        dag=dag,
    )


    # Define the order in which the tasks are supposed to run
    # You can also define paralell tasks by using an array 
    # I.e. task1 >> [task2a, task2b] >> task3
    start_task >> data_completion_dag
                
# data_completion_dag(unzip_csv_file_path="/home/python/Nexdeck/UnzipFolderShipment",
#                     unzip_csv_file_name="unzipfile_status_table.csv",
#                     incomplete_data_file_stored_path="/home/python/Nexdeck/UnzipFolderShipment/DataStandardization/Datacompletion/IngestedFiles",
#                     completed_data_file_destination_path="/home/python/Nexdeck/UnzipFolderShipment/DataStandardization/Datacompletion",
#                     model_path="/home/python/Nexdeck/Models/Doc-Standardization",
#                     completed_csv_file_name="addresses-completion-standardized-results.csv")


                    


                
                    
