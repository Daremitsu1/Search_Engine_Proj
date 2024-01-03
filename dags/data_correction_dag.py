

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

def data_correction_dag(unzip_csv_file_path,unzip_csv_file_name,incorrect_data_file_stored_path,corrected_data_file_destination_path,data_correction_model_path,corrected_csv_file_name):
    if os.path.exists(corrected_data_file_destination_path+'/'+"Results"):
        corrected_data_file_destination_path = corrected_data_file_destination_path+'/'+"Results"
        df_unzip = pd.read_csv(os.path.join(unzip_csv_file_path,unzip_csv_file_name))
        all_shipment_name_list = list(np.unique(df_unzip[df_unzip["FileType"]=="DataCorrection"]["Shipment Name"].values))
        completed_corrected_data_shipment_name_list = os.listdir(corrected_data_file_destination_path)
        shipment_name_list = [item for item in all_shipment_name_list if item not in completed_corrected_data_shipment_name_list]
        if len(shipment_name_list)>0:
            for shipment_name in shipment_name_list:
                files = os.listdir(incorrect_data_file_stored_path+'/'+shipment_name)
                for file in files:
                    file_path = incorrect_data_file_stored_path+'/'+shipment_name+'/'+file
                    df = pd.read_csv(file_path)
                    #print(df)
                    model_path = os.path.join(data_correction_model_path,"data_seq2seq","seq2seq_bleu.emd")
                    model = SequenceToSequence.from_model(model_path)
                    #print(model)
                    addresses_column = "non-std-address"
                    if addresses_column in df.columns:
                        addresses_list = df[addresses_column].astype(str).tolist()
                        seq_predictions = model.predict(addresses_list,num_beams=6,max_length=100)
                        std_addresses = [pred[1] for pred in seq_predictions]
                        results_df = pd.DataFrame({"std-addresses": std_addresses})
                        final_results_df = pd.concat([df, results_df],axis=1)
                        print(final_results_df)
                        if os.path.exists(corrected_data_file_destination_path + '/' + shipment_name):
                            pass
                        else:
                            os.mkdir(corrected_data_file_destination_path + '/' + shipment_name)
                        final_results_df.to_csv(corrected_data_file_destination_path + '/' + shipment_name + '/' + corrected_csv_file_name,index=False)

    else:
        os.mkdir(corrected_data_file_destination_path+'/'+"Results")
        corrected_data_file_destination_path = corrected_data_file_destination_path+'/'+"Results"
        df_unzip = pd.read_csv(os.path.join(unzip_csv_file_path,unzip_csv_file_name))
        all_shipment_name_list = list(np.unique(df_unzip[df_unzip["FileType"]=="DataCorrection"]["Shipment Name"].values))
        shipment_name_list = [item for item in all_shipment_name_list]
        if len(shipment_name_list)>0:
            for shipment_name in shipment_name_list:
                files = os.listdir(incorrect_data_file_stored_path+'/'+shipment_name)
                for file in files:
                    file_path = incorrect_data_file_stored_path+'/'+shipment_name+'/'+file
                    df = pd.read_csv(file_path)
                    #print(df)
                    model_path = os.path.join(data_correction_model_path,"data_seq2seq","seq2seq_bleu.emd")
                    model = SequenceToSequence.from_model(model_path)
                    #print(model)
                    addresses_column = "non-std-address"
                    if addresses_column in df.columns:
                        addresses_list = df[addresses_column].astype(str).tolist()
                        seq_predictions = model.predict(addresses_list,num_beams=6,max_length=100)
                        std_addresses = [pred[1] for pred in seq_predictions]
                        results_df = pd.DataFrame({"std-addresses": std_addresses})
                        final_results_df = pd.concat([df, results_df],axis=1)
                        print(final_results_df)
                        if os.path.exists(corrected_data_file_destination_path + '/' + shipment_name):
                            pass
                        else:
                            os.mkdir(corrected_data_file_destination_path + '/' + shipment_name)
                        final_results_df.to_csv(corrected_data_file_destination_path + '/' + shipment_name + '/' + corrected_csv_file_name,index=False)


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
        'data_correction_dag', # Name of the DAG / workflow
        description='DAG to correct data from csv and excel data',
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
    data_correction_dag = PythonOperator(
        task_id='data_correction_dag',
        python_callable=data_correction_dag,
        op_args=["/home/python/Nexdeck/UnzipFolderShipment",
                    "unzipfile_status_table.csv",
                    "/home/python/Nexdeck/UnzipFolderShipment/DataStandardization/DataCorrection/IngestedFiles",
                    "/home/python/Nexdeck/UnzipFolderShipment/DataStandardization/DataCorrection",
                    "/home/python/Nexdeck/Models/Doc-Standardization",
                    "results_df_corrected.csv"],  # Replace with your actual path
        dag=dag,
    )


    # Define the order in which the tasks are supposed to run
    # You can also define paralell tasks by using an array 
    # I.e. task1 >> [task2a, task2b] >> task3
    start_task >> data_correction_dag