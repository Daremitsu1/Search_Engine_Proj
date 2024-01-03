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


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import imageio
import fitz  # PyMuPDF




def zip_unzip_dag(zip_path,unzip_path,zip_csv_file_path,zip_csv_file_name,unzip_csv_file_path,unzip_csv_file_name):
    if os.path.exists(os.path.join(zip_csv_file_path,zip_csv_file_name)):  
        # Read the Existing CSV file into a DataFrame
            df_zip = pd.read_csv(os.path.join(zip_csv_file_path,zip_csv_file_name))
            # df_zip = df_zip[df_zip['UnzipStatus']=='Completed']
            old_zip_folder_names = df_zip["Zip Folder Name"].values
            old_zip_folder_status_list = list(df_zip["UnzipStatus"].values)
            print("old_zip_folder_status_list ",old_zip_folder_status_list)
            print("type old_zip_folder_status_list",type(old_zip_folder_status_list))
            all_zip_folder_names = [file for file in os.listdir(zip_path) if file.endswith(('.zip','.rar'))]
            new_zip_folder_names = [element for element in all_zip_folder_names if element not in old_zip_folder_names]
            if len(new_zip_folder_names)!=0:
                print("Zip folders name  : ",new_zip_folder_names)
                new_zip_folder_paths = [Path(zip_path+'/'+new_zip_folder_name) for new_zip_folder_name in new_zip_folder_names]
                print("Zip folders path  : ",new_zip_folder_paths)
                
                new_shipment_name_list = []
                new_zip_folder_name_list = []
                new_zip_folder_path_list = []
                new_zip_folder_status_list = []
                for new_zip_folder_name in new_zip_folder_names:
                    new_shipment_name_list.append(new_zip_folder_name.split('.')[0])
                    new_zip_folder_name_list.append(new_zip_folder_name)
                    new_zip_folder_path_list.append(zip_path+'/'+new_zip_folder_name)
                    new_zip_folder_status_list.append("Pending")

                new_data_to_write_zip = list(zip(new_shipment_name_list,new_zip_folder_name_list,new_zip_folder_path_list,new_zip_folder_status_list))

                df_zip = df_zip.append(pd.DataFrame(new_data_to_write_zip, columns=df_zip.columns), ignore_index=True)

                # Write the updated data back to the CSV file
                df_zip.to_csv(os.path.join(zip_csv_file_path, zip_csv_file_name), index=False)

                for new_zip_folder_path in new_zip_folder_paths:
                    if os.path.exists(unzip_path+'/'+'TempFolder'):
                        pass
                    else:
                        os.mkdir(unzip_path+'/'+'TempFolder')
                    unzip_destination_folder = unzip_path+'/'+'TempFolder'
                    with zipfile.ZipFile(new_zip_folder_path, 'r') as zip_ref:
                        zip_ref.extractall(unzip_destination_folder)
            
            
                for new_zip_folder_name in new_zip_folder_names:
                    new_zip_folder_status_list[new_zip_folder_names.index(new_zip_folder_name)] = "Completed"
                
                # Read the CSV file into a DataFrame
                df_zip = pd.read_csv(os.path.join(zip_csv_file_path,zip_csv_file_name))
                # df = df[df['UnzipStatus']=='Pending']
                # Modify the UnzipStatus column
                print("old_zip_folder_status_list",old_zip_folder_status_list)
                print("new_zip_folder_status_list",new_zip_folder_status_list)
                print("type old_zip_folder_status_list",type(old_zip_folder_status_list))
                print("type new_zip_folder_status_list",type(new_zip_folder_status_list))
                
                for item in new_zip_folder_status_list:
                    old_zip_folder_status_list.append(item)

                df_zip["UnzipStatus"] = old_zip_folder_status_list
                print("old_zip_folder_status_list ",old_zip_folder_status_list)
                print('df_zip["UnzipStatus"]',df_zip["UnzipStatus"])

                # Write the updated data back to the CSV file
                df_zip.to_csv(os.path.join(zip_csv_file_path, zip_csv_file_name), index=False)


                df_unzip = pd.read_csv(os.path.join(unzip_csv_file_path,unzip_csv_file_name))
                new_unzip_folder_name_list = []
                new_unzip_folder_file_name_list = []
                new_unzip_folder_file_path_list = []
                new_unzip_folder_shipment_name_list = []
                new_unzip_folder_file_type_list = []
                for new_currently_unzipped_folder in new_shipment_name_list:
                    for new_file in os.listdir(unzip_destination_folder+'/'+new_currently_unzipped_folder):
                        new_unzip_folder_shipment_name_list.append(new_zip_folder_name.split('.')[0])
                        new_unzip_folder_name_list.append(new_currently_unzipped_folder)
                        new_unzip_folder_file_name_list.append(new_file)
                        new_unzip_folder_file_path_list.append(unzip_destination_folder+'/'+new_currently_unzipped_folder+'/'+new_file)
                        new_unzip_folder_file_type_list.append('Not Known')

                new_data_to_write_unzip = list(zip(new_unzip_folder_shipment_name_list,new_unzip_folder_name_list,new_unzip_folder_file_name_list,new_unzip_folder_file_path_list,new_unzip_folder_file_type_list))
                df_unzip = df_unzip.append(pd.DataFrame(new_data_to_write_unzip, columns=df_unzip.columns), ignore_index=True)
                # Write the updated data back to the CSV file
                df_unzip.to_csv(os.path.join(unzip_csv_file_path, unzip_csv_file_name), index=False)
            
            
            elif len(new_zip_folder_names)==0:
                print(f"No zip folders found in {zip_path}")
                return None

    
    else:
        print("zip_path ",zip_path) 
        print("unzip_path ",unzip_path)
        print("files zip_path ",os.listdir(zip_path))
        # Function to check if a zip file exists in the specified path
        zip_folder_names = [file for file in os.listdir(zip_path) if file.endswith(('.zip','.rar'))]
        if len(zip_folder_names)!=0:
            print("Zip folders name  : ",zip_folder_names)
            zip_folder_paths = [Path(zip_path+'/'+zip_folder_name) for zip_folder_name in zip_folder_names]
            print("Zip folders path  : ",zip_folder_paths)
            
            shipment_name_list = []
            zip_folder_name_list = []
            zip_folder_path_list = []
            zip_folder_status_list = []
            for zip_folder_name in zip_folder_names:
                shipment_name_list.append(zip_folder_name.split('.')[0])
                zip_folder_name_list.append(zip_folder_name)
                zip_folder_path_list.append(zip_path+'/'+zip_folder_name)
                zip_folder_status_list.append("Pending")

            data_to_write_zip = list(zip(shipment_name_list,zip_folder_name_list,zip_folder_path_list,zip_folder_status_list))

            # Add headers
            header = ["Shipment Name", "Zip Folder Name", "Zip Folder Path", "UnzipStatus"]
            data_to_write_zip.insert(0, header)
            
            # Specify the file name
            # zip_csv_file_name = "zipfile_status_table.csv"

            # Write data to CSV file
            with open(os.path.join(zip_csv_file_path, zip_csv_file_name), 'w', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerows(data_to_write_zip)

            for zip_folder_path in zip_folder_paths:
                if os.path.exists(unzip_path+'/'+'TempFolder'):
                    pass
                else:
                    os.mkdir(unzip_path+'/'+'TempFolder')
                unzip_destination_folder = unzip_path+'/'+'TempFolder'
                with zipfile.ZipFile(zip_folder_path, 'r') as zip_ref:
                    zip_ref.extractall(unzip_path+'/'+'TempFolder')
            
            
            for zip_folder_name in zip_folder_names:
                zip_folder_status_list[zip_folder_names.index(zip_folder_name)] = "Completed"
                    
            # Read the CSV file into a DataFrame
            df_zip = pd.read_csv(os.path.join(zip_csv_file_path, zip_csv_file_name))

            # Modify the UnzipStatus column
            df_zip["UnzipStatus"] = zip_folder_status_list

            # Write the updated data back to the CSV file
            df_zip.to_csv(os.path.join(zip_csv_file_path, zip_csv_file_name), index=False)
                
    
            unzip_folder_name_list = []
            unzip_folder_file_name_list = []
            unzip_folder_file_path_list = []
            unzip_folder_shipment_name_list = []
            unzip_folder_file_type_list = []
            for currently_unzipped_folder in shipment_name_list:
                print("os.listdir(unzip_destination_folder+'/'+currently_unzipped_folder) : ",os.listdir(unzip_destination_folder+'/'+currently_unzipped_folder))
                for file in os.listdir(unzip_destination_folder+'/'+currently_unzipped_folder):
                    unzip_folder_shipment_name_list.append(zip_folder_name.split('.')[0])
                    unzip_folder_name_list.append(currently_unzipped_folder)
                    unzip_folder_file_name_list.append(file)
                    unzip_folder_file_path_list.append(unzip_destination_folder+'/'+currently_unzipped_folder+'/'+file)
                    
                    unzip_folder_file_type_list.append('Not Known')

            data_to_write_unzip = list(zip(unzip_folder_shipment_name_list,unzip_folder_name_list,unzip_folder_file_name_list,unzip_folder_file_path_list,unzip_folder_file_type_list))

            # Add headers
            header = ["Shipment Name", "UnZip Folder Name", "UnZip Folder File Name", "UnZip Folder FIle Path", "FileType"]
            data_to_write_unzip.insert(0, header)

            # Specify the file name
            # unzip_csv_file_name = "unzipfile_status_table.csv"
            # Write data to CSV file
            with open(os.path.join(unzip_csv_file_path, unzip_csv_file_name), 'w', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerows(data_to_write_unzip)
                   
            return shipment_name_list
            
        elif len(zip_folder_names)==0:
            print(f"No zip folders found in {zip_path}")
            return None



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
        'zip_unzip_dag', # Name of the DAG / workflow
        description='DAG to check whethera zip file is present or not. If zip file is there then unzip those files.',
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
    zip_unzip_dag = PythonOperator(
        task_id='zip_unzip_dag',
        python_callable=zip_unzip_dag,
        op_args=["/home/python/Nexdeck/ZipFolderShipment",
                    "/home/python/Nexdeck/UnzipFolderShipment",
                    "/home/python/Nexdeck/ZipFolderShipment",
                    "zipfile_status_table.csv",
                    "/home/python/Nexdeck/UnzipFolderShipment",
                    "unzipfile_status_table.csv"],  # Replace with your actual path
        dag=dag,
    )

    # Define the order in which the tasks are supposed to run
    # You can also define paralell tasks by using an array 
    # I.e. task1 >> [task2a, task2b] >> task3
    start_task >> zip_unzip_dag 
                