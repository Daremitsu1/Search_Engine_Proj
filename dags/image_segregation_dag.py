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


def image_segregation_dag(unzip_csv_file_path,unzip_csv_file_name,image_file_stored_path,segreagted_image_file_destination_path,model_label1_seg_path,model_door_seg_path,model_grass_seg_path):
    label1_img_seg_dict = {'Door Tasks': 0, 'Lawn Maintenance': 1}
    label1_img_seg_dict_reverse = {0:'Door Tasks', 1:'Lawn Maintenance'}

    door_img_seg_dict = {'Install Door Armor - 8955': 0, 'Install Exterior Door with Frame - 8953': 1, 'Repair Door Frame - 9173': 2, 'Repaired Replace Overhead Garage Door - 2215 3047': 3, 'Replaced Door - 0514 8952': 4}
    door_img_seg_dict_reverse = {0:'Install Door Armor - 8955', 1:'Install Exterior Door with Frame - 8953', 2:'Repair Door Frame - 9173', 3:'Repaired Replace Overhead Garage Door - 2215 3047', 4:'Replaced Door - 0514 8952'}

    grass_img_seg_dict = {'Initial Grass Cut - 2501': 0, 'Remove Vines - 2575': 1, 'Trim Shrubs - 2606': 2, 'Trim Trees - 9164': 3}
    grass_img_seg_dict_reverse = {0:'Initial Grass Cut - 2501', 1:'Remove Vines - 2575', 2:'Trim Shrubs - 2606', 3:'Trim Trees - 9164'}

    
    if os.path.exists(segreagted_image_file_destination_path+'/'+"SegregatedImageFiles"):
        segreagted_image_file_destination_path = segreagted_image_file_destination_path+'/'+"SegregatedImageFiles"
        df_unzip = pd.read_csv(os.path.join(unzip_csv_file_path,unzip_csv_file_name))
        all_shipment_name_list = list(np.unique(df_unzip[df_unzip["FileType"]=="Property-Preservation"]["Shipment Name"].values))
        completed_segreagted_image_shipment_name_list = os.listdir(segreagted_image_file_destination_path)
        shipment_name_list = [item for item in all_shipment_name_list if item not in completed_segreagted_image_shipment_name_list]
        if len(shipment_name_list)>0:
            for shipment_name in shipment_name_list:
                files = os.listdir(image_file_stored_path+'/'+shipment_name)
                for file in files:
                    file_path = image_file_stored_path+'/'+shipment_name+'/'+file
                    if file_path.lower().endswith(('.jpg', '.png','.jpeg','.JPEG','.PNG','.JPG','.Jpg','.Jpeg','.Png')):
                        try:
                            image = imageio.imread(file_path)
                            print("It Is A Image File")
                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name)

                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks')
                            
                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Install Door Armor - 8955'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Install Door Armor - 8955')

                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953')

                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Repair Door Frame - 9173'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Repair Door Frame - 9173')
                            
                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047')
                            
                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Replaced Door - 0514 8952'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Replaced Door - 0514 8952')

                            ###

                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance')
                            
                            ###
                            
                            
                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501')
                            
                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Remove Vines - 2575'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Remove Vines - 2575')
                            
                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Trim Shrubs - 2606'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Trim Shrubs - 2606')
                            
                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Trim Trees - 9164'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Trim Trees - 9164')

                        

                            X_1 = cv2.imread(file_path)
                            X_1 = cv2.resize(X_1,(210,210))
                            X_1  = X_1.reshape((1,210,210,3))
                            model_label1_seg = load_model(model_label1_seg_path)
                            P_1 = model_label1_seg.predict(X_1)
                            P_1 = np.argmax(P_1, axis=1)
                            if label1_img_seg_dict_reverse[P_1[0]]=='Door Tasks':
                                print("It is a door task image")
                                X_2 = cv2.imread(file_path)
                                X_2 = cv2.resize(X_2,(224,224))
                                X_2  = X_2.reshape((1,224,224,3))
                                model_door_seg = load_model(model_door_seg_path)
                                P_2 = model_door_seg.predict(X_2)
                                P_2 = np.argmax(P_2, axis=1)
                                if door_img_seg_dict_reverse[P_2[0]]=='Install Door Armor - 8955':
                                    print("It is a door task and Install Door Armor - 8955 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Install Door Armor - 8955'+'/'+file)
                                elif door_img_seg_dict_reverse[P_2[0]]=='Install Exterior Door with Frame - 8953':
                                    print("It is a door task and Install Exterior Door with Frame - 8953 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953'+'/'+file)
                                elif door_img_seg_dict_reverse[P_2[0]]=='Repair Door Frame - 9173':
                                    print("It is a door task and Repair Door Frame - 9173 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Repair Door Frame - 9173'+'/'+file)
                                elif door_img_seg_dict_reverse[P_2[0]]=='Repaired Replace Overhead Garage Door - 2215 3047':
                                    print("It is a door task and Repaired Replace Overhead Garage Door - 2215 3047 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047'+'/'+file)
                                elif door_img_seg_dict_reverse[P_2[0]]=='Replaced Door - 0514 8952':
                                    print("It is a door task and Replaced Door - 0514 8952 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Replaced Door - 0514 8952'+'/'+file)
                            
                            elif label1_img_seg_dict_reverse[P_1[0]]=='Lawn Maintenance':
                                print("It is a lawn maintenance task image")
                                X_2 = cv2.imread(file_path)
                                X_2 = cv2.resize(X_2,(224,224))
                                X_2  = X_2.reshape((1,224,224,3))
                                model_grass_seg = load_model(model_grass_seg_path)
                                P_2 = model_grass_seg.predict(X_2)
                                P_2 = np.argmax(P_2, axis=1)
                                if grass_img_seg_dict_reverse[P_2[0]]=='Initial Grass Cut - 2501':
                                    print("It is a lawn maintenance task and Initial Grass Cut - 2501 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501'+'/'+file)
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Remove Vines - 2575':
                                    print("It is a lawn maintenance task and Remove Vines - 2575 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Remove Vines - 2575'+'/'+file)
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Trim Shrubs - 2606':
                                    print("It is a lawn maintenance task and Trim Shrubs - 2606 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Trim Shrubs - 2606'+'/'+file)
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Trim Trees - 9164':
                                    print("It is a lawn maintenance task and Trim Trees - 9164 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Trim Trees - 9164'+'/'+file)
                            print("Image Segregation Task Completed")
                        except Exception as e:
                            print("It is not a Image File")
                    else:
                        pass
        else:
            print("No New Image Shipment Has Arrived.")
            pass
    else:
        os.mkdir(segreagted_image_file_destination_path+'/'+"SegregatedImageFiles")
        segreagted_image_file_destination_path = segreagted_image_file_destination_path+'/'+"SegregatedImageFiles"
        df_unzip = pd.read_csv(os.path.join(unzip_csv_file_path,unzip_csv_file_name))
        all_shipment_name_list = list(np.unique(df_unzip[df_unzip["FileType"]=="Property-Preservation"]["Shipment Name"].values))
        shipment_name_list = [item for item in all_shipment_name_list]
        if len(shipment_name_list)>0:
            for shipment_name in shipment_name_list:
                files = os.listdir(image_file_stored_path+'/'+shipment_name)
                for file in files:
                    file_path = image_file_stored_path+'/'+shipment_name+'/'+file
                    if file_path.lower().endswith(('.jpg', '.png','.jpeg','.JPEG','.PNG','.JPG','.Jpg','.Jpeg','.Png')):
                        try:
                            image = imageio.imread(file_path)
                            print("It Is A Image File")
                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name)

                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks')
                            
                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Install Door Armor - 8955'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Install Door Armor - 8955')

                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953')

                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Repair Door Frame - 9173'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Repair Door Frame - 9173')
                            
                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047')
                            
                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Replaced Door - 0514 8952'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Replaced Door - 0514 8952')

                            ###

                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance')
                            
                            ###
                            
                            
                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501')
                            
                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Remove Vines - 2575'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Remove Vines - 2575')
                            
                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Trim Shrubs - 2606'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Trim Shrubs - 2606')
                            
                            if os.path.exists(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Trim Trees - 9164'):
                                pass
                            else:
                                os.mkdir(segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Trim Trees - 9164')

                        

                            X_1 = cv2.imread(file_path)
                            X_1 = cv2.resize(X_1,(210,210))
                            X_1  = X_1.reshape((1,210,210,3))
                            model_label1_seg = load_model(model_label1_seg_path)
                            P_1 = model_label1_seg.predict(X_1)
                            P_1 = np.argmax(P_1, axis=1)
                            if label1_img_seg_dict_reverse[P_1[0]]=='Door Tasks':
                                print("It is a door task image")
                                X_2 = cv2.imread(file_path)
                                X_2 = cv2.resize(X_2,(224,224))
                                X_2  = X_2.reshape((1,224,224,3))
                                model_door_seg = load_model(model_door_seg_path)
                                P_2 = model_door_seg.predict(X_2)
                                P_2 = np.argmax(P_2, axis=1)
                                if door_img_seg_dict_reverse[P_2[0]]=='Install Door Armor - 8955':
                                    print("It is a door task and Install Door Armor - 8955 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Install Door Armor - 8955'+'/'+file)
                                elif door_img_seg_dict_reverse[P_2[0]]=='Install Exterior Door with Frame - 8953':
                                    print("It is a door task and Install Exterior Door with Frame - 8953 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953'+'/'+file)
                                elif door_img_seg_dict_reverse[P_2[0]]=='Repair Door Frame - 9173':
                                    print("It is a door task and Repair Door Frame - 9173 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Repair Door Frame - 9173'+'/'+file)
                                elif door_img_seg_dict_reverse[P_2[0]]=='Repaired Replace Overhead Garage Door - 2215 3047':
                                    print("It is a door task and Repaired Replace Overhead Garage Door - 2215 3047 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047'+'/'+file)
                                elif door_img_seg_dict_reverse[P_2[0]]=='Replaced Door - 0514 8952':
                                    print("It is a door task and Replaced Door - 0514 8952 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Door Tasks'+'/'+'Replaced Door - 0514 8952'+'/'+file)
                            
                            elif label1_img_seg_dict_reverse[P_1[0]]=='Lawn Maintenance':
                                print("It is a lawn maintenance task image")
                                X_2 = cv2.imread(file_path)
                                X_2 = cv2.resize(X_2,(224,224))
                                X_2  = X_2.reshape((1,224,224,3))
                                model_grass_seg = load_model(model_grass_seg_path)
                                P_2 = model_grass_seg.predict(X_2)
                                P_2 = np.argmax(P_2, axis=1)
                                if grass_img_seg_dict_reverse[P_2[0]]=='Initial Grass Cut - 2501':
                                    print("It is a lawn maintenance task and Initial Grass Cut - 2501 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501'+'/'+file)
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Remove Vines - 2575':
                                    print("It is a lawn maintenance task and Remove Vines - 2575 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Remove Vines - 2575'+'/'+file)
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Trim Shrubs - 2606':
                                    print("It is a lawn maintenance task and Trim Shrubs - 2606 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Trim Shrubs - 2606'+'/'+file)
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Trim Trees - 9164':
                                    print("It is a lawn maintenance task and Trim Trees - 9164 image")
                                    shutil.copy2(file_path,segreagted_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Trim Trees - 9164'+'/'+file)
                            print("Image Segregation Task Completed")
                        except Exception as e:
                            print("It is not a Image File")
                    else:
                        pass
        else:
            print("No New Image Shipment Has Arrived.")
            pass

# Default parameters for the workflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG

with DAG(
        'image_segregation_dag', # Name of the DAG / workflow
        description='DAG to segregate the images into different task and subtask',
        default_args=default_args,
        catchup=False,
        schedule_interval=timedelta(minutes=10),  # Set the interval to run every 10 minutes
) as dag:
    # This operator does nothing. 
    start_task = EmptyOperator(
        task_id='start_task', # The name of the sub-task in the workflow.
        dag=dag # When using the "with Dag(...)" syntax you could leave this out
    )
    # With the PythonOperator you can run a python function.
    image_segregation_dag = PythonOperator(
        task_id='image_segregation_dag',
        python_callable=image_segregation_dag,
        op_args=["/home/python/Nexdeck/UnzipFolderShipment",
                      "unzipfile_status_table.csv",
                      "/home/python/Nexdeck/UnzipFolderShipment/PropertyPreservation/IngestedFiles",
                      "/home/python/Nexdeck/UnzipFolderShipment/PropertyPreservation",
                      "/home/python/Nexdeck/Models/Property-Preservation/models/pp_image_segregation_label1.h5",
                      "/home/python/Nexdeck/Models/Property-Preservation/models/pp_image_segregation_doortask.h5",
                      "/home/python/Nexdeck/Models/Property-Preservation/models/pp_image_segregation_lawnmaintenancetask.h5"],  # Replace with your actual path
        dag=dag,
    )

    

    # Define the order in which the tasks are supposed to run
    # You can also define paralell tasks by using an array 
    # I.e. task1 >> [task2a, task2b] >> task3
    start_task >> image_segregation_dag
