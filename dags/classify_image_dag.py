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

def classify_image_dag(unzip_csv_file_path,unzip_csv_file_name,image_file_stored_path,classified_image_file_destination_path,model_grass_phase_path):
    grass_phase_label_dict = {'After':0,'Before':1,'During':2}
    grass_phase_label_dict_reverse = {0:'After',1:'Before',2:'During'}
    if os.path.exists(classified_image_file_destination_path+'/'+"ClassfiedImageFiles"):
        classified_image_file_destination_path = classified_image_file_destination_path+'/'+"ClassfiedImageFiles"
        print("classified_image_file_destination_path : ",classified_image_file_destination_path)
        df_unzip = pd.read_csv(os.path.join(unzip_csv_file_path,unzip_csv_file_name))
        all_shipment_name_list = list(np.unique(df_unzip[df_unzip["FileType"]=="Property-Preservation"]["Shipment Name"].values))
        completed_classified_image_shipment_name_list = os.listdir(classified_image_file_destination_path)
        print("completed_classified_image_shipment_name_list : ",completed_classified_image_shipment_name_list)
        shipment_name_list = [item for item in all_shipment_name_list if item not in completed_classified_image_shipment_name_list]
        print("shipment_name_list : ",shipment_name_list)
        if len(shipment_name_list)>0:
            for shipment_name in shipment_name_list:
                print("image_file_stored_path :",image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501')
                files = os.listdir(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501')
                print("files : ",files)
                files_with_duplicates = os.listdir(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501')
                if len(files)>0:
                    error_currently_uploaded_list = []
                    for f1, f2 in itertools.combinations(files, 2):
                        # Alogorithm for finding Duplicate Image (Root Mean Squared Error Distance)
                        image_information_as_hash_data_1 = imagehash.average_hash(Image.open(os.path.join(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501', f1)))
                        image_information_as_hash_data_2 = imagehash.average_hash(Image.open(os.path.join(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501', f2)))
                        error = image_information_as_hash_data_1-image_information_as_hash_data_2
                        error_currently_uploaded_list.append(error)
                    
                    
                    duplicate_images = []
                    for f1, f2 in itertools.combinations(files, 2):
                        image_information_as_hash_data_1 = imagehash.average_hash(Image.open(os.path.join(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501', f1)))
                        image_information_as_hash_data_2 = imagehash.average_hash(Image.open(os.path.join(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501', f2)))
                        error = image_information_as_hash_data_1-image_information_as_hash_data_2
                        if (error < 11):
                            if (f1 not in duplicate_images) & (f2 not in duplicate_images):
                                duplicate_images.append(f1)
                                duplicate_images.append(f2)
                                if f2 in files:
                                    files.remove(f2)
                                elif f2 not in files:
                                    pass
                            elif (f1 not in duplicate_images) & (f2 in duplicate_images):
                                duplicate_images.append(f1)
                                if f1 in files:
                                    files.remove(f1)
                                elif f1 not in files:
                                    pass
                            elif (f1 in duplicate_images) & (f2 not in duplicate_images):
                                duplicate_images.append(f2)
                                if f2 in files:
                                    files.remove(f2)
                                elif f2 not in files:
                                    pass
                            elif (f1 in duplicate_images) & (f2 in duplicate_images):
                                pass
                            
                        else:
                            pass
                    
                    duplicate_images_result = []
                    
                    for image in files_with_duplicates:
                        if image in duplicate_images:
                            duplicate_images_result.append({'file_name': image, 'is_duplicate': 'Duplicate'})
                        
                        elif image not in duplicate_images:
                            duplicate_images_result.append({'file_name': image, 'is_duplicate': 'NotDuplicate'})
                    print("duplicate_images_result :", duplicate_images_result)
                    duplicate_images_collection = [item for item in files_with_duplicates if item not in files]
                    duplicate_images_path_collection = [image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501'+'/'+item for item in files_with_duplicates if item not in files]

                    for item in duplicate_images_collection:
                        if os.path.exists( classified_image_file_destination_path + '/' + 'Duplicate'):
                            shutil.copy2(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501' + '/' + item,classified_image_file_destination_path + '/' + 'Duplicate'+'/'+item)
                    
                        else:
                            os.mkdir(classified_image_file_destination_path + '/' + 'Duplicate')
                            shutil.copy2(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501' + '/' + item,classified_image_file_destination_path + '/' + 'Duplicate'+'/'+item)
                    else:
                        pass

                    final_result = []
                    for file in files:
                        file_path = image_file_stored_path+'/'+file
                        if file_path.lower().endswith(('.jpg', '.png','.jpeg','.JPEG','.PNG','.JPG','.Jpg','.Jpeg','.Png')):
                            if os.path.exists(classified_image_file_destination_path + '/' + shipment_name):
                                pass
                            else:
                                os.mkdir(classified_image_file_destination_path + '/' + shipment_name)

                            
                            if os.path.exists(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'):
                                pass
                            else:
                                os.mkdir(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance')
                            
                            ###
                            
                            
                            if os.path.exists(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501'):
                                pass
                            else:
                                os.mkdir(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501')
                            
                            image_name = file
                            if ('During'in image_name):
                                actual_label_name = 'During'
                            elif ( 'DURING' in image_name):
                                actual_label_name = 'During' 
                            elif ('during' in image_name):
                                actual_label_name = 'During'
                            elif ('After'in image_name): 
                                actual_label_name = 'After'
                            elif ('AFTER' in image_name):
                                actual_label_name = 'After'
                            elif ('after' in image_name):
                                actual_label_name = 'After'
                            elif ('Before' in image_name):
                                actual_label_name = 'Before'
                            elif ('BEFORE' in image_name):
                                actual_label_name = 'Before'
                            elif ('before' in image_name):
                                actual_label_name = 'Before'

                            X = cv2.imread(os.path.join(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501', file))
                            X = cv2.resize(X, (224,224), interpolation = cv2.INTER_AREA)
                            X = X.reshape(224, 224, 3)
                            X = X.reshape(1, 224, 224, 3)
                            model_grass_phase = load_model(model_grass_phase_path)
                            P = model_grass_phase.predict(X)
                            P = np.argmax(P, axis=1)
                            
                            predicted_label = P[0]
                            
                            # Convert label index to label name
                            # label_dict_reverse = {0: 'After', 1: 'Before', 2: 'During'}
                            predicted_label_name = grass_phase_label_dict_reverse[predicted_label]


                            if predicted_label_name=='Before':
                                if os.path.exists(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'Before'):
                                    shutil.copy2(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501' + '/' + image_name,classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'Before'+'/'+image_name)
                                else:
                                    os.mkdir(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'Before')
                                    shutil.copy2(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501' + '/' + image_name,classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'Before'+'/'+image_name)
                                
                                

                            elif predicted_label_name=='After':
                                if os.path.exists(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'After'):
                                    shutil.copy2(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501' + '/' + image_name,classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'After'+'/'+image_name)
                                else:
                                    os.mkdir(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'After')
                                    shutil.copy2(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501' + '/' + image_name,classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'After'+'/'+image_name)

                                

                            elif predicted_label_name=='During':
                                if os.path.exists(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'During'):
                                    shutil.copy2(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501' + '/' + image_name,classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'During'+'/'+image_name)
                                else:
                                    os.mkdir(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'During')
                                    shutil.copy2(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501' + '/' + image_name,classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'During'+'/'+image_name)
                                
                                

                            if actual_label_name.lower() == predicted_label_name.lower():
                                final_result.append({'file_name':file,'label_name_predicted':predicted_label_name.upper(),'label_name_actual':actual_label_name.upper(),'result':'Validated'})
                            else:
                                final_result.append({'file_name':file,'label_name_predicted':predicted_label_name.upper(),'label_name_actual':actual_label_name.upper(),'result':'Corrected'})


                        else:
                            pass
                    print("final result :", final_result)        
                else:
                    pass
        else:
            print("No New Grass Image Shipment Has Arrived.")
            pass
    
    else:
        os.mkdir(classified_image_file_destination_path+'/'+"ClassfiedImageFiles")
        classified_image_file_destination_path = classified_image_file_destination_path+'/'+"ClassfiedImageFiles"
        print("classified_image_file_destination_path : ",classified_image_file_destination_path)
        df_unzip = pd.read_csv(os.path.join(unzip_csv_file_path,unzip_csv_file_name))
        all_shipment_name_list = list(np.unique(df_unzip[df_unzip["FileType"]=="Property-Preservation"]["Shipment Name"].values))
        shipment_name_list = [item for item in all_shipment_name_list ]
        print("shipment_name_list : ",shipment_name_list)
        
        if len(shipment_name_list)>0:
            for shipment_name in shipment_name_list:
                print("image_file_stored_path : ",image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501')
                files = os.listdir(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501')
                print("files : ",files)
                files_with_duplicates = os.listdir(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501')
                if len(files)>0:
                    error_currently_uploaded_list = []
                    for f1, f2 in itertools.combinations(files, 2):
                        # Alogorithm for finding Duplicate Image (Root Mean Squared Error Distance)
                        image_information_as_hash_data_1 = imagehash.average_hash(Image.open(os.path.join(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501', f1)))
                        image_information_as_hash_data_2 = imagehash.average_hash(Image.open(os.path.join(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501', f2)))
                        error = image_information_as_hash_data_1-image_information_as_hash_data_2
                        error_currently_uploaded_list.append(error)
                    
                    
                    duplicate_images = []
                    for f1, f2 in itertools.combinations(files, 2):
                        image_information_as_hash_data_1 = imagehash.average_hash(Image.open(os.path.join(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501', f1)))
                        image_information_as_hash_data_2 = imagehash.average_hash(Image.open(os.path.join(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501', f2)))
                        error = image_information_as_hash_data_1-image_information_as_hash_data_2
                        if (error < 11):
                            if (f1 not in duplicate_images) & (f2 not in duplicate_images):
                                duplicate_images.append(f1)
                                duplicate_images.append(f2)
                                if f2 in files:
                                    files.remove(f2)
                                elif f2 not in files:
                                    pass
                            elif (f1 not in duplicate_images) & (f2 in duplicate_images):
                                duplicate_images.append(f1)
                                if f1 in files:
                                    files.remove(f1)
                                elif f1 not in files:
                                    pass
                            elif (f1 in duplicate_images) & (f2 not in duplicate_images):
                                duplicate_images.append(f2)
                                if f2 in files:
                                    files.remove(f2)
                                elif f2 not in files:
                                    pass
                            elif (f1 in duplicate_images) & (f2 in duplicate_images):
                                pass
                            
                        else:
                            pass
                    
                    duplicate_images_result = []
                    
                    for image in files_with_duplicates:
                        if image in duplicate_images:
                            duplicate_images_result.append({'file_name': image, 'is_duplicate': 'Duplicate'})
                        
                        elif image not in duplicate_images:
                            duplicate_images_result.append({'file_name': image, 'is_duplicate': 'NotDuplicate'})
                    print("duplicate_images_result :", duplicate_images_result)
                    duplicate_images_collection = [item for item in files_with_duplicates if item not in files]
                    duplicate_images_path_collection = [image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501'+'/'+item for item in files_with_duplicates if item not in files]

                    for item in duplicate_images_collection:
                        if os.path.exists( classified_image_file_destination_path + '/' + 'Duplicate'):
                            shutil.copy2(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501' + '/' + item,classified_image_file_destination_path + '/' + 'Duplicate'+'/'+item)
                    
                        else:
                            os.mkdir(classified_image_file_destination_path + '/' + 'Duplicate')
                            shutil.copy2(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501' + '/' + item,classified_image_file_destination_path + '/' + 'Duplicate'+'/'+item)
                    else:
                        pass

                    final_result = []
                    for file in files:
                        file_path = image_file_stored_path+'/'+file
                        if file_path.lower().endswith(('.jpg', '.png','.jpeg','.JPEG','.PNG','.JPG','.Jpg','.Jpeg','.Png')): 
                            if os.path.exists(classified_image_file_destination_path + '/' + shipment_name):
                                pass
                            else:
                                os.mkdir(classified_image_file_destination_path + '/' + shipment_name)

                            
                            if os.path.exists(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'):
                                pass
                            else:
                                os.mkdir(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance')
                            
                            ###
                            
                            
                            if os.path.exists(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501'):
                                pass
                            else:
                                os.mkdir(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501')
                            
                            image_name = file
                            if ('During'in image_name):
                                actual_label_name = 'During'
                            elif ( 'DURING' in image_name):
                                actual_label_name = 'During' 
                            elif ('during' in image_name):
                                actual_label_name = 'During'
                            elif ('After'in image_name): 
                                actual_label_name = 'After'
                            elif ('AFTER' in image_name):
                                actual_label_name = 'After'
                            elif ('after' in image_name):
                                actual_label_name = 'After'
                            elif ('Before' in image_name):
                                actual_label_name = 'Before'
                            elif ('BEFORE' in image_name):
                                actual_label_name = 'Before'
                            elif ('before' in image_name):
                                actual_label_name = 'Before'

                            X = cv2.imread(os.path.join(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501', file))
                            X = cv2.resize(X, (224,224), interpolation = cv2.INTER_AREA)
                            X = X.reshape(224, 224, 3)
                            X = X.reshape(1, 224, 224, 3)
                            model_grass_phase = load_model(model_grass_phase_path)
                            P = model_grass_phase.predict(X)
                            P = np.argmax(P, axis=1)
                            
                            predicted_label = P[0]
                            
                            # Convert label index to label name
                            # label_dict_reverse = {0: 'After', 1: 'Before', 2: 'During'}
                            predicted_label_name = grass_phase_label_dict_reverse[predicted_label]


                            if predicted_label_name=='Before':
                                if os.path.exists(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'Before'):
                                    shutil.copy2(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501'+ '/' + image_name,classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'Before'+'/'+image_name)
                                else:
                                    os.mkdir(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'Before')
                                    shutil.copy2(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501' + '/' + image_name,classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'Before'+'/'+image_name)
                                
                                

                            elif predicted_label_name=='After':
                                if os.path.exists(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'After'):
                                    shutil.copy2(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501' + '/' + image_name,classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'After'+'/'+image_name)
                                else:
                                    os.mkdir(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'After')
                                    shutil.copy2(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501' + '/' + image_name,classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'After'+'/'+image_name)

                                

                            elif predicted_label_name=='During':
                                if os.path.exists(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'During'):
                                    shutil.copy2(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501' + '/' + image_name,classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'During'+'/'+image_name)
                                else:
                                    os.mkdir(classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'During')
                                    shutil.copy2(image_file_stored_path+'/'+shipment_name + '/' + 'Lawn Maintenance' + '/' + 'Initial Grass Cut - 2501' + '/' + image_name,classified_image_file_destination_path + '/' + shipment_name + '/' + 'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501' + '/' + 'During'+'/'+image_name)
                                
                                

                            if actual_label_name.lower() == predicted_label_name.lower():
                                final_result.append({'file_name':file,'label_name_predicted':predicted_label_name.upper(),'label_name_actual':actual_label_name.upper(),'result':'Validated'})
                            else:
                                final_result.append({'file_name':file,'label_name_predicted':predicted_label_name.upper(),'label_name_actual':actual_label_name.upper(),'result':'Corrected'})
  

                        else:
                            pass
                    print("final result :", final_result)        
                else:
                    pass


        else:
            print("No New Grass Image Shipment Has Arrived.")
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
        'classify_image_dag', # Name of the DAG / workflow
        description='DAG to classify those segregated the images into different phases (Before, After, During) .',
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
    classify_image_dag = PythonOperator(
        task_id='classify_image_dag',
        python_callable=classify_image_dag,
        op_args=["/home/python/Nexdeck/UnzipFolderShipment",
                    "unzipfile_status_table.csv",
                    "/home/python/Nexdeck/UnzipFolderShipment/PropertyPreservation/SegregatedImageFiles",
                    "/home/python/Nexdeck/UnzipFolderShipment/PropertyPreservation",
                    "/home/python/Nexdeck/Models/Property-Preservation/models/pp_image_segregation_phase_grass.h5"],  # Replace with your actual path
        dag=dag,
    )

    

    # Define the order in which the tasks are supposed to run
    # You can also define paralell tasks by using an array 
    # I.e. task1 >> [task2a, task2b] >> task3
    start_task >> classify_image_dag 
