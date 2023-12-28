# Import Dependencies
import streamlit as st
import os, torch
import shutil
import zipfile
from pathlib import Path
# Import Data Science Libraries
import os, random, spacy
import pandas as pd
import pickle
import json
import imageio
import requests
from arcgis.learn import prepare_textdata
from arcgis.learn.text import TextClassifier, SequenceToSequence
# Import tensorflow related libraries
import tensorflow
from tensorflow.keras.preprocessing.image import img_to_array
from tensorflow.keras.preprocessing.image import load_img
from tensorflow.keras.models import load_model
import urllib.parse
from PIL import Image
import cv2
import numpy as np
# Import transformers
from transformers import BertTokenizer, BertForSequenceClassification, AdamW
# Import html template
from htmltemplate import css, bot_template, user_template
# Import Elasticsearch and its dependencies
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.helpers import bulk

# 2. Generate Folder Paths
zip_path = 'D:/Python Projects/Honda-Invoice/Airflow/NexDeck/ZipFolderAbsolutePath/'
unzip_destination_folder = 'D:/Python Projects/Honda-Invoice/Airflow/NexDeck/UnZipFolderAbsolutePath/TempFolder/'
property_preservation_ingested_folder = 'D:/Python Projects/Honda-Invoice/Airflow/NexDeck/UnZipFolderAbsolutePath/PropertyPreservation/IngestedFiles'
doc_indexing_ingested_folder = 'D:/Python Projects/Honda-Invoice/Airflow/NexDeck/UnZipFolderAbsolutePath/DocumentIndexingExtraction/IngestedFiles'
property_preservation_segregated_folder = 'D:/Python Projects/Honda-Invoice/Airflow/NexDeck/UnZipFolderAbsolutePath/PropertyPreservation/Segregated Images'
doc_indexing_ocrtext_folder = 'D:/Python Projects/Honda-Invoice/Airflow/NexDeck/UnZipFolderAbsolutePath/DocumentIndexingExtraction/OCR_Text'

# 3. List of Labels
# Level 1 Images
label1_img_seg_dict = {'Door Tasks': 0, 'Lawn Maintenance': 1}
label1_img_seg_dict_reverse = {0:'Door Tasks', 1:'Lawn Maintenance'}
# Door Segregation Images
door_img_seg_dict = {'Install Door Armor - 8955': 0, 'Install Exterior Door with Frame - 8953': 1, 'Repair Door Frame - 9173': 2, 'Repaired Replace Overhead Garage Door - 2215 3047': 3, 'Replaced Door - 0514 8952': 4}
door_img_seg_dict_reverse = {0:'Install Door Armor - 8955', 1:'Install Exterior Door with Frame - 8953', 2:'Repair Door Frame - 9173', 3:'Repaired Replace Overhead Garage Door - 2215 3047', 4:'Replaced Door - 0514 8952'}
# Lawn Maintenance Segregation Images
grass_img_seg_dict = {'Initial Grass Cut - 2501': 0, 'Remove Vines - 2575': 1, 'Trim Shrubs - 2606': 2, 'Trim Trees - 9164': 3}
grass_img_seg_dict_reverse = {0:'Initial Grass Cut - 2501', 1:'Remove Vines - 2575', 2:'Trim Shrubs - 2606', 3:'Trim Trees - 9164'}
# Before After During Segregation Labels
label_dict = {'After': 0,'Before': 1,'During': 2}
label_dict_reverse = {0:'After',1:'Before',2:'During'}

# List of Models
label2_image_segregation_lawnmaintenance = load_model('models/property_preservation/models/grass_image_classification.h5')
model_label1_seg = load_model('D:/Python Projects/Honda-Invoice/NexDeck/models/prop_preserv/pp_image_segregation_label1.h5')
model_door_seg = load_model('D:/Python Projects/Honda-Invoice/NexDeck/models/prop_preserv/pp_image_segregation_doortask.h5')
model_grass_seg = load_model('D:/Python Projects/Honda-Invoice/NexDeck/models/prop_preserv/pp_image_segregation_lawnmaintenancetask.h5')
# Load the model for doc-indexing
index_model = BertForSequenceClassification.from_pretrained("NexDeck/models/docindex")
index_tokenizer = BertTokenizer.from_pretrained("NexDeck/models/docindex")

# Text/Sequence Models
text_model = os.path.join('NexDeck', 'models', 'attom','country-classifier', "country-classifier.emd")

# Lets load the spaCy model for use now
with open("doc_extract.pickle", "rb") as f:
    new_ner = pickle.load(f)

# spaCy color visualization 
def color_gen():
    random_number = random.randint(0,16777215) #16777215 ~= 256x256x256(R,G,B)
    hex_number = format(random_number, 'x')
    hex_number = '#' + hex_number
    return hex_number

colors = {ent.upper():color_gen() for ent in new_ner.entities}
options = {"ents":[ent.upper() for ent in new_ner.entities], "colors":colors}

# 4. Connect to Elasticsearch
es = Elasticsearch(hosts = [{"host":"localhost", "port":9200, "scheme":"http"}])

# Streamlit UI
st.set_page_config(page_title="NexDeck", page_icon="https://nexval.com/wp-content/uploads/2021/06/NEX_WEB_LOGO_NEXVAL.png", layout="wide")

with st.sidebar:
    st.markdown(
    """
    <style>
        [data-testid=stSidebar] [data-testid=stImage]{
            text-align: center;
            display: block;
            margin-left: auto;
            margin-right: auto;
            width: 100%;
        }
    </style>
    """, unsafe_allow_html=True
    )
    st.image('https://nexval.com/wp-content/uploads/2021/06/NEX_WEB_LOGO_NEXVAL.png')
    st.markdown("<h1 style='text-align: center; color: white;'>NexDeck</h1>",
                unsafe_allow_html=True)
    
    selection = st.selectbox('Select a Project',['DocumentIndexingExtraction', 'PropertyPreservation', 'DataStandardization'])
    choice = st.radio("Features", [
                      'Ingestion', 'Visualization', 'Intelligence', 'Dashboard', 'Colab'])
    st.info("NexDeck is our powerful platform providing documents & images to uncover hidden actionable insights organizations need when working on their mortgage lifecycles.")


# Define the sidebar functionalities
if choice == 'Ingestion':
    st.title('File Ingestion')

    # Either user uploads zips from a desired location...
    uploaded_zip_folders = st.file_uploader('Choose zip files to be uploaded', type=['.zip', '.rar'], accept_multiple_files=True)

    # ...empty space for OR...
    middle_space = st.empty()
    middle_space.markdown('<p style="text-align:center; font-size:20px;">OR</p>', unsafe_allow_html=True)

    # ...or from entered location
    zip_path = st.text_input('Enter the directory-path for extraction:')

    # Check if the user uploaded a file or entered a location
    if uploaded_zip_folders:
        zip_folders_name = [file.name for file in uploaded_zip_folders] 
        zip_folder_paths = [zip_path+'/'+zip_folder_name for zip_folder_name in zip_folders_name]
        print("List of zip files to be moved:", zip_folder_paths)
        
        for zip_folder_path in zip_folder_paths:
            print("Attempting to move file:", zip_folder_path)
            
            # Print the list of files in the directory
            print("Files in the directory:", os.listdir(zip_folder_path))
            print("Attempting to move file:", zip_folder_path)
            destination_directory = os.path.dirname(zip_folder_path)
            if not os.path.exists(destination_directory):
                os.makedirs(destination_directory)
            
            with zipfile.ZipFile(zip_folder_path, 'r') as zip_ref:
                zip_ref.extractall(unzip_destination_folder)
                st.write('Successfully extracted ZipFile')

        folders_newly_arrived = os.listdir(unzip_destination_folder)
        print("folders_newly_arrived :", folders_newly_arrived)
        for folder in folders_newly_arrived:
            files = os.listdir(unzip_destination_folder+'/'+folder)
            print("files_newly_arrived :", files)
            shipment_name = folder
            print("shipment_name :", shipment_name)
            for file in files:
                file_path = unzip_destination_folder+'/'+folder+'/'+file
                if file_path.lower().endswith(('.pdf', '.tif')):
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
                        

                    # Check if the extracted text is not empty
                    if bool(content.strip()):
                        print("It Is A Text File")
                        message = st.empty()
                        message.text(f"{file} is a Text File")
                        if os.path.exists(doc_indexing_ingested_folder+'/'+shipment_name):
                            
                            shutil.copy(file_path, doc_indexing_ingested_folder+'/'+shipment_name)
                        else:
                            os.mkdir(doc_indexing_ingested_folder+'/'+shipment_name)
                            shutil.copy(file_path, doc_indexing_ingested_folder+'/'+shipment_name)
                    else:
                        print("It Is Not A Text File")

                else:
                    try:
                        image = imageio.imread(file_path)
                        print("It Is A Image File")
                        message = st.empty()
                        message.text(f"{file} is a Image File")
                        if os.path.exists(property_preservation_ingested_folder+'/'+shipment_name):
                            shutil.copy(file_path, property_preservation_ingested_folder+'/'+shipment_name)
                        else:
                            os.mkdir(property_preservation_ingested_folder+'/'+shipment_name)
                            shutil.copy(file_path, property_preservation_ingested_folder+'/'+shipment_name)
                    except Exception as e:
                        print("It Is A Not Image File")


    elif zip_path:
        zip_folder_names = [file for file in os.listdir(zip_path) if file.endswith('.zip')]
        if not zip_folder_names:
            print(f"No zip folders found in {zip_path}")
            st.write(f"No zip folders found in {zip_path}")
        else:
            zip_folder_paths = [os.path.join(zip_path, zip_folder_name) for zip_folder_name in zip_folder_names]
            for zip_folder_path in zip_folder_paths:
                destination_directory = os.path.dirname(zip_folder_path)
                if not os.path.exists(destination_directory):
                    os.makedirs(destination_directory)
                
                with zipfile.ZipFile(zip_folder_path, 'r') as zip_ref:
                    zip_ref.extractall(unzip_destination_folder)
                    st.info('Successfully extracted ZipFile')

                folders_newly_arrived = os.listdir(unzip_destination_folder)
                print("folders_newly_arrived :", folders_newly_arrived)
                for folder in folders_newly_arrived:
                    files = os.listdir(unzip_destination_folder+'/'+folder)
                    print("files_newly_arrived :", files)
                    shipment_name = folder
                    print("shipment_name :", shipment_name)
                    for file in files:
                        file_path = unzip_destination_folder+'/'+folder+'/'+file
                        if file_path.lower().endswith(('.pdf', '.tif')):
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
                                

                            # Check if the extracted text is not empty
                            if bool(content.strip()):
                                print("It Is A Text File")
                                message = st.empty()
                                message.text(f"{file} is a Text File")
                                if os.path.exists(doc_indexing_ingested_folder+'/'+shipment_name):
                                    
                                    shutil.copy(file_path, doc_indexing_ingested_folder+'/'+shipment_name)
                                else:
                                    os.mkdir(doc_indexing_ingested_folder+'/'+shipment_name)
                                    shutil.copy(file_path, doc_indexing_ingested_folder+'/'+shipment_name)
                            else:
                                print("It Is Not A Text File")

                        else:
                            try:
                                image = imageio.imread(file_path)
                                print("It Is A Image File")
                                message = st.empty()
                                message.text(f"{file} is a Image File")  
                                if os.path.exists(property_preservation_ingested_folder+'/'+shipment_name):
                                    shutil.copy(file_path, property_preservation_ingested_folder+'/'+shipment_name)
                                else:
                                    os.mkdir(property_preservation_ingested_folder+'/'+shipment_name)
                                    shutil.copy(file_path, property_preservation_ingested_folder+'/'+shipment_name)
                            except Exception as e:
                                print("It Is A Not Image File")


if choice == 'Visualization':
    st.title('Data Visualization')
    
    # Choices of the Visualization
    #sub_choice = st.selectbox('Choose Visualization Option',['DocumentIndexingExtraction', 'PropertyPreservation', 'DataStandardization'])

    if selection == 'DocumentIndexingExtraction':
        st.subheader('List of Documents')
        shipment_name = os.listdir(doc_indexing_ingested_folder)[0]
        print('Shipment Name:', shipment_name)
        doc_files = os.listdir(doc_indexing_ingested_folder+'/'+shipment_name)
        if st.button('Generate Document OCRs'):
            for doc_file in doc_files:
                doc_file_path = doc_indexing_ingested_folder+'/'+shipment_name+'/'+doc_file
                print("doc_file_path : ",doc_file_path)
                if doc_file_path.lower().endswith(('.pdf', '.tif')):
                    user_name = "NEXVAL123"
                    license_code = "0D06A12C-1134-484F-8F39-1D11F9B49281"
                    API_ENDPOINT = "http://147.135.15.63/restservices/processDocument?gettext=true&newline=1&getwords=true&ocroption=3&ocrtype=11&outputformat=txt"
                    with open(doc_file_path, 'rb') as doc_image_file:
                        print("doc_image_path : ",doc_image_file)
                        doc_image_data = doc_image_file.read()
                    try:
                        r = requests.post(url=API_ENDPOINT, data=doc_image_data, auth=(user_name, license_code), timeout=300)
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
                        
                        print(content)
                        if os.path.exists(doc_indexing_ocrtext_folder+'/'+shipment_name):
                            print(doc_indexing_ocrtext_folder+'/'+shipment_name+'/'+doc_file.split('.')[0]+'.txt')
                            with open(doc_indexing_ocrtext_folder+'/'+shipment_name+'/'+doc_file.split('.')[0]+'.txt', 'w', encoding='utf-8') as docfile:
                                docfile.write(content)
                        else:
                            os.mkdir(doc_indexing_ocrtext_folder+'/'+shipment_name)
                            print(doc_indexing_ocrtext_folder+'/'+shipment_name+'/'+doc_file.split('.')[0]+'.txt')
                            with open(doc_indexing_ocrtext_folder+'/'+shipment_name+'/'+doc_file.split('.')[0]+'.txt', 'w', encoding='utf-8') as docfile:
                                docfile.write(content)
                    except:
                        import time
                        time.sleep(6)

        directory_path = doc_indexing_ocrtext_folder + '/' + shipment_name
        # Get the list of files in the directory
        file_list = os.listdir(directory_path)
        selected_file = st.selectbox("Select a file to visualize entities", file_list)
        
        # Create a dropdown for DocType
        doc_types = ['D', 'F', 'I', 'M']

        if selected_file:
            st.write("You selected file:", selected_file)   
            model_folder = os.path.join('NexDeck','models','docextract')
            input_encoding = index_tokenizer(selected_file, truncation=True, padding=True, return_tensors='pt')

            # Make predictions
            index_model.eval()
            with torch.no_grad():
                output = index_model(input_encoding['input_ids'], attention_mask=input_encoding['attention_mask'])
                logits = output[0]
                pred_label = torch.argmax(logits, dim=1).item()

            # Map the predicted label to the original DocType
            selected_doc_type = doc_types[pred_label]

            # Automatically set the dropdown value
            st.selectbox("Select DocType", doc_types, index=doc_types.index(selected_doc_type))
            
            # Display the selected DocType
            st.write(f"Predicted DocType: {selected_doc_type}")

            nlp = spacy.load(model_folder)
            
            file_path = os.path.join(directory_path, selected_file)
            with open(file_path, 'r', encoding='utf-8') as file:
                file_text = file.read()
            
            doc = nlp(file_text)
            ent_html = spacy.displacy.render(doc, jupyter=False, style='ent', options=options)
            st.markdown(ent_html, unsafe_allow_html=True)

            # Extract Entities
            results = new_ner.extract_entities([file_text])
            df = pd.DataFrame(results)
            
            if df.empty:
                st.warning("No entities extracted for the selected file.")
            else:
                df["Doc_Id"] = [selected_file]  # Add the filename to the DataFrame
                
            # Extract Entities for all files in the list
            all_results = []
            for file_name in file_list:
                file_path = os.path.join(directory_path, file_name)
                with open(file_path, 'r', encoding='utf-8') as file:
                    file_text = file.read()
                
                results = new_ner.extract_entities([file_text])
                #all_results.append(pd.DataFrame(results, columns=df.columns))
            
                df_result = pd.DataFrame(results, columns=df.columns)
                
                # Add the actual filename to the 'Doc_Id' column
                df_result['Doc_Id'] = [file_name]

                # Add 'Job_Id' column and fill it with the folder name
                df_result['Job_Id'] = os.path.basename(directory_path)
                
                all_results.append(df_result)

            # Concatenate results for all files into a single DataFrame
            all_df = pd.concat(all_results, ignore_index=True)

            # Remove the 'Filename' column
            if 'Filename' in all_df.columns:
                all_df = all_df.drop(columns=['Filename'])

            # Reorder columns with 'Job_Id' and 'Doc_Id' as the first two columns
            all_df = all_df[['Job_Id', 'Doc_Id'] + [col for col in all_df.columns if col not in ['Job_Id', 'Doc_Id']]]

            # Save results to a csv
            all_df.to_csv('NexDeck/results/index_extract.csv', index=False)

            # Specify the directory path where you have write permissions
            json_directory = 'D:/Python Projects/Honda-Invoice/NexDeck/NoSQL'

            # Combine the directory path with the JSON filename
            json_path = os.path.join(json_directory, 'index_extract.json')

            # Save results to JSON
            all_df.to_json(json_path, orient='records', lines=True)


    if selection == 'PropertyPreservation':
        st.subheader('Segregated Images')
        shipment_name = os.listdir(property_preservation_ingested_folder)[0]
        image_files = os.listdir(property_preservation_ingested_folder+'/'+shipment_name)
        count = 1
        placeholder = st.empty()
        if st.button('Segregate Images'):
            for image_file in image_files:
                image_file_path = property_preservation_ingested_folder+'/'+shipment_name+'/'+image_file
                if image_file_path.lower().endswith(('.jpg', '.png','.jpeg','.JPEG','.PNG','.JPG','.Jpg','.Jpeg','.Png')):
                    X_1 = cv2.imread(image_file_path)
                    X_1 = cv2.resize(X_1,(210,210))
                    X_1  = X_1.reshape((1,210,210,3))
                    P_1 = model_label1_seg.predict(X_1)
                    P_1 = np.argmax(P_1, axis=1)
                    
                    
                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name):
                        if label1_img_seg_dict_reverse[P_1[0]]=='Door Tasks':
                            if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'):
                                X_2 = cv2.imread(image_file_path)
                                X_2 = cv2.resize(X_2,(224,224))
                                X_2  = X_2.reshape((1,224,224,3))
                                P_2 = model_door_seg.predict(X_2)
                                P_2 = np.argmax(P_2, axis=1)
                                if door_img_seg_dict_reverse[P_2[0]]=='Install Door Armor - 8955':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Door Armor - 8955'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Door Armor - 8955')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Door Armor - 8955')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Door Armor - 8955')
                                elif door_img_seg_dict_reverse[P_2[0]]=='Install Exterior Door with Frame - 8953':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953')
                                
                                elif door_img_seg_dict_reverse[P_2[0]]=='Repair Door Frame - 9173':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repair Door Frame - 9173'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repair Door Frame - 9173')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repair Door Frame - 9173')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repair Door Frame - 9173')
                                
                                elif door_img_seg_dict_reverse[P_2[0]]=='Repaired Replace Overhead Garage Door - 2215 3047':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047')
                                
                                elif door_img_seg_dict_reverse[P_2[0]]=='Replaced Door - 0514 8952':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Replaced Door - 0514 8952'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Replaced Door - 0514 8952')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Replaced Door - 0514 8952')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Replaced Door - 0514 8952')
                            else:
                                os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks')
                                X_2 = cv2.imread(image_file_path)
                                X_2 = cv2.resize(X_2,(224,224))
                                X_2  = X_2.reshape((1,224,224,3))
                                P_2 = model_door_seg.predict(X_2)
                                P_2 = np.argmax(P_2, axis=1)
                                if door_img_seg_dict_reverse[P_2[0]]=='Install Door Armor - 8955':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Door Armor - 8955'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Door Armor - 8955')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Door Armor - 8955')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Door Armor - 8955')
                                elif door_img_seg_dict_reverse[P_2[0]]=='Install Exterior Door with Frame - 8953':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953')
                                
                                elif door_img_seg_dict_reverse[P_2[0]]=='Repair Door Frame - 9173':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repair Door Frame - 9173'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repair Door Frame - 9173')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repair Door Frame - 9173')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repair Door Frame - 9173')
                                
                                elif door_img_seg_dict_reverse[P_2[0]]=='Repaired Replace Overhead Garage Door - 2215 3047':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047')
                                
                                elif door_img_seg_dict_reverse[P_2[0]]=='Replaced Door - 0514 8952':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Replaced Door - 0514 8952'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Replaced Door - 0514 8952')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Replaced Door - 0514 8952')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Replaced Door - 0514 8952')
                        
                        elif label1_img_seg_dict_reverse[P_1[0]]=='Lawn Maintenance':
                            if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'):
                                X_2 = cv2.imread(image_file_path)
                                X_2 = cv2.resize(X_2,(224,224))
                                X_2  = X_2.reshape((1,224,224,3))
                                P_2 = model_grass_seg.predict(X_2)
                                P_2 = np.argmax(P_2, axis=1)
                                if grass_img_seg_dict_reverse[P_2[0]]=='Initial Grass Cut - 2501':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501')
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Remove Vines - 2575':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Remove Vines - 2575'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Remove Vines - 2575')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Remove Vines - 2575')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Remove Vines - 2575')
                                
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Trim Shrubs - 2606':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Shrubs - 2606'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Shrubs - 2606')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Shrubs - 2606')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Shrubs - 2606')
                                
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Trim Trees - 9164':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Trees - 9164'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Trees - 9164')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Trees - 9164')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Trees - 9164')
                                
                                
                            else:
                                os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance')
                                X_2 = cv2.imread(image_file_path)
                                X_2 = cv2.resize(X_2,(224,224))
                                X_2  = X_2.reshape((1,224,224,3))
                                P_2 = model_grass_seg.predict(X_2)
                                P_2 = np.argmax(P_2, axis=1)
                                if grass_img_seg_dict_reverse[P_2[0]]=='Initial Grass Cut - 2501':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501')
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Remove Vines - 2575':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Remove Vines - 2575'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Remove Vines - 2575')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Remove Vines - 2575')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Remove Vines - 2575')
                                
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Trim Shrubs - 2606':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Shrubs - 2606'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Shrubs - 2606')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Shrubs - 2606')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Shrubs - 2606')
                                
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Trim Trees - 9164':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Trees - 9164'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Trees - 9164')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Trees - 9164')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Trees - 9164')
                    else: 
                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name)
                        if label1_img_seg_dict_reverse[P_1[0]]=='Door Tasks':
                            if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'):
                                X_2 = cv2.imread(image_file_path)
                                X_2 = cv2.resize(X_2,(224,224))
                                X_2  = X_2.reshape((1,224,224,3))
                                P_2 = model_door_seg.predict(X_2)
                                P_2 = np.argmax(P_2, axis=1)
                                if door_img_seg_dict_reverse[P_2[0]]=='Install Door Armor - 8955':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Door Armor - 8955'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Door Armor - 8955')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Door Armor - 8955')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Door Armor - 8955')
                                elif door_img_seg_dict_reverse[P_2[0]]=='Install Exterior Door with Frame - 8953':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953')
                                
                                elif door_img_seg_dict_reverse[P_2[0]]=='Repair Door Frame - 9173':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repair Door Frame - 9173'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repair Door Frame - 9173')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repair Door Frame - 9173')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repair Door Frame - 9173')
                                
                                elif door_img_seg_dict_reverse[P_2[0]]=='Repaired Replace Overhead Garage Door - 2215 3047':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047')
                                
                                elif door_img_seg_dict_reverse[P_2[0]]=='Replaced Door - 0514 8952':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Replaced Door - 0514 8952'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Replaced Door - 0514 8952')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Replaced Door - 0514 8952')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Replaced Door - 0514 8952')
                            else:
                                os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks')
                                X_2 = cv2.imread(image_file_path)
                                X_2 = cv2.resize(X_2,(224,224))
                                X_2  = X_2.reshape((1,224,224,3))
                                P_2 = model_door_seg.predict(X_2)
                                P_2 = np.argmax(P_2, axis=1)
                                if door_img_seg_dict_reverse[P_2[0]]=='Install Door Armor - 8955':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Door Armor - 8955'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Door Armor - 8955')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Door Armor - 8955')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Door Armor - 8955')
                                elif door_img_seg_dict_reverse[P_2[0]]=='Install Exterior Door with Frame - 8953':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Install Exterior Door with Frame - 8953')
                                
                                elif door_img_seg_dict_reverse[P_2[0]]=='Repair Door Frame - 9173':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repair Door Frame - 9173'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repair Door Frame - 9173')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repair Door Frame - 9173')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repair Door Frame - 9173')
                                
                                elif door_img_seg_dict_reverse[P_2[0]]=='Repaired Replace Overhead Garage Door - 2215 3047':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Repaired Replace Overhead Garage Door - 2215 3047')
                                
                                elif door_img_seg_dict_reverse[P_2[0]]=='Replaced Door - 0514 8952':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Replaced Door - 0514 8952'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Replaced Door - 0514 8952')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Replaced Door - 0514 8952')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Door Tasks'+'/'+'Replaced Door - 0514 8952')
                        
                        elif label1_img_seg_dict_reverse[P_1[0]]=='Lawn Maintenance':
                            if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'):
                                X_2 = cv2.imread(image_file_path)
                                X_2 = cv2.resize(X_2,(224,224))
                                X_2  = X_2.reshape((1,224,224,3))
                                P_2 = model_grass_seg.predict(X_2)
                                P_2 = np.argmax(P_2, axis=1)
                                if grass_img_seg_dict_reverse[P_2[0]]=='Initial Grass Cut - 2501':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501')
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Remove Vines - 2575':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Remove Vines - 2575'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Remove Vines - 2575')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Remove Vines - 2575')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Remove Vines - 2575')
                                
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Trim Shrubs - 2606':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Shrubs - 2606'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Shrubs - 2606')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Shrubs - 2606')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Shrubs - 2606')
                                
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Trim Trees - 9164':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Trees - 9164'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Trees - 9164')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Trees - 9164')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Trees - 9164')
                                
                                
                            else:
                                os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance')
                                X_2 = cv2.imread(image_file_path)
                                X_2 = cv2.resize(X_2,(224,224))
                                X_2  = X_2.reshape((1,224,224,3))
                                P_2 = model_grass_seg.predict(X_2)
                                P_2 = np.argmax(P_2, axis=1)
                                if grass_img_seg_dict_reverse[P_2[0]]=='Initial Grass Cut - 2501':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Initial Grass Cut - 2501')
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Remove Vines - 2575':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Remove Vines - 2575'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Remove Vines - 2575')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Remove Vines - 2575')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Remove Vines - 2575')
                                
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Trim Shrubs - 2606':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Shrubs - 2606'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Shrubs - 2606')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Shrubs - 2606')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Shrubs - 2606')
                                
                                elif grass_img_seg_dict_reverse[P_2[0]]=='Trim Trees - 9164':
                                    if os.path.exists(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Trees - 9164'):
                                        shutil.copy(image_file_path,property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Trees - 9164')
                                    else:
                                        os.mkdir(property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Trees - 9164')
                                        shutil.copy(image_file_path, property_preservation_segregated_folder+'/'+shipment_name+'/'+'Lawn Maintenance'+'/'+'Trim Trees - 9164')                    
                            
        # Images Path
        images_path = 'D:/Python Projects/Honda-Invoice/Airflow/NexDeck/UnZipFolderAbsolutePath/PropertyPreservation/Segregated Images' + '/' + shipment_name

        # Function to display images in a thumbnail view with a horizontal scroller
        def display_images_in_folder(images_path):
            # Get a list of all items in the main path (both files and folders)
            all_items = os.listdir(images_path)

            # Iterate through each item in the main path
            for item_name in all_items:
                item_path = os.path.join(images_path, item_name)

                # Check if the item is a directory
                if os.path.isdir(item_path):
                    with st.expander(f"{item_name}"):
                        # Get a list of subfolders in the current main folder
                        subfolders = [subfolder for subfolder in os.listdir(item_path) if os.path.isdir(os.path.join(item_path, subfolder))]
                        # Display subfolders as tabs
                        if subfolders:
                            selected_subfolder = st.selectbox("Select a subfolder", subfolders)
                        else:
                            st.warning("No subfolders found.")
                        # Display images from the selected subfolder
                        image_files = []
                        subfolder_path = os.path.join(item_path, str(selected_subfolder))
                        for root, _, files in os.walk(subfolder_path):
                            for file in files:
                                if file.lower().endswith(('.png', '.jpg', '.jpeg', '.gif')):
                                    image_files.append(os.path.join(root, file))

                        # Display images one by one using PIL
                        for image_file in image_files:
                            image_path = os.path.join(subfolder_path, image_file)
                            image = Image.open(image_path)
                            st.image(image, width=100, caption=image_file)

        # Call the function to display images inside folders
        display_images_in_folder(images_path)

    if selection == 'DataStandardization':
        st.subheader('Data Completion and Correction/Standardization')
        data_selection = st.selectbox('Features', ['DataCompletion', 'DataCorrection'])
        if data_selection == 'DataCompletion':
            emd_path = os.path.join('NexDeck', 'models', 'attom','country-classifier', "country-classifier.emd")
            model = TextClassifier.from_model(emd_path)
            # Create a text input and get the user's query
            query = st.text_area(""" """)
            # Check if the user has entered a query
            if query:
                # Assuming 'model' has a predict method
                single_result = model.predict(query)
                st.write(single_result)
            else:
                st.warning("Please enter a query.")

        if data_selection == 'DataCorrection':
            seq_emd_path = os.path.join('NexDeck', 'models', 'attom', 'seq2seq_bleu', 'seq2seq_bleu.emd')
            model = SequenceToSequence.from_model(seq_emd_path)
            # Create a text input and get the user's query
            query = st.text_area(""" """)
            # Check if the user has entered a query
            if query:
                # Assuming 'model' has a predict method
                single_result = model.predict(query, num_beams=6, max_length=50)
                st.info('Non-Standard → Standard , Error → Correction')
                st.write(single_result[0][1])
            else:
                st.warning("Please enter a query.")


if choice == 'Intelligence':
    st.title('Actionable Insights')
    
    # Choices of the Visualization
    #sub_choice = st.selectbox('Choose Extraction-Classification Option',['DocumentIndexingExtraction', 'PropertyPreservation'])

    if selection == 'DocumentIndexingExtraction':
        st.subheader('Extracted Entities')
        index_results = r'D:/Python Projects/Honda-Invoice/NexDeck/results/index_extract.csv'
        # Read the CSV file into a DataFrame
        df = pd.read_csv(index_results, index_col=0)
        # Display the DataFrame using st.write() or st.dataframe()
        st.dataframe(df)

    if selection == 'PropertyPreservation':
        st.subheader('Classified Images')
        tab1, tab2 = st.tabs(['Correct', 'Incorrect'])
        images_root_path = 'D:/Python Projects/Honda-Invoice/Airflow/NexDeck/UnZipFolderAbsolutePath/PropertyPreservation/'
        images_path = os.path.join(images_root_path, 'Classified Images','2111202342','Lawn Maintenance', 'Initial Grass Cut - 2501')        
        model = label2_image_segregation_lawnmaintenance

        df = pd.DataFrame({'Image_Number':[],'Actual_Label':[],'Predicted_Label':[],'Result':[]})

        image_number_list = []
        actual_label_list = []
        predicted_label_list = []
        result_list = []
        image_view = []
        correct_images = []
        incorrect_images = []
        correct_images_indices = []
        incorrect_images_indices = []

        count = 1
        correct_classification_count = 0
        mis_classification_count = 0

        placeholder = st.empty()

        for root, dirs, files in os.walk(images_path):
            for f in files:
                image_name = f
                image_static_path = os.path.join(images_path, image_name)
                # st.write(image_static_path)
                image_name = image_name[:-4]
                image_name = image_name.split('.')[0]
                # actual_label_name = image_name[1]

                if ('During'in image_name):
                    actual_label_name = 'DURING'
                elif ( 'during' in image_name):
                    actual_label_name = 'DURING'
                elif ('After'in image_name):
                    actual_label_name = 'AFTER'
                elif ('after' in image_name):
                    actual_label_name = 'AFTER'
                elif ('Before' in image_name):
                    actual_label_name = 'BEFORE'
                elif ('before' in image_name):
                    actual_label_name = 'BEFORE'

                # st.image(image, caption='Grass_Image')
                # if st.button('Process'):
                X = cv2.imread(image_static_path)
                X = X.reshape(1, 224, 224, 3)

                P = model.predict(X)
                P = np.argmax(P, axis=1)

                if actual_label_name.lower() == label_dict_reverse[P[0]].lower():
                    # st.info('Result : Correct')
                    result_list.append('Correct')
                    correct_classification_count = correct_classification_count + 1
                    # correct_indices.append(f)
                else:
                    # st.info('Result : Incorrect')
                    result_list.append('Incorrect')
                    mis_classification_count = mis_classification_count + 1
                    # incorrect_indices.append(f)

                image_number_list.append('Grass_Image_{}'.format(count))
                # actual_label_list.append('{}_Maintainance'.format(actual_label_name.upper()))
                actual_label_list.append('{}'.format(actual_label_name.upper()))

                # predicted_label_list.append('{}_Maintainance'.format(label_dict_reverse[P[0]].upper()))
                predicted_label_list.append('{}'.format(label_dict_reverse[P[0]].upper()))

                if actual_label_name.lower() == label_dict_reverse[P[0]].lower():
                    # correct_images_indices.append(uploaded_files.index(f))
                    correct_images.append(image_static_path)

                elif actual_label_name != label_dict_reverse[P[0]].lower():
                    # incorrect_images_indices.append(uploaded_files.index(f))
                    incorrect_images.append(image_static_path)


        with tab1:
            # Display images one by one using PIL
            def display_images_in_folder(folder_name, images_list):
                with st.expander(f'{folder_name}'):
                    for image_path in images_list:
                        image = Image.open(image_path)
                        st.image(image, width=100, caption=os.path.basename(image_path))

            # Display incorrect images in tab2
            display_images_in_folder('Lawn Maintenance', correct_images)

        with tab2:
            # Function to display images in a thumbnail view with a horizontal scroller
            def display_images_in_folder(folder_name, images_list):
                with st.expander(f'{folder_name}'):
                    for image_path in images_list:
                        image = Image.open(image_path)
                        st.image(image, width=100, caption=os.path.basename(image_path))

            # Display incorrect images in tab2
            display_images_in_folder('Lawn Maintenance', incorrect_images)

        st.subheader('Summary Result')
        df['Image_Number'] = image_number_list
        df['Actual_Label'] = actual_label_list
        df['Predicted_Label'] = predicted_label_list
        df['Result'] = result_list
        #df['Index'] = [f.name[0:4] for f in images_path]
        #df.set_index('Index',inplace=True)
        st.dataframe(df,1400, 500)
        # Define the additional columns
        # Define the additional columns
        df['Sub-Task'] = [os.path.basename(os.path.dirname(image_path)) for image_path in correct_images + incorrect_images]
        print(df['Sub-Task'])
        df['Img_Id'] = [os.path.splitext(os.path.basename(image_path))[0] for image_path in correct_images + incorrect_images]
        print(df['Img_Id'])
        # Extract 'WorkOrder_Id', 'Task', and 'Sub-Task' from the image path
        df['Task'] = [os.path.basename(os.path.dirname(os.path.dirname(image_path))) for image_path in correct_images + incorrect_images]
        print(df['Task'])
        df['WorkOrder_Id'] = [os.path.basename(os.path.dirname(os.path.dirname(os.path.dirname(image_path)))) for image_path in correct_images + incorrect_images]
        print(df['WorkOrder_Id'])
        # Print the columns before attempting to reorder them
        print("Columns before reordering:", df.columns)

        # Reorder columns with the additional columns
        df = df[['WorkOrder_Id', 'Img_Id', 'Task', 'Sub-Task', 'Actual_Label', 'Predicted_Label', 'Result']]

        # Print the columns after reordering them
        print("Columns after reordering:", df.columns)

        # Save results to CSV
        csv_path = 'NexDeck/results/property_preserv.csv'
        df.to_csv(csv_path, index=False)

        # Specify the directory path where you have write permissions
        json_directory = 'D:/Python Projects/Honda-Invoice/NexDeck/NoSQL'

        # Combine the directory path with the JSON filename
        json_path = os.path.join(json_directory, 'prop_preserv.json')

        # Save results to JSON
        df.to_json(json_path, orient='records', lines=True)

    if selection == 'DataStandardization':
        data_choices = st.selectbox("Select a Feature",['DataCompletion', 'DataCorrection'])
        if data_choices == 'DataCompletion':
            folder_path = 'D:/Python Projects/Honda-Invoice/Airflow/NexDeck/UnZipFolderAbsolutePath/DataStandardization/DataCompletion/IngestedFiles/2111202342'
            #st.write('Code running for now..')
            # List all files in the directory
            file_list = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, file))]
            # Extract file names from full paths
            file_names = [os.path.basename(file) for file in file_list]
            # Dropdown to select a file
            selected_file_name = st.selectbox("Select a file", file_names)
            # Dropdown to select a file
            selected_file = os.path.join(folder_path, selected_file_name)
            # Read the selected file into a DataFrame
            df = pd.read_csv(selected_file)
            #st.write(df)
            emd_path = os.path.join('NexDeck', 'models', 'attom', 'country-classifier', 'country-classifier.emd')
            model = TextClassifier.from_model(emd_path)
            # State variable to track whether the model has been run for the current file
            model_run_for_current_file = False
            if st.button('TextClassification'):
                # Get addresses from the CSV File
                text_list = df['Address'].values
                # Make predictions for 'CountryCode' and 'Confidence'
                new_predictions = model.predict(text_list)
                # Create a DataFrame with the results
                new_results_df = pd.DataFrame(new_predictions, columns=["Address", "CountryCode", "Confidence"])
                # Save the new DataFrame to a CSV file
                new_results_df.to_csv("D:/Python Projects/Honda-Invoice/NexDeck/results/data_completed.csv", index=False)  # Replace with your desired output file path
                # Set the state variable to True
                model_run_for_current_file = True
                # Display the results
                st.subheader("Predicted CountryCode and Confidence for New Addresses")
                st.write(new_results_df)
            else:
                file_path = "D:/Python Projects/Honda-Invoice/NexDeck/results/data_completed.csv"
                result_df = pd.read_csv(file_path)
                st.write(result_df)

        if data_choices == 'DataCorrection':
            folder_path = 'D:/Python Projects/Honda-Invoice/Airflow/NexDeck/UnZipFolderAbsolutePath/DataStandardization/DataCorrection/IngestedFiles/2111202342'
            #st.write(df)
            emd_path = os.path.join('NexDeck', 'models', 'attom', 'seq2seq_bleu', 'seq2seq_bleu.emd')
            model = SequenceToSequence.from_model(emd_path)
            # List all files in the directory
            file_list = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, file))]
            # Extract file names from full paths
            file_names = [os.path.basename(file) for file in file_list]
            # Dropdown to select a file
            selected_file_name = st.selectbox("Select a file", file_names)
            # Dropdown to select a file
            selected_file = os.path.join(folder_path, selected_file_name)
            # Read the selected file into a DataFrame
            df = pd.read_csv(selected_file)
            # Assuming 'non-std-address' is the column containing addresses
            addresses_column = 'non-std-address'
            # Display the results
            st.subheader("Address Standardization and Correction")
            # Run the ML code
            if st.button('Run Seq2Seq'):
                # Perform predictions on the 'non-std-address' column
                if addresses_column in df.columns:
                    # Convert the column to a list before making predictions
                    address_list = df[addresses_column].astype(str).tolist()
                    # Make predictions for 'CountryCode' and 'Confidence'
                    new_predictions = model.predict(address_list, num_beams=6, max_length=50)
                    # Unpack the tuples in new_predictions
                    std_addresses = [pred[1] for pred in new_predictions]
                    # Create a DataFrame with the results
                    new_results_df = pd.DataFrame({ "std-address": std_addresses })
                    # Concatenate the original DataFrame with the new results
                    final_df = pd.concat([df, new_results_df], axis=1)
                    # Save the dataframe
                    final_df.to_csv("D:/Python Projects/Honda-Invoice/NexDeck/results/data_standardized_corrected.csv", index=False)
                    # Display the final DataFrame
                    st.write("DataFrame with Predictions:")
                    st.write(final_df)
            else:
                seq_file_path = "D:/Python Projects/Honda-Invoice/NexDeck/results/data_standardized_corrected.csv"
                seq_results_df = pd.read_csv(seq_file_path)
                st.dataframe(seq_results_df)

if choice == 'Dashboard':
    st.title('Day-to-Day Analytics')
    
    # Choices of the Visualization
    #sub_choice = st.selectbox('Choose Analytics Dashboard',['DocumentIndexingExtraction', 'PropertyPreservation'])

    if selection == 'DocumentIndexingExtraction':
        st.subheader('DocuChief')

        # Set the height and width attributes of the iframe as percentages
        iframe_html = (
            '<iframe src="http://localhost:5601/app/dashboards#/view/f0b039e0-8aec-11ee-8667-3f33f0642b98?embed=true&_g=(refreshInterval:(pause:!t,value:60000),time:(from:now-15m,to:now))&_a=()" '
            'style="height: 120svh; width: 70vw;"></iframe>'
        )

        # Embed the Kibana dashboard
        st.markdown(iframe_html, unsafe_allow_html=True)

    if selection == 'PropertyPreservation':
        st.subheader('NexImage')

        # Set the height and width attributes of the iframe as percentages
        iframe_html = (
            '<iframe src="http://localhost:5601/app/dashboards#/view/c3952920-8b6e-11ee-8667-3f33f0642b98?embed=true&_g=(refreshInterval:(pause:!t,value:60000),time:(from:now-15m,to:now))&_a=()" '
            'style="height: 85svh; width: 70vw;"></iframe>'
        )

        # Embed the Kibana dashboard
        st.markdown(iframe_html, unsafe_allow_html=True)

if choice == 'Colab':
    st.subheader('Let\'s Chat with your mortgage documents!')

    # Choices of the Visualization
    #sub_choice = st.selectbox('Choose Chatting Option',['DocumentIndexingExtraction', 'PropertyPreservation'])
    
    st.write(css, unsafe_allow_html=True)

    user_question = st.text_input('Ask a question about the documents: 👨🏻‍💻')

    def extract_field_from_doc_question(question):
        # Define a mapping of common keywords to fields
        keyword_to_field = {
            'buyer': 'Buyer',
            'seller': 'Seller',
            'date': 'Recording Date',
            'jobs': 'Job_Id',
            'docs': 'Doc_Id',
            'documents': 'Doc_Id',
            'text': 'TEXT',
            'number': 'Recording Number',
            'type': 'Doc Type',
            'record date': 'Recording Date',
            'job': 'Job_Id',
            'document': 'Doc_Id',
            'recording': 'Recording Number',
            'doc type': 'Doc Type',
            'recording date': 'Recording Date',
            'document type': 'Doc Type',
            'buyer name': 'Buyer',
            'seller name': 'Seller',
            'purchase date': 'Recording Date',
            'work order': 'Job_Id',
            'file': 'Doc_Id',
            'file type': 'Doc Type',
            'content': 'TEXT',
            'transaction date': 'Recording Date',
            'seller info': 'Seller',
            'buyer info': 'Buyer',
            'transaction type': 'Doc Type',
            'text content': 'TEXT',
            'identification': 'Recording Number',
            'file number': 'Doc_Id',
            'text document': 'TEXT',
            'seller document': 'Seller',
            'buyer document': 'Buyer',
            'document content': 'TEXT',
            'record number': 'Recording Number',
            'recorded date': 'Recording Date',
            'work identifier': 'Job_Id',
            'file identifier': 'Doc_Id',
            'document identifier': 'Doc_Id',
            'file content': 'TEXT',
            'transaction number': 'Recording Number',
            'transaction type': 'Doc Type',
            'document text': 'TEXT',
            # Add more mappings as needed
        }


        # Convert the question to lowercase for case-insensitive matching
        question_lower = question.lower()

        # Check if any keyword matches the question
        for keyword, field in keyword_to_field.items():
            if keyword in question_lower:
                return field

        # If no matching keyword is found, return None
        return None

    if sub_choice == 'DocumentIndexingExtraction':
        if st.button('Submit Question'):
            with st.spinner('Processing'):
                # Extract the field mentioned in the user's question (e.g., 'Buyer', 'Seller', etc.)
                field_mentioned = extract_field_from_doc_question(user_question)

                # Check if a valid field is mentioned
                if field_mentioned:
                    # Perform an aggregation query for unique values of the extracted field
                    # Replace 'index_extract' with the actual index you want to query
                    result_aggregation = es.search(index='index_extract', body={
                        "size": 0,
                        "aggs": {
                            "unique_values": {
                                "terms": {
                                    "field": f"{field_mentioned}.keyword",
                                    "size": 20  # Adjust the size based on your requirements
                                }
                            }
                        }
                    })

                    # Extract the unique values from the aggregation result
                    unique_values = [bucket['key'] for bucket in result_aggregation['aggregations']['unique_values']['buckets']]


                    # Display the user question in user_template
                    st.write(user_template.replace("{{MSG}}", f"{user_question}"), unsafe_allow_html=True)

                    # Display the unique values as a response in bot_template
                    if unique_values:
                        # Format the unique values with a newline after "Thank you for your patience!"
                        formatted_values = '\n'.join(map(str, unique_values))

                        # Display the response without brackets
                        st.write(bot_template.replace("{{MSG}}", f"Thank you for your patience!\nThe {field_mentioned} are:\n{formatted_values}"), unsafe_allow_html=True)
                    else:
                        # Display the response without brackets
                        st.write(bot_template.replace("{{MSG}}", f"Sorry! No {field_mentioned} values found. Please retry with a different question."), unsafe_allow_html=True)

    def extract_field_from_img_question(question):
        # Define a mapping of common keywords to fields
        keyword_to_field = {
            'actual label': 'Actual_Label',
            'predicted label': 'Predicted_Label',
            'result': 'Result',
            'sub-task': 'Sub-Task',
            'task': 'Task',
            'work order': 'WorkOrder_Id',
            'label actual': 'Actual_Label',
            'label predicted': 'Predicted_Label',
            'outcome': 'Result',
            'subtask': 'Sub-Task',
            'task identifier': 'Task',
            'order of work': 'WorkOrder_Id',
            'actual outcome': 'Actual_Label',
            'anticipated label': 'Predicted_Label',
            'task result': 'Result',
            'subordinate task': 'Sub-Task',
            'job': 'Task',
            'work identifier': 'WorkOrder_Id',
            'expected outcome': 'Predicted_Label',
            'assigned task': 'Task',
            'order for work': 'WorkOrder_Id',
            'outcome label': 'Result',
            'subsequent task': 'Sub-Task',
            'work task': 'Task',
            'work assignment': 'WorkOrder_Id',
            'label from actual': 'Actual_Label',
            'predicted outcome': 'Predicted_Label',
            'resultant effect': 'Result',
            'underlying task': 'Sub-Task',
            'assigned job': 'Task',
            'job order': 'WorkOrder_Id',
            'label as per actual': 'Actual_Label',
            'prediction label': 'Predicted_Label',
            'end result': 'Result',
            'subordinate assignment': 'Sub-Task',
            'task to perform': 'Task',
            'work identification': 'WorkOrder_Id',
            'actual label value': 'Actual_Label',
            'predicted value label': 'Predicted_Label',
            'final result': 'Result',
            'task in question': 'Task',
            'assigned work order': 'WorkOrder_Id',
            'actual value of label': 'Actual_Label',
            'label predicted by model': 'Predicted_Label',
            'resulting outcome': 'Result',
            'associated sub-task': 'Sub-Task',
            'task at hand': 'Task',
            'identification of work order': 'WorkOrder_Id',
            'label from observation': 'Actual_Label',
            'model prediction label': 'Predicted_Label',
            'outcome determination': 'Result',
            'subordinate job': 'Sub-Task',
            'task assignment': 'Task',
            'identifiable work order': 'WorkOrder_Id',
            'label derived from actual': 'Actual_Label',
            'predicted label value': 'Predicted_Label',
            'result achieved': 'Result',
            'assigned sub-task': 'Sub-Task',
            'assigned task order': 'Task',
            'unique work order identifier': 'WorkOrder_Id',
            # Add more variations as needed
        }


        # Convert the question to lowercase for case-insensitive matching
        question_lower = question.lower()

        # Check if any keyword matches the question
        for keyword, field in keyword_to_field.items():
            if keyword in question_lower:
                return field

        # If no matching keyword is found, return None
        return None

    if sub_choice == 'PropertyPreservation':
        if st.button('Submit Question'):
            with st.spinner('Processing'):
                # Extract the field mentioned in the user's question (e.g., 'Buyer', 'Seller', etc.)
                field_mentioned = extract_field_from_img_question(user_question)

                # Check if a valid field is mentioned
                if field_mentioned:
                    # Perform an aggregation query for unique values of the extracted field
                    # Replace 'index_extract' with the actual index you want to query
                    result_aggregation = es.search(index='prop_preserv', body={
                        "size": 0,
                        "aggs": {
                            "unique_values": {
                                "terms": {
                                    "field": f"{field_mentioned}.keyword",
                                    "size": 20  # Adjust the size based on your requirements
                                }
                            }
                        }
                    })

                    # Extract the unique values from the aggregation result
                    unique_values = [bucket['key'] for bucket in result_aggregation['aggregations']['unique_values']['buckets']]


                    # Display the user question in user_template
                    st.write(user_template.replace("{{MSG}}", f"{user_question}"), unsafe_allow_html=True)

                    # Display the unique values as a response in bot_template
                    if unique_values:
                        # Format the unique values with a newline after "Thank you for your patience!"
                        formatted_values = '\n'.join(map(str, unique_values))

                        # Display the response without brackets
                        st.write(bot_template.replace("{{MSG}}", f"Thank you for your patience!\nThe {field_mentioned} are:\n{formatted_values}"), unsafe_allow_html=True)
                    else:
                        # Display the response without brackets
                        st.write(bot_template.replace("{{MSG}}", f"Sorry! No {field_mentioned} values found. Please retry with a different question."), unsafe_allow_html=True)


