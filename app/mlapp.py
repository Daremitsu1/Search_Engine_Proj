# Import Dependencies
import streamlit as st
import os
import shutil
import zipfile
from pathlib import Path
# Import Data Science Libraries
import os, random, spacy
import pandas as pd
import pickle
import json

# Lets load the model for use now
with open("ner_model.pickle", "rb") as f:
    new_ner = pickle.load(f)

def color_gen():
    random_number = random.randint(0,16777215) #16777215 ~= 256x256x256(R,G,B)
    hex_number = format(random_number, 'x')
    hex_number = '#' + hex_number
    return hex_number

colors = {ent.upper():color_gen() for ent in new_ner.entities}
options = {"ents":[ent.upper() for ent in new_ner.entities], "colors":colors}

# Define Zip File Location
def check_zip_file(path):
    zip_folders = [file for file in os.listdir(path) if file.endswith('.zip')]

    if not zip_folders:
        print(f"No zip folders found in {path}")
    else:
        name_zip_folder = zip_folders[0]
        zip_file_path = path+'/'+name_zip_folder

    return f"Zip folder found in {path} and Name of the Zip folder {name_zip_folder}"

# Define Unzip Methodology
def unzip_files(path, unzip_destination_folder):
    zip_folders = [file for file in os.listdir(path) if file.endswith('.zip')]

    if not zip_folders:
        print(f"No zip folders found in {path}")
    else:
        name_zip_folder = zip_folders[0]

    zip_file_path = Path(path+'/'+name_zip_folder)
    
    # Create UnzipFolderShipment if it doesn't exist
    destination_path = os.path.join(unzip_destination_folder, 'UnzipFolderShipment')
    os.makedirs(destination_path, exist_ok=True)

    # Create DocumentIndexingExtraction and PropertyPreservation folders
    document_extraction_folder = os.path.join(destination_path, 'DocumentIndexingExtraction')
    property_preservation_folder = os.path.join(destination_path, 'PropertyPreservation')

    os.makedirs(document_extraction_folder, exist_ok=True)
    os.makedirs(property_preservation_folder, exist_ok=True)

    # Perform the unzip
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(destination_path)

    # Move files to appropriate folders
    move_files_to_folders(destination_path, document_extraction_folder, property_preservation_folder)

    # Remove the original zip folder
    shutil.rmtree(zip_file_path)

    print(f"Successfully unzipped '{name_zip_folder}' and organized files.")

# Define moving files to folders
def move_files_to_folders(source_folder, document_extraction_folder, property_preservation_folder):
    files = os.listdir(source_folder)

    for file in files:
        file_path = os.path.join(source_folder, file)

        if file.lower().endswith(('.pdf', '.tif')):
            destination_folder = document_extraction_folder
        else:
            destination_folder = property_preservation_folder

        # Create the shipment-specific folder inside the destination
        shipment_folder = os.path.join(destination_folder, file.split('.')[0])
        os.makedirs(shipment_folder, exist_ok=True)

        # Construct the destination path
        destination_path = os.path.join(shipment_folder, file)

        # Copy the file to the shipment-specific folder
        shutil.copy(file_path, destination_path)

        # Remove the original file
        os.remove(file_path)

    print("Successfully organized files into DocumentIndexingExtraction and PropertyPreservation folders.")


# Streamlit UI
st.set_page_config(page_title="NexDeck", page_icon="https://nexval.com/wp-content/uploads/2021/06/NEX_WEB_LOGO_NEXVAL.png", layout="wide")

with st.sidebar:
    st.image('https://nexval.com/wp-content/uploads/2021/06/NEX_WEB_LOGO_NEXVAL.png')
    st.title('NexDeck')
    choice = st.radio("Features", ['Ingestion', 'Visualization', 'Intelligence', 'Dashboard'])
    st.info("NexDeck is our powerful platform providing document as well mortgage image understanding to uncover hidden actionable insights organizations need when working on their mortgage lifecycles.")

# Define the sidebar functionalities
if choice == 'Ingestion':
    st.title('File Ingestion')

    # Either user uploads zips from a desired location...
    uploaded_file = st.file_uploader('Choose zip files to be uploaded', type=['zip'], accept_multiple_files=True)

    # ...empty space for OR...
    middle_space = st.empty()
    middle_space.markdown('<p style="text-align:center; font-size:20px;">OR</p>', unsafe_allow_html=True)

    # ...or from entered location
    extraction_location = st.text_input('Enter the directory-path for extraction:')

    # Check if the user uploaded a file or entered a location
    if uploaded_file:
        # Temporary directory to store the uploaded zip files
        temp_dir = "temp_uploaded_files"
        os.makedirs(temp_dir, exist_ok=True)

        # Save uploaded files to the temporary directory
        for file in uploaded_file:
            with open(os.path.join(temp_dir, file.name), "wb") as f:
                f.write(file.getbuffer())

        # Display information about the uploaded files
        st.info(f"Uploaded {len(uploaded_file)} file(s): {', '.join([file.name for file in uploaded_file])}")

        # Perform zip file extraction and classification
        st.info(check_zip_file(path=temp_dir))
        unzip_files(path=temp_dir, unzip_destination_folder='D:/Python Projects/NexDeck/NexDeck/Airflow/UnZipFolderShipment/NewlyArrivedUnZipShipment')

        # Display success message
        st.success("File Ingestion, Unzipping, and Organization completed successfully!")

        # Cleanup: Remove temporary directory
        shutil.rmtree(temp_dir)

    elif extraction_location:
        # Perform zip file extraction and classification from the specified location
        st.info(check_zip_file(path=extraction_location))
        unzip_files(path=extraction_location, unzip_destination_folder='D:/Python Projects/NexDeck/NexDeck/Airflow/UnZipFolderShipment/NewlyArrivedUnZipShipment')

        # Display success message
        st.success(f"File Ingestion, Unzipping and Organization completed successfully for files in {extraction_location}.")

        # Optional: Clear the input field after processing
        extraction_location = ""

if choice == 'Visualization':
    st.title('Data Visualization')
    
    # Choices of the Visualization
    sub_choice = st.selectbox('Choose Visualization Option',['DocumentIndexingExtraction', 'PropertyPreservation'])

    if sub_choice == 'DocumentIndexingExtraction':
        st.subheader('List of Documents')
        directory_path = r'D:/Python Projects/NexDeck/NexDeck/Airflow/UnZipFolderShipment/DocumentIndexingExtraction/OCR_Text'
        # Get the list of files in the directory
        file_list = os.listdir(directory_path)
        selected_file = st.selectbox("Select a file to visualize entities", file_list)
        
        if selected_file:
            st.write("You selected file:", selected_file)   
            model_folder = os.path.join('models','invoice_reports')
            nlp = spacy.load(model_folder)
            
            file_path = os.path.join(directory_path, selected_file)
            with open(file_path, 'r', encoding='utf-8') as file:
                file_text = file.read()
            
            doc = nlp(file_text)
            ent_html = spacy.displacy.render(doc, jupyter=False, style='ent', options=options)
            st.markdown(ent_html, unsafe_allow_html=True)

    if sub_choice == 'PropertyPreservation':
        st.subheader('Segregated Images')

if choice == 'Intelligence':
    st.title('Actionable Insights')
    
    # Choices of the Visualization
    sub_choice = st.selectbox('Choose Extraction-Classification Option',['DocumentIndexingExtraction', 'PropertyPreservation'])

    if sub_choice == 'DocumentIndexingExtraction':
        st.subheader('Extracted Entities')
        directory_path = r'D:/Python Projects/NexDeck/NexDeck/Airflow/UnZipFolderShipment/DocumentIndexingExtraction/OCR_Text'
        file_list = os.listdir(directory_path)
        results = new_ner.extract_entities(file_list)
        st.write(results)


    if sub_choice == 'PropertyPreservation':
        st.subheader('Classified Images')

if choice == 'Dashboard':
    st.title('Day-to-Day Analytics')
    
    # Choices of the Visualization
    sub_choice = st.selectbox('Choose Analytics Dashboard',['DocumentIndexingExtraction', 'PropertyPreservation'])

    if sub_choice == 'DocumentIndexingExtraction':
        st.subheader('DocuChief')

    if sub_choice == 'PropertyPreservation':
        st.subheader('NexImage')
