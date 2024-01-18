# Technical Test Phase II: Data Engineer & Analyst

- Candidato: Camilo Andrés Pérez Ospino
- Telfono: 3238119950
- Correo: camilo.perez.osp@gmail.com

## Technical test: 
Our team designed and save a DataFrame with dummy records in a Source HubSpot
Account, in order to be able to perform the data Extraction through this access token key:

xxxxxx-xxxxxxxxxxxxx-xxxxxxxxx

In this test you will apply the most common data processing in the company, use the IDE of your choice for the development of the algorithms, however, the building of the data pipeline with the ETL process must be presented and documented in a Jupyter notebook. WARNING: API implementation could be using Requests python library, however you can't use HubSpot library.

## Requirement

### 1. Create a library
- Extraction Functions
    - a. Contact collection
- Transformation functions
    - a. Country Recognition
    - b. Found Emails
    - c. Fix Phone Numbers
- Load Functions
    - a. Saving Contacts
- Duplicate management

### 2. Interactive graphics

### 3. Data Pipeline covering

## My solution
### Files
- data_pipeline.ipynb: Notebook containing the scan and execution of the pipeline necessary for exporting data from Source HubSpot Account to your HubSpot Account.

- viwer.ipynb: Notebook containing interactive graph visualizations. 
    \
    ** Note: First, data_pipeline.ipynb must be executed to save locally the necessary records to make the graphs

- automatic_migrate.py: script en lenguaje python que ejecuta la migracion de datos de Source HubSpot Account a tu Cuenta HubSpot. Para su ejecución se solicita API KEY de Source HubSpot Account y la API KEY de HubSpot Account.
### Folders
- models: contains a set of scripts that are part of the library created for the pipeline execution.

- utils: set of help or support scripts implemented in this solution.
