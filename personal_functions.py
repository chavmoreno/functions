def exportar_df_bq(df, dataset_tablename, gcp_project_name):
    '''
    Uploads a pandas dataframe to a table in BigQuery. If the table aleady exists, it's replaced.

        Parameters:
                df (dataframe): The dataframe to upload.
                dataset_tablename (str): The name of the dataset and name of the table in the format of "dataset_name.tablename".
                gcp_project_name (str): The GCP Project ID.

        Returns:
                The table uploaded to BigQuery
    '''
    import pandas as pd
    
    df.to_gbq(
        destination_table=dataset_tablename,
        project_id=gcp_project_name,
        if_exists="replace")
    print('\n The dataframe has been exported to the table \033[1m{}\033[0m in the proyect \033[1m{}\033[0m'.format(dataset_tablename, gcp_project_name))


def getListOfFiles(google_drive, id_carpeta, fileExt = None, nombre_carpeta = None):
    '''
    Return the id from Google Drive of all files inside a folder  (includes files from subfolders too).

    Parameters:
            google_drive (auth): Google drive authorization.
            id_carpeta (str): Goolge id of the folder.
            fileExt (str, optional): files extension.
            nombre_carpeta (str, optional): name of the Google Drive's folder.

    Returns:
            allFiles (list): All the files from the folder listed.
    '''

    file_list = google_drive.ListFile({'q': "'"+id_carpeta +"' in parents and trashed=false"}).GetList()
    allFiles = list()
    for entry in file_list:
        if fileExt == None and entry['mimeType'] not in\
        ['application/vnd.google-apps.folder',
         'application/vnd.google-apps.document']:
          fileExt = entry['fileExtension'].lower()
        elif entry['mimeType'] == 'application/vnd.google-apps.document':
          fileExt = 'doc'
        arch_id = entry['id']
        nombre = entry['title']
        # Si el elemento es una carpeta, extrae los id's de sus archivos
        if entry['mimeType'] == 'application/vnd.google-apps.folder':
          allFiles = allFiles + getListOfFiles(google_drive,entry['id'],fileExt,nombre)
          #allFiles.append(((nombre,arch_id,nombre_carpeta)))
          if fileExt == None:
            allFiles.append((nombre,arch_id,nombre_carpeta))
        elif 'fileExtension' in entry.keys():
          if entry['fileExtension'].lower() == fileExt.lower():
            allFiles.append((nombre,arch_id,nombre_carpeta))

    allFiles = list(set(allFiles))
    return allFiles

def descarga_bmx_series(serie,fechainicio,fechafin):
    '''
    Downloads series information from Banxico's Api.
    
    Parameters:
            serie (str): Series to import
            fechainicio (str): Start date of the series.
            fechafin (str): End date of the series.

    Returns:
            df (dataframe): Dataframe containing the series.
    '''
    import requests
    import pandas as pd
    import numpy as np
    
    url = "https://www.banxico.org.mx/SieAPIRest/service/v1/series/"+serie+"/datos/"+fechainicio+"/"+fechafin
    token = "dc06f08527080a993a39de4c3d02b594c1bc12dd1644256ad1d64231d0c62df5"
    headers = {"Bmx-Token":token}
    response = requests.get(url,headers = headers)
    status = response.status_code
    if status!= 200:
        return print("error en la consulta, codigo{}".format(status))
    raw_data = response.json()
    data = raw_data["bmx"]["series"][0]["datos"]
    df = pd.DataFrame(data)
    df.columns = ["FECHA", "TC"]
    df.replace('N/E', np.nan, inplace = True)
    df["TC"] = df.TC.apply(lambda x: float(x))
    df["FECHA"] = pd.to_datetime(df.FECHA, format = "%d/%m/%Y")
    return df

def busqueda_cercano(df, columna, valor, extracto = True):
    '''
    Returns the nearest value of another value in a dataframe.
        
    Parameters:
            df (dataframe): dataframe to look up the value's neares.
            columna (str): Column to look up the value's nearest.
            valor (number): Value to be searched.
            extracto (bool, optional): If it's True, retrieves an extract of the dataframe that matches with the nearest value. if It's False, It only retrieves the nearest value. Default = True

    Returns:
            cercano (number): If extracto == False returns a number.
            df (dataframe): If extracto == True returns a dataframe.
    '''
    import pandas as pd
    
    idx = (df[columna] - valor).abs().idxmin()
    cercano = df[columna].loc[idx]

    if extracto == False:
        return cercano

    if extracto == True:
        return df[df[columna] == cercano]


def get_bq_tables(project_id,dataset = None):

    '''
    Get all the tables id's in a BigQuery project.

        Parameters:
                project_id (str): the project id.
                dataset (str, optional): The name of the dataset.

        Returns:
                tables_dict (dict): A dictionary containing all the tables in each dataset of the BigQuery Project.
                tables_list (list): A list containing all the tables from the dataset selected in that parameter.
    '''

    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)

    bq_datasets = client.list_datasets()
    tables_dict = {}

    for dataset_1 in bq_datasets:
        datasets_tables_dict = []
        tables = []
        for table in client.list_tables(dataset_1.dataset_id):
            tables.append(table.table_id)
        tables_dict[dataset_1.dataset_id] = tables
    
    if dataset is None:
        return tables_dict
    else:
        tables_list = tables_dict[dataset]
        return tables_list

def download_and_parse_schema(drive,folder_id, file_name):
    import ast
    
    drive = drive
    
    # Obtener la lista de archivos en la carpeta especificada
    lista_archivos = getListOfFiles(drive, folder_id, fileExt='txt')

    # Encontrar el archivo que coincide con el nombre dado
    link = [i[1] for i in lista_archivos if i[0] == file_name]

    if not link:
        raise FileNotFoundError(f"El archivo '{file_name}' no se encontró en la carpeta con ID '{folder_id}'.")

    # Descargar el archivo
    downloaded = drive.CreateFile({'id': link[0]})
    downloaded.GetContentFile(file_name)

    # Leer el contenido del archivo de texto
    with open(file_name, 'r') as file:
        file_content = file.read()

    # Convertir el contenido en una estructura de datos de Python (lista de diccionarios)
    schema = ast.literal_eval(file_content)

    return schema


def load_dataframe_to_bigquery(schema, dataframe, dataset_id, table_id, project=None, write_disposition="WRITE_APPEND"):
    
    column_names = [column['name'] for column in schema] # Extrae los nombres de las columnas
    dataframe = dataframe[column_names] #únicamente nos quedamos en el dataframe con las columnas que están en el esquema
    
    from google.cloud import bigquery
    # Convertir la lista de diccionarios a bigquery.SchemaField
    bq_schema = []
    for field in schema:
        bq_schema.append(bigquery.SchemaField(
            name=field['name'],
            field_type=field['type'],
            mode=field.get('mode', 'NULLABLE'),  # Default to 'NULLABLE' if not specified
            description=field.get('description', "")
        ))

    # Configurar el LoadJobConfig con el esquema y write_disposition
    job_config = bigquery.LoadJobConfig(
        schema=bq_schema,
        write_disposition=write_disposition
    )

    # Crear el cliente de BigQuery, especificando el proyecto si se proporciona
    client = bigquery.Client(project=project) if project else bigquery.Client()

    # Construir el ID completo de la tabla
    table_full_id = f"{dataset_id}.{table_id}"

    # Cargar el DataFrame en BigQuery
    load_job = client.load_table_from_dataframe(
        dataframe,
        table_full_id,
        job_config=job_config
    )

    # Esperar a que la carga se complete
    load_job.result()
    print(f'\033[1;32mCarga completada con éxito en la tabla {table_full_id}\033[0m')
