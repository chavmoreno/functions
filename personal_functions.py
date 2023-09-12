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
