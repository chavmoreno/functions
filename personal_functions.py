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
    df.to_gbq(
        destination_table=dataset_tablename,
        project_id=gcp_project_name,
        if_exists="replace")
    print('\n The dataframe has been exported to the table \033[1m{}\033[0m in the proyect \033[1m{}\033[0m'.format(dataset_tablename, gcp_project_name))


def getListOfFiles(google_drive, id_carpeta, fileExt = None, nombre_carpeta = None):
    """
    Return the id from Google Drive of all files inside a folder  (includes files from subfolders too).

    Parameters:
            google_drive (auth): Google drive authorization.
            id_carpeta (str): Goolge id of the folder.
            fileExt (str, optional): files extension.
            nombre_carpeta (str, optional): name of the Google Drive's folder.

    Returns:
            allFiles (list): All the files from the folder listed.
    """

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
