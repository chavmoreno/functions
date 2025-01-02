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

def align_df_and_schema(dataframe, schema):
    """
    Función auxiliar que:
      - Valida si el DataFrame y el schema están vacíos.
      - Calcula las diferencias de columnas entre el DataFrame y el schema.
      - Filtra el DataFrame y el schema para alinearlos.

    Parámetros
    ----------
    dataframe : pandas.DataFrame
        El DataFrame que se desea alinear.
    schema : list
        Lista de diccionarios que describe el esquema de la tabla en BigQuery.
        Cada diccionario incluye campos como: 'name', 'type', 'description', 'mode'.

    Retorno
    -------
    dataframe : pandas.DataFrame
        DataFrame filtrado, solo con las columnas que coinciden con el schema.
    schema : list
        Lista de campos del schema filtrados, para coincidir con las columnas del DataFrame.
    df_not_in_schema : list
        Columnas presentes en el DataFrame pero ausentes en el schema.
    schema_not_in_df : list
        Nombres de campo presentes en el schema pero ausentes en el DataFrame.
    """

    # 1) Validar si el DataFrame está vacío
    if dataframe.empty:
        print("\033[1;31mEl DataFrame está vacío; no se realizará alineación.\033[0m")
        return dataframe, schema, [], []

    # 2) Validar si el schema está vacío
    if not schema:
        print("\033[1;31mEl esquema está vacío; no se realizará alineación.\033[0m")
        return dataframe, schema, [], []

    # 3) Transformar a conjuntos para facilitar operaciones de diferencia/intersección
    schema_col_names = {field['name'] for field in schema}
    df_original_cols = set(dataframe.columns)

    # 4) Hallar qué columnas NO coinciden
    df_not_in_schema = list(df_original_cols - schema_col_names)   # Sobra en DF
    schema_not_in_df = list(schema_col_names - df_original_cols)   # Falta en DF

    # 5) Filtrar el DataFrame y el schema para mantener solo la intersección
    valid_columns = schema_col_names & df_original_cols
    dataframe = dataframe[list(valid_columns)]
    schema = [field for field in schema if field['name'] in valid_columns]

    return dataframe, schema, df_not_in_schema, schema_not_in_df


def validate_dtypes(dataframe, schema):
    """
    Validación sencilla de tipos de datos:
    - Si la columna está definida como INTEGER en el schema pero en el DataFrame
      es float o string, se intenta convertir a entero (en la medida de lo posible).
    - Para otros tipos (STRING, TIMESTAMP, etc.), solo se imprime una advertencia 
      si no coinciden, pero no se hace conversión automática aquí.
    
    Parámetros
    ----------
    dataframe : pandas.DataFrame
        DataFrame ya filtrado según el schema.
    schema : list
        Lista de diccionarios que describe el esquema.
    """

    for field in schema:
        col_name = field['name']
        col_type = field['type'].upper()

        # Si por algún motivo la columna no está en el DataFrame (aunque filtramos antes),
        # se continúa sin procesar.
        if col_name not in dataframe.columns:
            continue

        # Intento de conversión de float a int si el tipo esperado es INTEGER.
        if col_type == "INTEGER":
            if pd.api.types.is_float_dtype(dataframe[col_name]):
                print(f"\033[1;33mConvirtiendo la columna '{col_name}' de float a entero.\033[0m")
                try:
                    # Usamos pd.Int64Dtype() para permitir valores nulos (NaN) 
                    # en caso de que existan y no se puedan convertir a int.
                    dataframe[col_name] = dataframe[col_name].astype(pd.Int64Dtype())
                except Exception as e:
                    print(f"\033[1;31mNo se pudo convertir la columna '{col_name}' a entero: {e}\033[0m")


def load_dataframe_to_bigquery(
    schema,
    dataframe,
    dataset_id,
    table_id,
    project=None,
    write_disposition="WRITE_APPEND"
):
    """
    Función principal para cargar un DataFrame de pandas en una tabla de BigQuery,
    asegurando que DataFrame y schema estén alineados, y mostrando diferencias.

    Parámetros
    ----------
    schema : list
        Lista de diccionarios que describe el esquema de la tabla en BigQuery.
        Cada diccionario incluye, por ejemplo: 'name', 'type', 'description', 'mode'...
    dataframe : pandas.DataFrame
        DataFrame que se desea cargar a BigQuery.
    dataset_id : str
        ID del dataset de BigQuery donde se encuentra (o se creará) la tabla.
    table_id : str
        Nombre de la tabla en BigQuery.
    project : str, opcional
        ID del proyecto de Google Cloud. Si no se especifica, usará el proyecto por defecto.
    write_disposition : str, opcional
        Define la forma de escribir en la tabla:
        - "WRITE_APPEND": Inserta (anexa) nuevas filas.
        - "WRITE_TRUNCATE": Borra la tabla y vuelve a crearla con los nuevos datos.
        - "WRITE_EMPTY": Falla si la tabla ya contiene datos.
        Por defecto, "WRITE_APPEND".

    Retorno
    -------
    None
    """

    # -------------------------------------------------------------------------
    # 1) Alineación del DataFrame y el schema (función auxiliar).
    # -------------------------------------------------------------------------
    dataframe, schema, df_not_in_schema, schema_not_in_df = align_df_and_schema(dataframe, schema)

    # 2) Reportar diferencias, usando color ANSI en consola:
    #    - rojo si hay diferencias
    #    - verde si no
    if schema_not_in_df:
        print(
            f"\033[1;31mColumnas del schema que NO estaban en el DataFrame ({len(schema_not_in_df)}): {schema_not_in_df}\033[0m"
        )
    else:
        print("\033[1;32mNo hay columnas del schema que falten en el DataFrame.\033[0m")

    if df_not_in_schema:
        print(
            f"\033[1;31mColumnas del DataFrame que NO estaban en el schema ({len(df_not_in_schema)}): {df_not_in_schema}\033[0m"
        )
    else:
        print("\033[1;32mNo hay columnas del DataFrame que falten en el schema.\033[0m")

    # -------------------------------------------------------------------------
    # 3) Validación sencilla de tipos de datos en el DataFrame (opcional).
    # -------------------------------------------------------------------------
    validate_dtypes(dataframe, schema)

    # -------------------------------------------------------------------------
    # 4) Configurar el esquema para BigQuery (bigquery.SchemaField).
    # -------------------------------------------------------------------------
    bq_schema = []
    for field in schema:
        bq_schema.append(
            bigquery.SchemaField(
                name=field['name'],
                field_type=field['type'],
                mode=field.get('mode', 'NULLABLE'),  # Por defecto, si 'mode' no existe, se asume NULLABLE
                description=field.get('description', "")
            )
        )

    # -------------------------------------------------------------------------
    # 5) Crear el cliente de BigQuery, si no se especifica un proyecto 
    #    se toma el proyecto por defecto de la configuración local.
    # -------------------------------------------------------------------------
    client = bigquery.Client(project=project) if project else bigquery.Client()

    # -------------------------------------------------------------------------
    # 6) Configurar la carga con LoadJobConfig (esquema + write_disposition).
    # -------------------------------------------------------------------------
    job_config = bigquery.LoadJobConfig(
        schema=bq_schema,
        write_disposition=write_disposition
    )

    # -------------------------------------------------------------------------
    # 7) Identificador completo de la tabla "dataset.table_id".
    # -------------------------------------------------------------------------
    table_full_id = f"{dataset_id}.{table_id}"

    # -------------------------------------------------------------------------
    # 8) Ejecutar la carga del DataFrame a BigQuery dentro de un bloque try/except.
    # -------------------------------------------------------------------------
    print(f"\033[1;34mIniciando carga a la tabla {table_full_id}...\033[0m")

    try:
        load_job = client.load_table_from_dataframe(
            dataframe, table_full_id, job_config=job_config
        )
        load_job.result()  # Espera a que finalice el job de carga
    except Exception as e:
        print(f"\033[1;31mError al cargar los datos en BigQuery: {e}\033[0m")
        return

    # -------------------------------------------------------------------------
    # 9) Si no hubo excepciones, se muestra el éxito en color verde.
    # -------------------------------------------------------------------------
    print(f"\033[1;32mCarga completada con éxito en la tabla {table_full_id}\033[0m")

