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