from google.cloud import bigquery
from google.oauth2 import service_account

def salvar_bigquery(dataframe, project, table_name, key_path='.'):
    '''
    key_path = caminho para chave de nome key_bigquery.json
    '''

    #Login
    credentials = service_account.Credentials.from_service_account_file(key_path + '/key_bigquery.json')
    client = bigquery.Client(project = project,
                            credentials = credentials)

    # Salvar no bigquery
    dataframe.to_gbq(destination_table = table_name,
          project_id = project,
          if_exists='replace')

# def salvar_mysql
