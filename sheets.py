import gspread
import pandas as pd
from datetime import timedelta, date, datetime

def sheets_faturamento(url, aba, path='.'):
    x = pd.read_pickle(path + '/synsuite_carteira.pkl')

    gc = gspread.service_account(filename=path + '/key_bigquery.json')
    sh = gc.open_by_key(url) # or by sheet name: gc.open("TestList")
    worksheet = sh.worksheet(aba)

    worksheet.delete_rows(3,10000000)

    colunas = ['Competencia','Estado', 'Cidade', 'Cliente', 'CPF_CNPJ', 'N_Titulo', 'Status_fatura', 'Vendedor', 'Vendedor_2', 'Receita', 'NF_Bruto', 'Google_Montante_Total', 'Facebook_Montante_Total', 'Waze_Montante_Total', 'Fee_Liquido', 'Midia_Bruto', 'Fee_Montante_Total']
    x19 = x[colunas][x.Competencia >= date(2019,1,1)]
    x19['Competencia'] = x19['Competencia'].apply(lambda x: x.strftime('%d/%m/%Y'))

    colunas = ['Mês/Ano Competência', 'Estado', 'Cidade', 'NOME_CLIENTE', 'CNPJ_CPF',
          'Nº Título', 'STATUS_FATURA', 'Vendedor 1', 'Vendedor 2', 'Receita',
          'NF (Bruto)', 'GA (bruto)', 'Facebook (bruto)', 'Waze (bruto)',
          'Fee (líquido)', 'Mídia total (bruto)', 'Fee (bruto)']

    x19 = x19.astype(str)
    for coluna in x19.loc[:,'Receita':].columns:
       x19[coluna] = x19[coluna].str.replace('.',',')

    x19.columns = colunas
    worksheet.update('A1', [x19.columns.values.tolist()] + x19.values.tolist(), value_input_option='USER_ENTERED')
    worksheet.update('E1', [x19[['CNPJ_CPF']].columns.values.tolist()] + x19[['CNPJ_CPF']].values.tolist(), value_input_option='RAW')

def sheets_carteira(url, aba, project, table_name, path='.'):

    from google.cloud import bigquery
    from google.oauth2 import service_account

    credentials = service_account.Credentials.from_service_account_file(path + '/key_bigquery.json')
    client = bigquery.Client(project = project,
                            credentials = credentials)

    # Salvar no bigquery
    df = pd.read_gbq("SELECT cnpj, title, grupo FROM reweb.carteira_clientes",
          project_id = project)

    gc = gspread.service_account(filename=path + '/key_bigquery.json')
    sh = gc.open_by_key(url) # or by sheet name: gc.open("TestList")
    worksheet = sh.worksheet(aba)

    worksheet.delete_rows(3,10000000)

    df = df.astype(str)
    # for coluna in df.loc[:,'Receita':].columns:
    #    x19[coluna] = x19[coluna].str.replace('.',',')

    # df.columns = colunas
    worksheet.update('A1', [df.columns.tolist()] + df.values.tolist(), value_input_option='RAW')
    # worksheet.update('E1', [x19[['CNPJ_CPF']].columns.values.tolist()] + x19[['CNPJ_CPF']].values.tolist(), value_input_option='RAW')
