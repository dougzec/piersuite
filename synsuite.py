#importacoes
import os
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import time
import unicodedata
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import timedelta, date, datetime

#limpar dados
def limpar_pasta(path='.'):
    for file in os.listdir(path + '/arquivos_synsuite/'):
        os.remove(path + '/arquivos_synsuite/' + file)

def scrap_synsuite(login, senha, path='.'):

    chromeOptions = webdriver.ChromeOptions() #inicializa o webdriver

    # nao baixar imagens e diretorio para download do chrome
    if path == '.':
        path_ = os.getcwd()
    else:
        path_ = path

    prefs = {'profile.managed_default_content_settings.images':2,
            'download.default_directory' : path_ + '/arquivos_synsuite'}

    chromeOptions.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chromeOptions)
    try:
        driver.get('https://synsuite.reweb.com.br/customized_reports') #acessa o synsuite

        # Login
        driver.find_element_by_xpath('//*[@id="UserLogin"]').send_keys(login)
        driver.find_element_by_xpath('//*[@id="UserPassword2"]').send_keys(senha)
        driver.find_element_by_xpath('//*[@id="UserLoginForm"]/button').click()

        time.sleep(10)

        campos = [26, 27, 28, 29] #ordem dos relatorios a serem baixados

        # loop para baixar os arquivos. 15s de espera para dar tempo de processar
        for campo in campos:
            driver.find_element_by_xpath('//*[@id="customized-reports-table"]/tbody/tr[{}]/td'.format(campo)).click()
            time.sleep(2)
            driver.find_element_by_xpath('//*[@id="download"]').click()
            time.sleep(2)
            try:
                driver.find_element_by_xpath('/html/body/div[4]/div[3]/div/button[1]').click()
            except:
                pass
            time.sleep(15)

        time.sleep(10)

        driver.close()
    except:
        driver.close()

def scrap_synsuite_multithread(login, senha, path='.'):

    chromeOptions = webdriver.ChromeOptions() #inicializa o webdriver

    # nao baixar imagens e diretorio para download do chrome
    if path == '.':
        path_ = os.getcwd()
    else:
        path_ = path

    prefs = {'profile.managed_default_content_settings.images':2,
            'download.default_directory' : path_ + '/arquivos_synsuite'}

    chromeOptions.add_experimental_option("prefs", prefs)

    def scrap_unico(campo):
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=chromeOptions)
        try:
            driver.get('https://synsuite.reweb.com.br/customized_reports') #acessa o synsuite

            # Login
            driver.find_element_by_xpath('//*[@id="UserLogin"]').send_keys(login)
            driver.find_element_by_xpath('//*[@id="UserPassword2"]').send_keys(senha)
            driver.find_element_by_xpath('//*[@id="UserLoginForm"]/button').click()

            time.sleep(10)


            driver.find_element_by_xpath('//*[@id="customized-reports-table"]/tbody/tr[{}]/td'.format(campo)).click()
            time.sleep(2)
            driver.find_element_by_xpath('//*[@id="download"]').click()
            time.sleep(2)
            try:
                driver.find_element_by_xpath('/html/body/div[4]/div[3]/div/button[1]').click()
            except:
                pass
            time.sleep(30)

            driver.close()
        except:
            driver.close()

    campos = [26, 27, 28, 29] #ordem dos relatorios a serem baixados

    from threading import Thread as t
    threads = [t(target=scrap_unico, args=(campo,)) for campo in campos]
    for t in threads: t.start()
    for t in threads: t.join()

def strip_accents(text):

    try:
        text = unicode(text, 'utf-8')
    except NameError: # unicode is a default on python 3
        pass

    text = unicodedata.normalize('NFD', text)\
           .encode('ascii', 'ignore')\
           .decode("utf-8")

    return str(text)

def get_last_mod(file_name):
    (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(str(file_name))
    return ctime

def gerar_dataframes(path='.'):
    arquivos_gerar = {'car': 'datastudio_contas_a_receber',
                      'contas': 'datastudio_contas-',
                      'notas': 'datastudio_notas_fiscais',
                      'itens_nf': 'datastudio_itens_nf'}

    arquivos = {}
    for var in arquivos_gerar:
        files = {file_name: get_last_mod(path + '/arquivos_synsuite/' + file_name) for file_name in os.listdir(path + '/arquivos_synsuite/') if arquivos_gerar[var] in file_name}
        file = max(files, key=files.get)
        print(file)
        arquivos.update({var: path + '/arquivos_synsuite/' + file})

    notas = pd.read_csv(arquivos['notas'], sep=';', decimal=',', dtype={'CPF/CNPJ':'string', 'Dcto': 'string'})
    itens_nf = pd.read_csv(arquivos['itens_nf'], sep=';', decimal=',', dtype={'Número':'string'})
    contas = pd.read_csv(arquivos['contas'], sep=';', decimal=',', dtype={'CPF/CNPJ':'string', 'Cliente': 'string'})
    car = pd.read_csv(arquivos['car'], sep=';', decimal=',', dtype={'Nota Fiscal':'string'})

    ### Ajustes CAR
    car['Excluído'] = car['Excluído'].replace({'Não':0, 'Sim':1})
    car['Excluído'] = car['Excluído'].fillna(3)
    car = car[(car['Tipo Cobrança'] != r'\N')] #  & (car['Competência'] == '10/2020')

    colunas = ['Vcto', 'Vcto Original', 'Dt Último Pagamento']
    for coluna in colunas:
        car[coluna] = pd.to_datetime(car[coluna], format='%d/%m/%Y', errors='coerce').dt.date

    car['Competência'] = pd.to_datetime(car['Competência'], format='%m/%Y', errors='coerce').dt.date

    hoje = date.today()

    car['Status_fatura'] = np.where((car['Saldo'] == 0), 'Pago',
                           np.where((car['Saldo'] > 0) & (hoje > car['Vcto']), 'Vencido',
                           np.where((car['Saldo'] > 0) & (hoje <= car['Vcto']), 'A vencer', 'Outro')))

    car['Status_fatura'] = np.where((car['Vcto'] > car['Vcto Original']) | (car['Dt Último Pagamento'] > car['Vcto']), car['Status_fatura'] + ' - Renegociado', car['Status_fatura'])

    ### Ajustes Notas
    notas['Imposto_Retido'] = (notas['Aliquota PIS'] + notas['Aliquota COFINS'] + notas['Aliquota CSLL'])*notas['CSLL retido'].replace({'Sim': 1, 'Não': 0})/100 + notas['Aliquota IRRF']*notas['IRRF retido'].replace({'Sim': 1, 'Não': 0})/100
    notas['Mês/Ano'] = pd.to_datetime(notas['Mês/Ano'], format='%m/%Y', errors='coerce').dt.date
    notas = notas[(notas['CPF/CNPJ'] != '04456693000182') &
                 (notas['CPF/CNPJ'] != '07647721000137') &
                 (notas['CPF/CNPJ'] != '13876759000184')]

    print('Status da fatura criado')

    replace = {
        'Provimento de servico de aplicacao para internet - S21': 'Google',
        'Provimento de servico de aplicacao para internet - SI21': 'Google',
        'Provimento de servico de aplicacao para internet - SI22': 'Facebook',
        'Provimento de servico de aplicacao para internet - SI23': 'Waze',
        'Provimento de servico de aplicacao para internet - SI31': 'LinkedIn',
    }

    ### Ajustes itens_nf
    itens_nf['Descricao_final'] = itens_nf['Descrição']
    itens_nf['Descricao_final'] = np.where(np.isin(itens_nf['Descricao_final'], list(replace)), itens_nf['Descricao_final'], 'Fee')
    itens_nf['Descricao_final'] = itens_nf['Descricao_final'].replace(replace)

    ### Ajustes notas
    notas['Dcto'] = notas['Dcto'].astype(str)

    ### Cruzamentos
    itens_nf['Número'] = itens_nf['Número'].astype(str)
    colunas = [coluna for coluna in itens_nf.columns if coluna not in notas.columns]
    colunas.append('Local')
    notas_itens = pd.merge(notas, itens_nf[colunas], how='left', left_on=['Dcto', 'Local'], right_on=['Número', 'Local'], copy=False)

    colunas = [coluna for coluna in contas.columns if coluna not in notas_itens.columns]
    notas_itens_contas = pd.merge(notas_itens, contas[colunas], how='left', left_on='Cod. Cliente', right_on='Código', copy=False)

    car['Nota Fiscal'] = car['Nota Fiscal'].astype(str)
    colunas = [coluna for coluna in car.columns if coluna not in notas_itens_contas.columns]
    colunas.append('Local')
    notas_itens_contas_car = pd.merge(notas_itens_contas, car[colunas], how='left', left_on=['Dcto', 'Local'], right_on=['Nota Fiscal', 'Local'], copy=False)

    ### Trocar nome colunas
    colunas = notas_itens_contas_car.columns
    colunas = [coluna.replace(' ', '_') for coluna in colunas]
    colunas = [coluna.replace('.', '') for coluna in colunas]
    colunas = [coluna.replace('/', '_') for coluna in colunas]
    colunas = [strip_accents(coluna) for coluna in colunas]

    notas_itens_contas_car.columns = colunas

    notas_itens_contas_car['Liquido'] = (1-notas_itens_contas_car['Imposto_Retido'])*notas_itens_contas_car['Montante_Total']

    colunas = ['Competencia', 'Estado', 'Cidade', 'Cliente', 'CPF_CNPJ', 'N_Titulo', 'Vendedor', 'Vendedor_2', 'Montante_Total', 'Descricao_final', 'Descricao', 'Status_fatura', 'Liquido', 'Imposto_Retido', 'Mes_Ano', 'Dcto', 'Local', 'Excluido', 'Vcto', 'Vcto_Original' ,'Dt_Ultimo_Pagamento', 'Unidades', 'Montante_Unitario']

    export = notas_itens_contas_car[colunas][notas_itens_contas_car['Competencia'] >= date(2010,1,1)] # Ajuste do período
    export = notas_itens_contas_car[colunas]

    date_cols = ['Competencia', 'Mes_Ano', 'Vcto', 'Vcto_Original', 'Dt_Ultimo_Pagamento']
    str_cols = ['Estado', 'Cidade', 'Cliente', 'CPF_CNPJ', 'N_Titulo', 'Vendedor', 'Vendedor_2', 'Status_fatura', 'Excluido', 'Dcto', 'Local', 'Descricao', 'Unidades', 'Montante_Unitario']

    for col in date_cols:
        export[col] = export[col].fillna(date(2099,1,1))

    for col in str_cols:
        export[col] = export[col].fillna('0')

    colunas = ['Competencia', 'Estado', 'Cidade', 'Cliente', 'CPF_CNPJ', 'N_Titulo', 'Vendedor', 'Vendedor_2', 'Status_fatura', 'Excluido', 'Vcto', 'Vcto_Original' ,'Dt_Ultimo_Pagamento', 'Local', 'Descricao', 'Unidades', 'Montante_Unitario'] #
    df = pd.pivot_table(export, ['Montante_Total', 'Liquido'], colunas, 'Descricao_final', 'sum').reset_index()
    df.columns = [col[1] + '_' + col[0] if col[0] not in colunas else col[0] for col in df.columns]

    df.loc[:,'Facebook_Liquido':] = df.loc[:,'Facebook_Liquido':].fillna(0)
    df['Receita'] = df['Fee_Liquido']+df['Google_Liquido']+df['Facebook_Liquido']+df['Waze_Liquido']
    df['NF_Bruto'] = df['Fee_Montante_Total']+df['Google_Montante_Total']+df['Facebook_Montante_Total']+df['Waze_Montante_Total']
    df['Midia_Bruto'] = df['Google_Montante_Total']+df['Facebook_Montante_Total']+df['Waze_Montante_Total']

    dia_hoje = date.today().day
    df['Menor_igual_hoje'] = df.Dt_Ultimo_Pagamento
    df['Menor_igual_hoje'] = pd.to_datetime(df['Menor_igual_hoje'], errors = 'coerce')
    df['Menor_igual_hoje'] = df['Menor_igual_hoje'].fillna(date(2000,1,1))
    df['Mes_pgto'] = df.Menor_igual_hoje.dt.to_period('M').dt.to_timestamp()
    df['Menor_igual_hoje'] = np.where(df['Menor_igual_hoje'].dt.day < dia_hoje, 'Sim', 'Não')
    if dia_hoje == 1:
        df['Menor_igual_hoje'] = 'Sim'
    else:
        pass


    fat18 = pd.read_excel(path + '/fat18.xls', dtype={'CNPJ_CPF':'string', 'NOME_CLIENTE': 'string'})
    fat18 = fat18.iloc[1:,:]

    fat18.columns = ['Competencia', 'Estado', 'Cidade', 'Cliente', 'CPF_CNPJ', 'N_Titulo',
                    'Status_fatura', 'Vendedor', 'Vendedor_2', 'Receita', 'NF_Bruto', 'Google_Montante_Total',
                    'Facebook_Montante_Total', 'Waze_Montante_Total', 'Fee_Liquido']

    fat18.Competencia = pd.to_datetime(fat18.Competencia).dt.date

    fat18['Fee_Montante_Total'] = fat18['NF_Bruto'] - fat18['Google_Montante_Total'] - fat18['Facebook_Montante_Total'] - fat18['Waze_Montante_Total']



    df = df[(df.Competencia <= date(2017,12,1)) | (df.Competencia >= date(2018,9,1))]
    fat18_util = fat18[(fat18.Competencia >= date(2018,1,1)) & (fat18.Competencia <= date(2018,8,1))]
    df = pd.concat([df, fat18_util])

    date_cols = ['Competencia', 'Mes_Ano', 'Vcto', 'Vcto_Original', 'Dt_Ultimo_Pagamento', 'Mes_pgto']
    str_cols = ['Estado', 'Cidade', 'Cliente', 'CPF_CNPJ', 'N_Titulo', 'Vendedor', 'Vendedor_2', 'Status_fatura', 'Excluido', 'Dcto', 'Local', 'Descricao', 'Unidades']

    for col in date_cols:
        try:
            df[col] = df[col].fillna(date(2099,1,1))
        except:
            pass

    for col in str_cols:
        try:
            df[col] = df[col].fillna('0')
        except:
            pass

    cols = ['Competencia', 'Vcto' ,'Dt_Ultimo_Pagamento', 'Mes_pgto']

    for col in cols:
        try:
            df[col] = pd.to_datetime(df[col]).dt.date
        except:
            pass

    df = df.fillna(0)

    df['Excluido'] = df['Excluido'].fillna('0')
    df = df[(df.Excluido == 0) | (df.Excluido == '0')]

        ### Trocar nome colunas
#     df.Cliente = df.Cliente.str.replace(' ', '_')
    df.Cliente = df.Cliente.str.replace('.', '')
    df.Cliente = df.Cliente.str.replace('/', '_')
#     df.Cliente = df.Cliente.apply(lambda x: strip_accents(x))

    return (df, contas, notas, itens_nf, car, fat18)

def unir_synsuite_carteira(path = '.'):
    df_synsuite = pd.read_pickle(path + '/dados_synsuite.pkl')
    df_carteira = pd.read_pickle(path + '/dados_carteira.pkl')

    x = pd.merge(df_synsuite, df_carteira, how='left', left_on = 'CPF_CNPJ', right_on='cnpj_cart') #.drop('cnpj_cart', axis='columns')
    x = x.fillna('Sem Dados')

    return x
