import pandas as pd
import numpy as np
from datetime import datetime, date
import dateutil.relativedelta



### COMPARATIVOS DE FEE E MIDIA

def classificar(row):
    if (row.M0 == row.M_1) and (row.M0 != 0): classif = 'Igual'
    elif ((row.M0+row.M_1+row.M_2) == row.M0) and (row.M0 != 0): classif = 'Novo'
    elif row.Delta > 0: classif = 'Upsell'
    elif (row.Delta < 0) and (row.M0 > 0): classif = 'Downsell'
    elif (row.Delta < 0) and (row.M0 == 0): classif = 'Cancelamento/Isenção/Job'
    else: classif = 'Outro'

    return classif

def gerar_comparativo(valor='Fee_Liquido', dia_base=None, path = '.'):

    x = pd.read_pickle(path + '/synsuite_carteira.pkl')

    if dia_base is None:
        d = date.today()
    else:
        d = date(dia_base[0],dia_base[1],dia_base[2])

    d2 = d - dateutil.relativedelta.relativedelta(months=4)

    x = x[(x['Competencia'] >= d2) & (x['Competencia'] <= d)]

    colunas = ['Estado', 'Cliente', 'CPF_CNPJ', 'Customer_Success_Manager', 'Atendimento', 'Comercial', 'Rating', 'Squad', 'Grupo', 'Vendedor', 'Local']

    comp = pd.pivot_table(x, valor, colunas, 'Competencia', 'sum', fill_value=0).reset_index().fillna(0)
    colunas = list(comp.columns)
    colunas[-4:] = ['M_3', 'M_2', 'M_1', 'M0']
    comp.columns = colunas
    comp['Data_atualizacao'] = d
    comp['Data_atualizacao'] = pd.to_datetime(comp['Data_atualizacao'])
    comp['Delta'] = comp.M0 - comp.M_1

    comp['Classificacao'] = comp.apply(lambda x: classificar(x), axis=1)

    colunas.extend(['Data_atualizacao', 'Delta' ,'Classificacao'])

    comp = comp[colunas]

    return comp
