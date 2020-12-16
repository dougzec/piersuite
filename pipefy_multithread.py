from urllib import request
import json
import pandas as pd
import numpy as np
import time

def requerir(querys, key_pipefy):

    headers = {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + key_pipefy
    }

    req = request.Request('https://app.pipefy.com/queries', data=querys.encode('utf-8'), headers=headers)
    r = request.urlopen(req).read()
    r = json.loads(r)
    #r = json.dumps(r, indent=2)
    #print(json.dumps(r, indent=2))
    return r

def run_criados_solo(r, out_fields=None):
    outputs = []
    for card in r['data']['allCards']['edges']:

        ## Base
        ID = card['node']['id'] #ID
        title = card['node']['title'] # title
        current_phase = card['node']['current_phase']['name'] # Fase atual
        done = card['node']['done'] #done
        created_at = card['node']['created_at'] # Created at
        finished_at = card['node']['finished_at'] # Finished at

        output = {'ID': ID,
                 'title': title,
                 'current_phase': current_phase,
                 'done': done,
                 'created_at': created_at,
                 'finished_at': finished_at}

        ## Fields
        if out_fields == None:
            fields = {field['field']['id']: field['value'] for field in card['node']['fields']}
            for out_field in fields:
                try:
                    try:
                        output.update({out_field: eval(fields[out_field])[0]})
                    except:
                        valor = fields[out_field]
                        if valor == '[]':
                            output.update({out_field: 'Sem Dados'})
                        else:
                            output.update({out_field: fields[out_field]})
                except:
                    output.update({out_field: 'Sem Dados'})
            outputs.append(output)
        else:
            out_fields = [field.lower().replace(' ', '_') for field in out_fields]
            fields = {field['field']['id']: field['value'] for field in card['node']['fields']}
            for out_field in out_fields:
                try:
                    try:
                        output.update({out_field: eval(fields[out_field])[0]})
                    except:
                        valor = fields[out_field]
                        if valor == '[]':
                            output.update({out_field: 'Sem Dados'})
                        else:
                            output.update({out_field: fields[out_field]})
                except:
                    output.update({out_field: 'Sem Dados'})
            outputs.append(output)
    return outputs

def pegar_ids(key_pipefy, pipe_id):

    IDs = ['']
    values_init = """
      {
        "query": "{ allCards(pipeId: """ + pipe_id + """, first: 50) { pageInfo { endCursor hasNextPage } } }"
      }
    """

    r = requerir(values_init, key_pipefy)
    id_fim = "\\\"" + r['data']['allCards']['pageInfo']['endCursor'] + "\\\""
    teste = r['data']['allCards']['pageInfo']['hasNextPage']

    IDs.append(id_fim)

    while teste == True: # Faz um loop ate que tenha dados


        values_init = """
          {
            "query": "{ allCards(pipeId: """ + pipe_id + """, first: 50, after: """ + id_fim + """) { pageInfo { endCursor hasNextPage } } }"
          }
        """

        r = requerir(values_init, key_pipefy)
        id_fim = "\\\"" + r['data']['allCards']['pageInfo']['endCursor'] + "\\\""
        teste = r['data']['allCards']['pageInfo']['hasNextPage']

        IDs.append(id_fim)

    return IDs[:-1]

def run_mid(id_fim, CAMPOS, rs, key_pipefy, pipe_id):
    values_next = """
      {
        "query": "{ allCards(pipeId: """ + pipe_id + """, first: 50, after: """ + id_fim + """) { edges { node { id title done created_at finished_at fields { field { id } value } current_phase { name } } } } }"
      }
    """

    r = requerir(values_next, key_pipefy)
    dados = run_criados_solo(r, CAMPOS)
#     print(dados)
    rs.extend(dados)

def run_init(CAMPOS, rs, key_pipefy, pipe_id):
    values_init = """
      {
        "query": "{ allCards(pipeId: """ + pipe_id + """, first: 100000) { edges { node { id title done created_at finished_at fields { field { id } value } current_phase { name } } } } }"
      }
    """

    r = requerir(values_init, key_pipefy)
    dados = run_criados_solo(r, CAMPOS)
#     print(dados)
    rs.extend(dados)

def rodar_multithread(key_pipefy, CAMPOS, pipe_id):

    rs = []

    IDs = pegar_ids(key_pipefy, pipe_id)

    from threading import Thread as t
    threads = [t(target=run_init, args=(CAMPOS, rs, key_pipefy, pipe_id)) if id_fim == '' else t(target=run_mid, args=(id_fim, CAMPOS, rs, key_pipefy, pipe_id)) for id_fim in IDs]
    for t in threads: t.start()
    for t in threads: t.join()

    print('Aguardando threads')
    time.sleep(1)

    return rs

def get_pipefy_multithread(key_pipefy, CAMPOS, pipe_id, is_carteira = True):
    rs = rodar_multithread(key_pipefy, CAMPOS, pipe_id)
    cart = pd.DataFrame(rs)
    cart = cart.fillna('Sem Dados')

    if is_carteira == False:
        return cart
    else:
        cart.grupo = np.where((cart.grupo == '0.0'), 'Sem Dados', cart.grupo)
        cart = cart.drop_duplicates('cnpj')
        colunas = ['cnpj', 'o_qu', 'grupo', 'customer_success_manager', 'atendimento_1', 'gerente_1', 'perfil_do_cliente', 'squad']
        cart = cart[colunas]
        cart.columns = ['cnpj_cart', 'Nome_Fantasia', 'Grupo', 'Customer_Success_Manager', 'Atendimento', 'Comercial', 'Rating', 'Squad']
        cart = cart.fillna('Sem Dados')

    return cart
