from urllib import request
import json
import pandas as pd
import numpy as np

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

def run_criados(r, dados, out_fields=None):
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
            pass
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

        dados.append(output)

    return dados

def dataframe_carteira(key_pipefy):

    #ultimo da pagina
    values_init = """
      {
        "query": "{ allCards(pipeId: 790743, first: 100000) { pageInfo { endCursor hasNextPage } edges { node { id title done created_at finished_at fields { field { id } value } current_phase { name } } } } }"
      }
    """

    CAMPOS = ['cnpj',
                'o_qu',
                'grupo',
                'customer_success_manager',
                'atendimento_1',
                'gerente_1',
                'perfil_do_cliente',
                'squad']

    dados = [] #inicia uma lista para jogar as informacoes dentro
    r = requerir(values_init, key_pipefy)
    id_fim = "\\\"" + r['data']['allCards']['pageInfo']['endCursor'] + "\\\""
    teste = r['data']['allCards']['pageInfo']['hasNextPage']

    dados = run_criados(r, dados, CAMPOS)

    cont = 50

    while teste == True: # Faz um loop ate que tenha dados
        values_next = """
          {
            "query": "{ allCards(pipeId: 790743, first: 50, after: """ + id_fim + """) { pageInfo { endCursor hasNextPage } edges { node { id title done created_at finished_at fields { field { id } value } current_phase { name } } } } }"
          }
        """

        r = requerir(values_next, key_pipefy)
        id_fim = "\\\"" + r['data']['allCards']['pageInfo']['endCursor'] + "\\\""
        teste = r['data']['allCards']['pageInfo']['hasNextPage']

        dados = run_criados(r, dados, CAMPOS)

        cont += 50

    cart = pd.DataFrame(dados)
    cart.grupo = np.where((cart.grupo == '0.0'), 'Sem Dados', cart.grupo)
    cart = cart.drop_duplicates('cnpj')
    colunas = ['cnpj', 'o_qu', 'grupo', 'customer_success_manager', 'atendimento_1', 'gerente_1', 'perfil_do_cliente', 'squad']
    cart = cart[colunas]
    cart.columns = ['cnpj_cart', 'Nome_Fantasia', 'Grupo', 'Customer_Success_Manager', 'Atendimento', 'Comercial', 'Rating', 'Squad']
    cart = cart.fillna('Sem Dados')
    # cart['cnpj_cart'] = cart['cnpj_cart'].apply(lambda x: x.zfill(14))

    return cart
