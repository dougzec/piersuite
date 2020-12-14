from datetime import datetime

def criar_log(mensagem, path=None):
    if path == None:
        log = './log.txt'
    else:
        log = path + '/log.txt'
    with open(log, 'a') as log:
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        log.write('{} {}\n'.format(current_time, mensagem))
        print('{} {}\n'.format(current_time, mensagem))


## Teste stestesfsfsdf
# fsdçofksdfjsdklfdsj
