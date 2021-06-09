import os
import pandas as pd
import funcoes.conexao as conectModel
import arq.consulta as cons
from collections import defaultdict
from datetime import datetime

DIRARQ = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'arq')

# Leitura do meu arquivo CSV
pulls = pd.read_csv(os.path.join(DIRARQ, 'pulls.csv'))

# Conexão com o meu banco de dados SQLITE:
con = conectModel.conexaoSQLITE()


try:
    # Criando tabela pulls dentro do meu banco SQLITE
    pulls.to_sql('pulls', con, if_exists='fail')
except ValueError as erro:
    if str(erro) == "Table 'pulls' already exists.":
        print('A tabela "Pulls já esta criada no banco"')
        print('Continue....')
    else:
        print(f'Mensagem de erro: {erro}')

# Lendo uma consulta que esta dentro do pacote de "arq".consulta
dataFramePulls = pd.read_sql_query(cons.consultauser_login(), con)


dic = defaultdict(list)
for indice, linha in dataFramePulls.iterrows():
    dic[linha['user_login']] = {
        'Data': [],
        'Diferenca': []
    }


for indice, linha in dataFramePulls.iterrows():
    dic[linha['user_login']]['Data'].append(linha['updated_at'])

for chave, valor in dic.items():
    size = len(valor['Data']) - 1
    for x in range(size):
        dic[chave]['Diferenca'].append(
            abs(
                (datetime.strptime(dic[chave]['Data'][x+1][:10], '%Y-%m-%d').date() -
                 datetime.strptime(dic[chave]['Data'][x]
                                   [:10], '%Y-%m-%d').date()
                 ).days)
        )
    else:
        dic[chave]['Diferenca'].append(0)


for chave, valor in dic.items():
    df = pd.DataFrame(dic[chave])
    try:
        df.to_sql(chave, con)
    except ValueError as erro:
        if str(erro) == f"Table '{chave}' already exists.":
            print(f'A tabela "{chave}" já esta criada no banco"')
        else:
            print(f'Mensagem de erro: {erro}')
