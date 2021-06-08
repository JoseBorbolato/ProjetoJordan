import os
import pandas as pd
from pandas.core.frame import DataFrame
import funcoes.conexao as conectModel
import arq.consulta as cons

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

for indice, linha in dataFramePulls.iterrows():
    print(
        ' user_login->', linha['user_login'],
        ' created_at->', linha['created_at'],
        ' updated_at->', linha['updated_at'],
        ' diferenca->', linha['diferenca']
    )
