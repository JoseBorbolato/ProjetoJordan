from sqlalchemy import create_engine
import pandas as pd
import os

DIRDEFAULT = os.path.dirname(os.path.abspath(__file__))
DIRARQ = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'arq')
BANCODADOS = 'meubanco.db'
strconexao = f'sqlite:///{os.path.join(DIRDEFAULT, BANCODADOS)}'

engine = create_engine(strconexao, echo=True)
engine.connect()

pulls = pd.read_csv(os.path.join(DIRARQ, 'pulls.csv'))
pulls.to_sql('pulls', engine)


selecionados = pd.read_sql_query(
    '''
    select
        a.user_login,
        a.created_at,
        a.updated_at,
        (julianday(date('now')) - julianday(a.updated_at)) as diferenca
    from pulls a
    where
        a.user_login in (
            'tobiasdiez',
            'koppor',
            'matthiasgeiger',
            'Siedlerchr',
            'simonharrer',
            'mortenalver',
            'oscargus'
    )order by user_login, a.updated_at
    ''', engine
)

print(selecionados)
