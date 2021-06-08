from sqlalchemy import create_engine
import pandas as pd
import os

DIRDEFAULT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BANCODADOS = 'meubanco.db'


def conexaoSQLITE():
    strconexao = f'sqlite:///{os.path.join(DIRDEFAULT, BANCODADOS)}'
    engine = create_engine(strconexao, echo=False)
    return engine.connect()
