import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import os
import sys
from icecream import ic
from datetime import datetime, timedelta
from conexao_denodo import conexao_denodo as conn

pd.options.mode.chained_assignment = None  # default='warn'
pd.options.display.max_columns = None
pd.options.display.float_format = '{:,.2f}'.format

user = os.getlogin()
bases_parquet = rf"C:\Users\{user}\Sicredi\TimeBI_0730 - Documentos\_BASES\arquivos_parquet"
bases_excel = rf"C:\Users\{user}\Sicredi\TimeBI_0730 - Documentos\_BASES"

### Datas
anomes_atual = datetime.today().strftime("%Y%m")


### Funções utilitárias
def ler_parquet(nome_arquivo: str) -> pd.DataFrame:
    return pq.read_table(os.path.join(bases_parquet, nome_arquivo+".parquet")).to_pandas(safe=False)


def ler_excel(nome_arquivo: str, nome_sheet: str = None) -> pd.DataFrame:
    return pd.read_excel(bases_excel + "\\" + nome_arquivo+".xlsx", sheet_name=nome_sheet) 


def tratar_conta(df: pd.DataFrame, coluna: str) -> pd.DataFrame:
    df[coluna] = df['conta_principal'].astype(str).str.replace("-", "").str.zfill(6)
    return df