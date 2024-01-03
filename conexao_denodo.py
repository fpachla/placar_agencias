import pyodbc as dbdriver
import pandas as pd
import numpy as np
from datetime import datetime
from socket import gethostname
from urllib.request import Request, urlopen  # Python 3
import warnings
import traceback
import time
import shutil
import os
import json
warnings.simplefilter("ignore")
from urllib import parse
from sqlalchemy import create_engine
from threading import Thread
import queue


class conexao_denodo():
    
    #não está sendo usado
    def SetDBConn(self, database:str="ldw"):
        ''' 
        Seta uma conexão com o DenodoODBC utilizando as credenciais do DenodoODBC Driver 
        '''
        self.denodoserver_dsn = "DenodoODBC"
        self.client_hostname = gethostname()
        # self.useragent = "%s-%s" % (dbdriver.__name__, self.client_hostname)
        # self.cnxn = dbdriver.connect("DSN=%s;UserAgent=%s" %
        #                         (self.denodoserver_dsn, self.useragent))
        
        
        
        
        self.useragent = "%s-%s" % (dbdriver.__name__, self.client_hostname)
        self.cnxn = dbdriver.connect("DATABASE=%s;SERVER=virtualizador.sicredi.net;PORT=9996;DSN=%s;UserAgent=%s" % (database,self.denodoserver_dsn, self.useragent))
        
        
        return self.cnxn
    
    
    
    def baixar_consulta_denodoobdc(query, database="ldw"):
        '''  databases ['ldw','seguros','credito']
            '''
        con = conexao_denodo().SetDBConn(database=database)
                
        # query =  '''
        #             SELECT * FROM ag_cart
        #             WHERE ano_mes in (202303, 202304)
        #             '''
                
        df = pd.read_sql(query, con)
        return df
        
        
    def baixar_consulta_manual(query:str, database:str='ldw', **kwargs):
      ''' 
      Esta função recebe uma string para uma consulta sql e retorna um pd dataframe da consulta
      **kwargs são repassadados para os 
      '''
      try: 
            #usa o SQL Alchemy
            conn = Denodo(database=database).set_engine()
            
            # conn = set_db_conn()
            result = pd.DataFrame()
            for chunk in pd.read_sql_query(query, conn, chunksize=5000, **kwargs):
                  result = pd.concat([result, chunk], ignore_index=True)
      except (UnicodeDecodeError, pd.io.sql.DatabaseError) as e: 
            print('\tErro ao baixar a consulta')
            conn.dispose()
            pass
      if conn and isinstance(conn, Denodo): conn.dispose()
      return result
  
  
    def baixar_consulta_manual_com_timeout(query:str, database:str='ldw', timeout=1000, **kwargs):
        ''' Este método usa timeout, se a consulta demorar mais que X, encerramos o processo.
        \n Esta demanda surgiu de consultas que ficam em execução e não encerram durante a tarde inteira.'''
        def run_query(q):
            try:
                conn = Denodo(database=database).set_engine()
                result = pd.DataFrame()
                for chunk in pd.read_sql_query(query, conn, chunksize=5000, **kwargs):
                    result = pd.concat([result, chunk], ignore_index=True)
                q.put(result)
            except (UnicodeDecodeError, pd.io.sql.DatabaseError) as e:
                print('\tErro ao baixar a consulta')
            if conn and isinstance(conn, Denodo): conn.dispose()

        q = queue.Queue()
        t = Thread(target=run_query, args=(q,))
        t.start()
        t.join(timeout)  # Substitua X pelo número de segundos

        if t.is_alive():
            raise RuntimeError("A consulta demorou muito para ser executada e foi interrompida.")
            # print("A consulta demorou muito para ser executada e foi interrompida.")
            # return pd.DataFrame()

        return q.get()



    def baixar_consulta_manual_v2(query: str, database: str = 'ldw', chunksize=5000, max_retries=3, **kwargs):
        """
        Esta função recebe uma string para uma consulta SQL e retorna um pd.DataFrame da consulta.
        **kwargs são repassados para os
        """
        try:
            # Usa o SQL Alchemy
            conn = Denodo().set_engine()

            # Aumente o tempo limite da consulta, se necessário (verifique a documentação para o valor correto)
            conn.execution_options(timeout=300)  # Aumenta o tempo limite para 300 segundos

            result = pd.DataFrame()
            retries = 0
            while retries < max_retries:
                try:
                    for chunk in pd.read_sql_query(query, conn, chunksize=chunksize, **kwargs):
                        result = pd.concat([result, chunk], ignore_index=True)
                    break
                except (UnicodeDecodeError, pd.io.sql.DatabaseError) as e:
                    print('\tErro ao baixar a consulta. Tentando novamente...')
                    retries += 1
                    if retries >= max_retries:
                        print('\tFalha após várias tentativas. Abortando.')
                        raise e
        except Exception as e:
            print(f'\tErro: {e}')
            result = None

        if conn and isinstance(conn, Denodo):
            conn.dispose()
        return result


class Denodo():
      def __init__(self, database: str = "ldw"):
            conf = get_config('DENODO')
            host = conf['host']
            user = conf['user']
            db = database
            self.url = f'denodo://%s:%s@%s:%s/%s' % (parse.quote_plus(conf['user']), parse.quote_plus(conf['password']),
                                                      parse.quote_plus(conf['host']), parse.quote_plus(conf['port']), parse.quote_plus(db))

      def set_engine(self):
            return create_engine(self.url)
    
    
def get_config(key:str):
    user = os.getlogin()
    filename_configuration = rf"C:\Users\{user}\config.json"
    
    dirname = os.path.dirname(__file__)
    
    # if len(sys.argv) > 1:
    #     filename_configuration = sys.argv[1]
        
    file_path = os.path.join(dirname, filename_configuration)
    
    try:
        with open(file_path) as json_orch:
            config = json.load(json_orch)
            return json.loads(json.dumps(config[key]))
    except Exception as e:
        #log.c_log_info(str(e), colorama.Fore.RED)
        return None