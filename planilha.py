from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from oauth2client.service_account import ServiceAccountCredentials
from mysql.connector import Error
import mysql.connector
import gspread, re
import pandas as pd

sched = BlockingScheduler()

@sched.scheduled_job('interval', minutes = 60*12)
def update():
     
    print('funcionando...')
        
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)

    # Find a workbook by name and open the first sheet
    # Make sure you use the right name here.
    sheet = client.open("Expressões-Chaves").sheet1

    # Extract the values and transform in dictionary
    list_of_hashes = sheet.get_all_records()

    df = pd.DataFrame(list_of_hashes)
    
    dict_expressoes = {}
    for column in df['Expressões-Chaves']:
        
        linha = df.loc[df['Expressões-Chaves'] == column][['Variações']].values[0][0].split(',')
        dict_expressoes[column] = linha
    
    
    try:
        con = mysql.connector.connect(host="us-cdbr-east-03.cleardb.com", user="b8cd7b592349c7", passwd="67006c83", db="heroku_0c2e37b2ea952c5")
        #con = MySQLdb.connect(host="host", user="user", passwd="passwd", db="db")
        cursor = con.cursor()
        cursor.execute("SELECT * FROM expressoeschaves")
        
        print("ESTE É A TABELA NO BANCO DE DADOS")
        print("expressoes_chaves --------- variacao")
        for i in cursor.fetchall():
            print(i)
            
        for expressao_chave in dict_expressoes:
            for variacoes in dict_expressoes[expressao_chave]:
                #print(f'{expressao_chave}: {variacoes}')
                
                try:
                    
                    comando_inserir = "INSERT INTO expressoeschaves (expressoes_chaves, variacao) VALUE ('{0}', '{1}')".format(expressao_chave, variacoes)
                    cursor.execute(comando_inserir)
                    con.commit()
                    print("expressao chave: ({0}) adicionada".format(variacoes))
                           
                except:
                    
                    comando_update = "UPDATE expressoeschaves SET expressoes_chaves='{0}' WHERE variacao='{1}'".format(expressao_chave, variacoes.replace(' ', ''))
                    cursor.execute(comando_update)
                    con.commit()
                    print("expressao chave: ({0}) atualizada".format(variacoes))
                    
    
    except Error as e:
        print("Erro ao acessar tabela MySQL", e) 
    
    finally:
        if (con.is_connected()):
            con.close()
            cursor.close()
            print("Conexão ao MySQL encerrada")

    print('terminando...')        
    
    

if __name__ == '__main__':
    
    sched.start()

