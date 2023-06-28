import pyodbc
#import sqlalchemy as msql
import Connexion_bdd as Cx_bdd
import pandas as pd

def F_SQL_Requete(cnxn,requete_sql,pyodbc):
   import pandas as pd
   try:
        df = pd.read_sql(requete_sql, cnxn)
        #cursor.execute(requete_sql) # ATTENTION SI ERREUR, le SERVEUR SQL plante!!  VOIR https://www.mytecbits.com/internet/python/execute-sql-server-stored-procedure
        #cnxn.commit()      
   except pyodbc.Error as ex:
           sqlstate = ex.args[1]
           sqlstate = sqlstate.split(".")
           print('F_SQL_Requete - ERROR')
           print(sqlstate)                
   else :
        print('F_SQL_Requete - OK')
   return df



#Initialisation des paramètres d'accès à la BDD Activité
try:
    #import pyodbc 
    server = '192.168.62.244'
    database = 'ICO_Activite'
    username = 'DSTI_User' 
    password = '`wHRcQ@A*h&CmJ7q>Hz'
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';ENCRYPT=no;UID='+username+';PWD='+ password)
    #engine=msql.create_engine("mssql+pyodbc://" + username + ":" + password +"@" + server +"/" + database ,echo=True)
    #cnxn = msql.Connection(engine,)
    #cursor = cnxn.cursor()

except pyodbc.Error as ex:
    sqlstate = ex.args[1]
    sqlstate = sqlstate.split(".")
    print(sqlstate[-3])
else :
    print('OK')


#PREPROCESSING

#Definition des tables d'entrée

Tble_Liste_Actes = 'Table_Act'
Tble_UF ='Table_UF'

Requete = 'SELECT TOP (1000) * FROM [ICO_Activite].[dbo].[A_Actes_ICO_2018_2021]'

df=F_SQL_Requete(cnxn,Requete,pyodbc)

print(df)