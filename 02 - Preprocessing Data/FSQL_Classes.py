
#Module dedicated for SQL Functions queries

import pyodbc

def F_SQL_Requete(cnxn,requete_sql,pyodbc,output=None):
   import warnings
   warnings.filterwarnings('ignore')

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
        if output:
             print('     F_SQL_Requete - OK')
   return df

def F_SQL_Execute(cnxn,requete_sql,pyodbc,output=None):
   import pandas as pd
   import warnings
   warnings.filterwarnings('ignore')
   try:
        #df = pd.read_sql(requete_sql, cnxn)
        cursor.execute(requete_sql) # ATTENTION SI ERREUR, le SERVEUR SQL plante!!  VOIR https://www.mytecbits.com/internet/python/execute-sql-server-stored-procedure
        cnxn.commit()      
   except pyodbc.Error as ex:
           sqlstate = ex.args[1]
           sqlstate = sqlstate.split(".")
           print('F_SQL_Execution - ERROR')
           print(sqlstate)                
   else :
        if output:
          print('     F_SQL_Execution - OK')
   return 



#Initialisation des paramètres d'accès à la BDD Activité
try:
    #import pyodbc 
    server = '192.168.166.198'
    database = 'ICO_Activite'
    username = 'sa' 
    password = 'vyNM~pgDxO>0[5+ryM>F'
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';ENCRYPT=no;UID='+username+';PWD='+ password)
    #engine=msql.create_engine("mssql+pyodbc://" + username + ":" + password +"@" + server +"/" + database ,echo=True)
    #cnxn = msql.Connection(engine,)
    cursor = cnxn.cursor()

except pyodbc.Error as ex:
    sqlstate = ex.args[1]
    sqlstate = sqlstate.split(".")
    print(sqlstate[-3])
else :
    print('Connexion BDD - OK')