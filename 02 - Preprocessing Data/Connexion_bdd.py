def F_SQL_execute(cnxn,cursor,requete_sql,pyodbc): # fonction d'initialisation de la connexion de la BDD
    try:
        cursor.execute(requete_sql) # ATTENTION SI ERREUR, le SERVEUR SQL plante!!  VOIR https://www.mytecbits.com/internet/python/execute-sql-server-stored-procedure
        cnxn.commit()      
    except pyodbc.Error as ex:
           sqlstate = ex.args[1]
           sqlstate = sqlstate.split(".")
           print('F_sql_execute - ERROR')
           print(sqlstate)                
    else :
        print('F_sql_execute - OK')
    return 

def F_SQL_Requete(cnxn,cursor,requete_sql,pyodbc):
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

def F_SQL_Fermeture_Connexion(cnxn,cursor,pyodbc): # fonction de fermeture des connexions à la BDD
    try:
        cursor.close()
        del cursor
        cnxn.close()
    except pyodbc.Error as ex:
           sqlstate = ex.args[1]
           sqlstate = sqlstate.split(".")
           print('F_SQL_Fermeture_Connexion - ERROR')
           print(sqlstate)        
    else: 
        print('F_SQL_Fermeture_Connexion - OK')
    return


