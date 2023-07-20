
# ****************** INITIALISATION DU PROGRAMME ******************

from datetime import datetime
import pyodbc
#import sqlalchemy as msql
#import Connexion_bdd as Cx_bdd
import pandas as pd
import Requetes_SQL as Req_SQL
import matplotlib.pyplot as plt


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
        print('     F_SQL_Requete - OK')
   return df

def F_SQL_Execute(cnxn,requete_sql,pyodbc):
   import pandas as pd
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
        print('     F_SQL_Execution - OK')
   return 


#Initialisation des paramètres d'accès à la BDD Activité
try:
    #import pyodbc 
    server = '172.25.81.48'
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


# ***********************     STEP 1 - PREPROCESSING     ***************************

    # Define filters parameters for a list of NIP
    # For example :
        # Tous les patients qui ont eu un acte à l'ICO le 9 septembre 2020.

Table_Liste_Actes = 'A_Actes_ICO_2018_2021_TRIMED' #INCRIT EN DUR DANS LA PROC SQL !!
Table_Actes_filtres ='Tmp_Py_A_Actes_Export'
Site='2'
My_filter_date=datetime.strptime('09-06-2019', '%m-%d-%Y')

Requete = ' EXECUTE Delete_Table_if_exists ' + Table_Actes_filtres
Requete += ' EXECUTE Preproc_A0_Filter_NIP_BY_DATE_AND_SITE ' + Table_Liste_Actes + ','+ Table_Actes_filtres + ',' + str(My_filter_date.day) + ',' + str(My_filter_date.month) + ',' + str(My_filter_date.year) +',' + Site

#print(Requete)
print("STEP 1.1 : Filter NIP ON /n Site = " + Site + "/n Date = " + str(My_filter_date) + " - launched at " + str(datetime.now()))
F_SQL_Execute(cnxn,Requete,pyodbc)

#Mise en forme du dataset (Création J0V1234, Date_sejour)
print("STEP 1.2 : Prepare_Data_set - launched at " + str(datetime.now()))
Requete = ' EXECUTE Preproc_B1_Prepare_Dataset ' + Table_Actes_filtres + ',' + 'Listing_UF_V3' + ',' + 'YES' #Table acte / Table_UF / Summary YES -> just first 2000 lines
F_SQL_Execute(cnxn,Requete,pyodbc)

    #SE TROUVE A PRESENT DISPONIBLE DANS LA BDD 
# Listing_UF_V3
# Tmp_A_Actes_Table_Analyse - Base de donnée d'activité filtrée par le NIP dont les colonnes sont normalisées
    # ID_A / NIP / N_S / DD_A / DF_A / DD_M / HD_M / DF_M / HF_M / UFX / INX / R_NGAP / R_CCAM / UFH / Statut / Code_Equip / Site 
# Tmp_PS_
    # NIP / N_S / J0V1 / J0V2 / Poids_Sejour_DS / Poids_Sejour_DSOS 
# Tmp_Type_Sequence
    # NIP / id_Seq / N_S / J0V3 / J0V4 / Type_Sequence 


# ***********************     STEP 2 - LOADING DATASET OF PATIENTS     ***************************

#Definition des tables d'entrée
#Tble_Liste_Actes = Table_Actes_filtres
#Tble_UF ='Table_UF'
Requete = Req_SQL.Req_Export_Acte

df=F_SQL_Requete(cnxn,Requete,pyodbc)
print(df)

fig, axs = plt.subplots(1, 3, figsize=(16, 6))
df.scatterplot(data=df, x='num_pages', y='average_rating', ax=axs[2])

plt.show()