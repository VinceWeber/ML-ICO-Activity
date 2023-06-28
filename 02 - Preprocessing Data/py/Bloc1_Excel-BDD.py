# -*- coding: utf-8 -*-
"""
Created on Fri Aug 19 14:31:59 2022
@author: v-weber
"""

#https://python.sdv.univ-paris-diderot.fr/   "Bonnes pratiques de code en python"


#"Import des modules complémentaires"
import math    
import csv  #Lecture / Ecriture des fichiers .csv
import os #Gestion des fichier et chemins - https://python.doctor/page-gestion-fichiers-dossiers-python  
import Module_Fonctions_Principales_Import as MFP # module des fonctions d'import de BDD
import pandas as pd #module panda gestion des dataframes et BDD https://pandas.pydata.org/
import datetime

#Import des modules "maison"


#Définition des constantes


#Définition des variables principales


""" VARIABLES POUR LECTURE ET INTEGRATION DU FICHIER CSV DANS LA BDD"""
Chemin_DIM_TBD='\\\\NTES-ETL.crg.fr\\ETL\\extracts\\DIM_TDB' #Chemin de réseau où sont localisé les fichiers d'activité
#Chemin_Local_TBD='C:\\Users\\v-weber\\Documents\\99-Perso\\Analyses statistiques\\2019-2021\\Exports csv - CERNER\\test'
Chemin_Local_TBD='C:\\Users\\v-weber\\Documents\\99-Perso\\Analyses statistiques\\Activité ICO\\Python\\Import Activite'


Char_Delimiter=';'
Nom_fichier_ACCAM='exportCernerACCAM_Test_50_lignes.csv'
Nom_fichier_NCCAM='exportCernerANGAP_Test_50_lignes.csv'
Nom_fichier_ANGAP='exportCernerNCCAM_Test_50_lignes.csv'
Nom_fichier_NNGAP='exportCernerNNGAP_Test_50_lignes.csv'
Nom_fichier_AMVT='MVT-ANG-2019-2021_Test_50_lignes.csv'
Nom_fichier_NMVT='MVT-SH-2019-2021_Test_50_Lignes.csv'


""" Structure de la variable de lecture du fichier CSV
Var_Lecture_csv=[ligne,LI_DATE,PA_NIP,HO_NUM,STATUT,SEANCE,UFXCODE,UFXCODE_LIB,INXCODE,INXCODE_NOM,INXCODE_SPE,CDAM_GRAT,CDAM_UNITE,AC_REF,AC_LIB,AC_ACTI,AC_ASSOC,MODIF1,MODIF2,MODIF3,PRIX_ACTE,COUT_ACTE,MOIS,AN,HO_RECODE]
Var_Lecture_csv=[0,]

"""

""" -> exemple de tête de fichier ACCAM
     LI_DATE PA_NIP	HO_NUM	STATUT	SEANCE	UFXCODE	UFXCODE_LIB	INXCODE	INXCODE_NOM	INXCODE_SPE	CDAM_GRAT	CDAM_UNITE	AC_REF	AC_LIB	AC_ACTI	AC_ASSOC	MODIF1	MODIF2	MODIF3	PRIX_ACTE	COUT_ACTE	MOIS	AN	HO_RECODE
"""

#Définition des fonctions principales (+/- externalisable)


#Corps du programme

#---------------------------------------------------------------------------------------------------------------------------------------------------
#ETAPE 0 - Initialisation des paramètres d'accès à la BDD Activité
print('-----------------------------')
print('-> Initialisation connexion BDD')

try:
    import pyodbc 
    server = 'ANG-APP-BDD-01.paulpapin.loc\APPBDD01' 
    database = 'ICOActivite' 
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes;')
    cursor = cnxn.cursor()

except pyodbc.Error as ex:
    sqlstate = ex.args[1]
    sqlstate = sqlstate.split(".")
    print(sqlstate[-3])
else :
    print('OK')

#---------------------------------------------------------------------------------------------------------------------------------------------------
""" Intégration des fichiers :
    si Nom_fichier ="" -> pas d'import souhaité'
    
    Principe :
        Pour chaque ligne du fichier .csv
        Vérifier qu'il n'existe pas un doublon dans la base
            Si pas de doublon - Ajouter la ligne dans la base
            Si doublon - signaler qu'un doublon existe et ne pas importer'      
        
        Version 2 : importer l'ensemble du fichier Csv dans une table provisoire, comparer les versions puis générer une nouvelle table
                    d'activité ave cles actes nouveaux, supprimés et inchangés'            
            
            Table_Activite = Activite connue
            Table_Activite_Nouvelle = Nouvel import
            
"""
#Création d'une table d'import vide dans la bdd ( A compléter / corriger)

print('-> Execution requete SQL')
requete_sql1="""CREATE TABLE ZAZ_UTILISATEUR(id INT)"""
requete_sql2="""DROP TABLE [dbo].[ZAZ_UTILISATEUR]"""

#MFP.F_SQL_execute(cnxn,cursor,requete_sql2,pyodbc)

                                                   # https://python.doctor/page-database-data-base-donnees-query-sql-mysql-postgre-sqlite



#ETAPE 1 - Fichier CCAM Angers
print('-----------------------------')
    #Creation table d'import provisoire

#requete_sql=MFP.F_SQL_Creation_table_import('Z_TEMP_NEW')
#MFP.F_SQL_execute(cnxn,cursor,requete_sql,pyodbc)
    

#print('-> Lecture Fichier CCAM Angers') 

                        """ Exemple de code pour afficher le contenu du fichier .csv
                            with open(Chemin_Local_TBD + '\\' + Nom_fichier, newline='') as csvfile:
                                reader = csv.DictReader(csvfile,delimiter=Char_Delimiter)
                                for row in reader:
                                    print(row)
                                    print(row['PA_NIP'])
                        """
                        """
                        with open(Chemin_Local_TBD + '\\' + Nom_fichier_ACCAM, newline='') as csvfile:
                            reader = csv.DictReader(csvfile,delimiter=Char_Delimiter)
                            for row in reader:
                                print(row)
                                print(reader.line_num)
                        """
                        """
                        
                        """
#Import du fichier .csv dans un dataframe
Chemin_fich=Chemin_Local_TBD + '\\' + Nom_fichier_ACCAM
print(Chemin_fich)

dataACCAM=pd.read_csv(Chemin_fich,encoding='ISO-8859-1',sep=';') #lecteur du fichier .csv
df=pd.DataFrame(dataACCAM) #Intégration des données dans un dataframe panda

#Definition des types de données du fichier.csv
#pd.to_datetime(df['LI_DATE'], dayfirst=true)
#df['LI_DATE']=pd.to_datetime(df['LI_DATE'],infer_datetime_format=False)
df['LI_DATE'] = pd.to_datetime(df['LI_DATE'], dayfirst=True)

df = df.astype({'PA_NIP':'string','HO_NUM':'string','STATUT':'string','SEANCE':'string','UFXCODE':'string', \
                'UFXCODE_LIB':'string','UFXCODE_LIB':'string','INXCODE':'string','INXCODE_NOM':'string',\
                'INXCODE_SPE':'string','CDAM_GRAT':'string','CDAM_UNITE':'string','AC_REF':'string',\
                'AC_LIB':'string','AC_ACTI':'string','AC_ASSOC':'string','MODIF1':'string','MODIF2':'string',\
                'MODIF3':'string','MOIS':'string','AN':'string','HO_RECODE':'string'})
#print(df)

#print('-> Liste des entête du fichier CCAM.csv')
#print(df.dtypes)

#ZONE OU L'ON PEUT AJOUTER DES TRANSFORMATIONS DE DONNEES ex : filtrage de la date de l'acte, suppression des espaces 
#print('-> modification du df')
df['PA_NIP']=df.apply(lambda x: 'A' + x['PA_NIP'],axis=1)
df['HO_NUM']=df.apply(lambda x: 'A' + x['HO_NUM'],axis=1)
df['INXCODE']=df.apply(lambda x: x['INXCODE'].replace(" ", ""),axis=1)
df['HO_RECODE']=df.apply(lambda x: x['HO_RECODE'].replace(" ", ""),axis=1)

#print(df)
#df.loc[row,'AC_ASSOC'] -> Si NULL -> "" , SI Not NULL = même valeur

#affiche la longueur du fichier .csv
chaine_txt= "Nombre de lignes du fichier : " + Chemin_fich + " est de " + str(len(df.index))
print(chaine_txt)


""" Requete insertion du fichier CSV sur le serveur SQL"""

#Creation de la table de d'import (suppression  + création )
requete_sql=MFP.F_SQL_Drop_table('ZZ_TEMP')
MFP.F_SQL_execute(cnxn,cursor,requete_sql,pyodbc)

requete_sql=MFP.F_SQL_Creation_table_import('ZZ_TEMP')
MFP.F_SQL_execute(cnxn,cursor,requete_sql,pyodbc)

#Import des données du .csv dans une table d'import sur le serveur sql
MFP.F_SQL_Import_DF_dans_Table_Import('ZZ_TEMP',df,cnxn,cursor,pyodbc)


"""
print(len(df.index))
for row in range(len(df.index)):
    print(df.loc[row,'LI_DATE'])
"""

#---------------------------------------------------------------------------------------------------------------------------------------------------
#ETAPE 2 - Fichier NGAP Angers
#---------------------------------------------------------------------------------------------------------------------------------------------------
#ETAPE 3 - Fichier MVT Angers
#---------------------------------------------------------------------------------------------------------------------------------------------------
#ETAPE 4 - Fichier CCAM Saint Herblain
#---------------------------------------------------------------------------------------------------------------------------------------------------
#ETAPE 5 - Fichier NGAP Saint Herblain
#---------------------------------------------------------------------------------------------------------------------------------------------------
#ETAPE 6 - Fichier MVT Saint Herblain

print('-----------------------------')
print('-> Liste des tables de la bdd')
#MFP.F_SQL_Liste_tables(cnxn, cursor, pyodbc)

# Fermeture des connexions BDD
print('-> Femeture des connexions bdd')
MFP.F_SQL_Fermeture_Connexion(cnxn, cursor, pyodbc)

