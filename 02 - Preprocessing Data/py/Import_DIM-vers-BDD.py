# -*- coding: utf-8 -*-
"""
Created on 13/09/2022

Script python d'analyse du process de RDV d'imagerie scanner

Module de visualisation du parcours faisant apparaitre les éléments suivants :

A partir des activités de consultations RDTH / CHIR / ONCO 

Vérifier si il y a eu un examen de scanner entre cette CS et la précédente
    cet examen aura été réalisé à l'ICO :
            -> Trace dans l''activité CCAM (horodatée)
            -> Demande de RDV confirmée + patient accueilli (horodatée)
    cet examen aura été réalisé à l'extérieur :
            -> CR d'examen Scanner EXT présent dans le DPI' (horodatée) + DATE DE CREATION ??
            -> Demande de RDV annulée


@author: v-weber """

#https://python.sdv.univ-paris-diderot.fr/   "Bonnes pratiques de code en python"


#"Import des modules complémentaires"
import math    
import csv  #Lecture / Ecriture des fichiers .csv
import os #Gestion des fichier et chemins - https://python.doctor/page-gestion-fichiers-dossiers-python  
import Module_Fonctions_Principales_Import as MFP # module des fonctions d'import de BDD
import pandas as pd #module panda gestion des dataframes et BDD https://pandas.pydata.org/
import datetime
import matplotlib.pyplot as plt
import numpy as np

#Import des modules "maison"


#Définition des constantes


#Définition des variables principales


""" VARIABLES POUR LECTURE ET INTEGRATION DU FICHIER CSV DANS LA BDD"""
Chemin_DIM_TBD='\\\\NTES-ETL.crg.fr\\ETL\\extracts\\DIM_TDB' #Chemin de réseau où sont localisé les fichiers d'activité
#Chemin_Local_TBD='C:\\Users\\v-weber\\Documents\\99-Perso\\Analyses statistiques\\2019-2021\\Exports csv - CERNER\\test'
Chemin_Local_TBD='C:\\Users\\v-weber\\Documents\\99-Perso\\Analyses statistiques\\Activité ICO\\Python\\Analyse RDV exterieurs SH'


Char_Delimiter=';'
Nom_fichier_ACCAM='exportCernerACCAM_Test_50_lignes.csv'
Nom_fichier_Acti='Sortie_BDD_Activite.csv'
Nom_fichier_NCCAM='exportCernerNCCAM2022.csv'  #exportCernerNCCAM2022.csv
#Nom_fichier_ANGAP='exportCernerNCCAM_Test_50_lignes.csv'
#Nom_fichier_NNGAP='exportCernerNNGAP_Test_50_lignes.csv'
#Nom_fichier_AMVT='MVT-ANG-2019-2021_Test_50_lignes.csv'
#Nom_fichier_NMVT='MVT-SH-2019-2021_Test_50_Lignes.csv'
Nom_Fichier_DPI_CR_Ext='CR_Exterieurs.csv'   #CR_Exterieurs.csv
Nom_Fichier_LOG_RDV='Log_RDV_IMA_SH_2021-2022_v3.csv' #Log_RDV_IMA_SH_2021-2022.csv

#filtre de la periode à étudier
Date_Borne_inf=datetime.datetime(2015,1,1)
Date_Borne_sup=datetime.datetime(2024,1,1)

#-filtre nb de patients pour tester le process
NB_Patients_filtre=1E20



""" Structure de la variable de lecture du fichier CSV
Var_Lecture_csv=[ligne,LI_DATE,PA_NIP,HO_NUM,STATUT,SEANCE,UFXCODE,UFXCODE_LIB,INXCODE,INXCODE_NOM,INXCODE_SPE,CDAM_GRAT,CDAM_UNITE,AC_REF,AC_LIB,AC_ACTI,AC_ASSOC,MODIF1,MODIF2,MODIF3,PRIX_ACTE,COUT_ACTE,MOIS,AN,HO_RECODE]
Var_Lecture_csv=[0,]

"""

""" -> exemple de tête de fichier ACCAM
     LI_DATE PA_NIP	HO_NUM	STATUT	SEANCE	UFXCODE	UFXCODE_LIB	INXCODE	INXCODE_NOM	INXCODE_SPE	CDAM_GRAT	CDAM_UNITE	AC_REF	AC_LIB	AC_ACTI	AC_ASSOC	MODIF1	MODIF2	MODIF3	PRIX_ACTE	COUT_ACTE	MOIS	AN	HO_RECODE
"""

#Définition des fonctions principales (+/- externalisable)


#Corps du programme

#---------------------------------------------------------------------------------
#IMPORT DES DONNEES
#---------------------------------------------------------------------------------------------------------------------------------------------------
#ETAPE 0 - Initialisation des paramètres d'accès à la BDD Activité
print('-----------------------------')
"""
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
"""
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

#IMPORT DES DONNEES DEPUIS LA BASE SQL ACTIVITE
"""
requete = 'SELECT [id_acte]
    ,[Site]
    ,[NIP]
    ,[Ddebsej]
    ,[J_Parcours_V1]
    ,[J_Parcours_V2]
    ,[J_Parcours_V3]
    ,[J_Parcours_V4]
    ,[Séquence_Parcours]
    ,[INX_Code_Lib_refx]
    ,[INX_Code_Spe_refx]
    ,[UFX_Code]
    ,[UFX_Code_Lib]
    ,[Service]
    ,[Activite]
    ,[Phase Parcours]
FROM [ICOActivite].[dbo].[A_DataSet_Global]
WHERE [Ddebsej]> '2021-01-01' and ( [Activite] like 'Scanner' or [Activite] like 'Consultations' ) and site =2'
print(requete)
df_NCCAM = pd.read_sql(requete, cnxn)
#df_NCCAM = MFP.F_SQL_Requete(cnxn,cursor,requete,pyodbc)
print(df_NCCAM.head(26))
"""

#IMPORT DES DONNEES DEPUIS LES FICHIERS CSV

#ETAPE0 - Fichier csv d'une requete SQL de la base activité
print(' ')
print('---Lecture Fichier csv BDD_Activité')
df_Acti = MFP.F_Acti_csv_to_panda_df(Nom_fichier_Acti, Chemin_Local_TBD) #lecture du fichier .csv vers un dataframe panda



#ETAPE 1 - Fichier CCAM Angers
print(' ')
print('---Lecture Fichier csv ACCAM')
#df_ACCAM = MFP.F_ACCAM_csv_to_panda_df(Nom_fichier_ACCAM, Chemin_Local_TBD) #lecture du fichier .csv vers un dataframe panda
print('---Ecriture Dataframe ACCAM vers SQL')
#MFP.P_df_to_SQL ('ZZ_TEMP2',df_ACCAM,cnxn,cursor,pyodbc,'CCAM') #Ecriture du df panda sur la base sql


#---------------------------------------------------------------------------------------------------------------------------------------------------
#ETAPE 2 - Fichier NGAP Angers
#---------------------------------------------------------------------------------------------------------------------------------------------------
#ETAPE 3 - Fichier MVT Angers
#---------------------------------------------------------------------------------------------------------------------------------------------------
#ETAPE 4 - Fichier CCAM Saint Herblain
print(' ')
print('---Lecture Fichier csv NCCAM')
#df_NCCAM = MFP.F_NCCAM_csv_to_panda_df(Nom_fichier_NCCAM, Chemin_Local_TBD) #lecture du fichier .csv vers un dataframe panda
#print(df_NCCAM)
print('---Ecriture Dataframe NCCAM vers SQL')
#MFP.P_df_to_SQL ('ZZ_TEMP3',df_NCCAM,cnxn,cursor,pyodbc,'CCAM')  #Ecriture du df panda sur la base sql


#---------------------------------------------------------------------------------------------------------------------------------------------------
#ETAPE 5 - Fichier NGAP Saint Herblain
#---------------------------------------------------------------------------------------------------------------------------------------------------
#ETAPE 6 - Fichier MVT Saint Herblain
#---------------------------------------------------------------------------------------------------------------------------------------------------
#ETAPE 7 - Fichier CR Exterieurs
print(' ')
print('---Lecture Fichier csv CR - bisite')
df_DPI_CR_EXt = MFP.F_CR_Ext_csv_to_panda_df(Nom_Fichier_DPI_CR_Ext, Chemin_Local_TBD) #lecture du fichier .csv vers un dataframe panda
#print(df_DPI_CR_EXt)
print('---Ecriture Dataframe CR - bisite vers SQL')
#MFP.P_df_to_SQL ('ZZ_TEMP4',df_DPI_CR_EXt,cnxn,cursor,pyodbc,'CR')  #Ecriture du df panda sur la base sql


#--------------------------------------------------------------------------
# MODIF NIP post IMS avec A et N suivant données d'activité
'le NIP ICO 202209048 sera transformé en A202209048 si il provient d une activité ANGERS et N si NANTES'

""" Pour tous les NIP ICO types [202209048], vérifier dans DF CCAM s'il existe un NIP equivalent, puis ajouter A ou N"""

#♦---------------------------------------------------------------------------------------------------------------------------------------------------
#ETAPE 8 - Fichier LOG_RDV
df_DPI_LOG_RDV = MFP.F_LOG_RDV_csv_to_panda_df(Nom_Fichier_LOG_RDV, Chemin_Local_TBD) #lecture du fichier .csv vers un dataframe panda
print(df_DPI_LOG_RDV['NIP'])
#   traiter le champ horaire pour le convertir en datetime


print('-----------------------------')
print('-> Liste des tables de la bdd')
#MFP.F_SQL_Liste_tables(cnxn, cursor, pyodbc)



# Fermeture des connexions BDD
print('-> Femeture des connexions bdd -DESACTIVE !!')
#MFP.F_SQL_Fermeture_Connexion(cnxn, cursor, pyodbc)



#---------------------------------------------------------------------------------
#ANALYSE DES DONNEES
#---------------------------------------------------------------------------------


#print(df_circuit_RDV)

#identifier toutes les actes de consultations et de scanner (NIP, DATE ACTE, UFX_EXE = CST ou SCANNER_ICO , SOURCE=CCAM)
# ajoute la colonne 'SOURCE' 


#df_CCAM_mask_scanner=df_NCCAM['UFXCODE']=='9061' #ATTENTION CRITER NON EXHAUSTIF
#df_CCAM_mask_cs_rdth=df_NCCAM['UFXCODE']=='9050'
#df_CCAM_mask=df_NCCAM.query('UFXCODE_LIB.str.contains("SCANNER") or UFXCODE_LIB.str.contains("CONSUL")', engine='python') #Filtre déjà initié dans la requete de la base activité
df_CCAM_mask=df_Acti

#=df_NCCAM[df_CCAM_mask_scanner]
#df2=df_NCCAM[df_CCAM_mask_cs_rdth]

#frames = [df1, df2]
#result = pd.concat(frames)

#print(result['PA_NIP'])
print('Liste des NIP ayant eu eune activité de consult ou scanner dans le fichier CCAM')
print(df_CCAM_mask)

#identifier tous les CR_EXT pour les patients identifiés dans la requete CCAM avec un libellé contenant "SCANNER" (NIP,DATE ACTE_RDV, "RDV " +  lib statut (attribué, accueilli, annulé) , SOURCE=LOG_RDV)


#print(df_DPI_CR_EXt)
df_DPI_CR_mask2=df_DPI_CR_EXt[df_DPI_CR_EXt.NIP.isin(df_CCAM_mask['NIP'])]
df_DPI_CR_mask=df_DPI_CR_mask2.query('LIB_DOC_FIC.str.contains("SCANNER")', engine='python')
print('Liste des CR des patients identifiés dans le fichier CCAM ayant un CR_EXT dans le DPI')
print(df_DPI_CR_mask)

#identifier tous les RDV pour les patients identifiés dans la requete CCAM avec un libellé contenant "SCANNER" (NIP,DATE ACTE_RDV, "RDV " +  lib statut (attribué, accueilli, annulé) , SOURCE=LOG_RDV)

print('Liste des CR des patients identifiés dans le fichier CCAM ayant un RDV dans le log RDV_DPI')
df_DPI_LOG_RDV_mask2=df_DPI_LOG_RDV[df_DPI_LOG_RDV.NIP.isin(df_CCAM_mask['NIP'])]
df_DPI_LOG_RDV_mask=df_DPI_LOG_RDV_mask2.query('LIBELLE.str.contains("SCANNER")', engine='python')
print(df_DPI_LOG_RDV_mask)



#Concatener les 3 sources de données pour réaliser un dataset Colonnes = NIP / Date activité/ Type Activité/ Source donnée
""" Version avec lecture initiale du fichier CCAM du DIEM
df1=pd.concat([df_CCAM_mask['PA_NIP'],df_CCAM_mask['LI_DATE'],df_CCAM_mask['UFXCODE_LIB'],df_CCAM_mask['SOURCE']],axis=1)
df1.rename(columns={'PA_NIP':'NIP'}, inplace = True)
df1.rename(columns={'LI_DATE':'DATE_ACTIVITE'}, inplace = True)
df1.rename(columns={'UFXCODE_LIB':'TYPE_ACTIVITE'}, inplace = True)
df1['PROJECTION_RDV']=''
df1['STATUT_RDV']=''
print('df1')
print(df1)
"""

print('liste colonnes Data Acti')
print(df_CCAM_mask.columns)

""" Version basée sur la requete de la base activité - table DatasetGlobal"""
df1=pd.concat([df_CCAM_mask['NIP'],df_CCAM_mask['Ddebsej'],df_CCAM_mask['Activite'],df_CCAM_mask['SÃ©quence_Parcours'],df_CCAM_mask['SOURCE']],axis=1)
df1.rename(columns={'PA_NIP':'NIP'}, inplace = True)
df1.rename(columns={'Ddebsej':'DATE_ACTIVITE'}, inplace = True)
df1.rename(columns={'Activite':'TYPE_ACTIVITE'}, inplace = True)
df1.rename(columns={'SÃ©quence_Parcours':'STATUT_RDV'}, inplace = True)
df1['DATE_CREATION']=None
df1['LIB_NATURE']='-'
df1['TYPE_LOG']='REALISE'
print('df1')
print(df1)

df2=pd.concat([df_DPI_CR_mask['NIP'],df_DPI_CR_mask['DAT_VALEUR'],df_DPI_CR_mask['LIB_DOC_FIC'],df_DPI_CR_mask['SOURCE']],axis=1)
df2.rename(columns={'DAT_VALEUR':'DATE_ACTIVITE'}, inplace = True)
df2.rename(columns={'LIB_DOC_FIC':'TYPE_ACTIVITE'}, inplace = True)
df2['DATE_CREATION']=None
df2['STATUT_RDV']='-'
df2['LIB_NATURE']='-'
df2['TYPE_LOG']='REALISE'
print('df2')
print(df2)

df3=pd.concat([df_DPI_LOG_RDV_mask['NIP'],df_DPI_LOG_RDV_mask['DATE_CREATION'],df_DPI_LOG_RDV_mask['LIBELLE'],\
               df_DPI_LOG_RDV_mask['SOURCE'],df_DPI_LOG_RDV_mask['HORAIRE'],df_DPI_LOG_RDV_mask['LIB_STATUT'],df_DPI_LOG_RDV_mask['LIB_NATURE']],axis=1)
#df3.rename(columns={'DATE_CREATION':'DATE_CREATION'}, inplace = True)
df3.rename(columns={'LIBELLE':'TYPE_ACTIVITE'}, inplace = True)
df3.rename(columns={'HORAIRE':'DATE_ACTIVITE'}, inplace = True)
df3.rename(columns={'LIB_STATUT':'STATUT_RDV'}, inplace = True)
df3['TYPE_LOG']='PREVU'
print('df3')
print(df3)


#df1=df1[(df1['TYPE_ACTIVITE']=='Scanner')] ####### FILTRE à l'ACTIVITE DE SCANNER SEULE

df=pd.concat([df1, df2, df3])
print('df')
print(df)

#Affichage du parcours 'RDV SCANNER'
    #Etape 1 : créer un chrono NIP pour positionner les parcours en ordonnée
#df=df.sort_values(by=['NIP'])
df=df.sort_values(by = ['NIP', 'DATE_ACTIVITE','SOURCE'], ascending = [True, True, True], na_position = 'first')
print('df _ trié')
df=df.drop_duplicates()


#filtre la fenêtre temporelle considérée
df=df[(df['DATE_ACTIVITE']>Date_Borne_inf) & (df['DATE_ACTIVITE']<Date_Borne_sup)]



#df['Cpt-Realise']=0
#df['Cpt-Prevu']=0
#print(df)
#Analyse du parcours des patients, isoler les parcours conteant au moin s1 CR_EXt et au moins 1 RDV Annulé
"""
import numpy as np

df['Presence_CR_Ext'] = np.where(df['TYPE_ACTIVITE']!='SCANNER EXT', False , True)
df['Presence_RDV_Annul'] = np.where(df['STATUT_RDV']!='Annul', False , True)

df_CR=pd.concat([df['NIP'],df['Presence_CR_Ext'],df['Presence_RDV_Annul']],axis=1)
df_CR2=df_CR.groupby('NIP').sum().reset_index()
df_CR2['Eligible']=df_CR2['Presence_CR_Ext']*df_CR2['Presence_RDV_Annul']
print(df_CR2)

Liste_NIP=df_CR2[(df_CR2['Eligible']>=1)]
print('Liste des NIPs')
print(Liste_NIP['NIP'])
print('Liste des NIPs - types')
print(Liste_NIP.dtypes)

#df_RDV_Annul=pd.concat(df['NIP'],df['Presence_RDV_Annul'],axis=1)
df=df[df.NIP.isin(Liste_NIP['NIP'])]
print(df)  
"""

compteur=1
compteur_realise=1
Compteur_prevu=1

Liste_compteur=[compteur]
Liste_forme=[]

Lcpt_Realise=[]
Lcpt_Prevu=[]

for row in range(len(df.index)-1):
    if df['NIP'].iloc[row]==df['NIP'].iloc[row+1]:
        liste_temp=compteur
        Liste_compteur.append(liste_temp)
   
        if df['TYPE_LOG'].iloc[row]=='REALISE':
            Lcpt_Realise.append(compteur_realise)
            Lcpt_Prevu.append(None)
            compteur_realise+=1
        
        elif df['TYPE_LOG'].iloc[row]=='PREVU':
            
            Lcpt_Realise.append(None)
            Lcpt_Prevu.append(Compteur_prevu)
            Compteur_prevu+=1
            
        else:
            Lcpt_Realise.append(None)
            Lcpt_Prevu.append(None)
           
    else:
       
        if df['TYPE_LOG'].iloc[row]=='REALISE':
           Lcpt_Realise.append(compteur_realise)
           Lcpt_Prevu.append(None)
                       
           compteur_realise+=1
       
        elif df['TYPE_LOG'].iloc[row]=='PREVU':

           Lcpt_Realise.append(None)
           Lcpt_Prevu.append(Compteur_prevu)
           Compteur_prevu+=1
        
        else:
           Lcpt_Realise.append(None)
           Lcpt_Prevu.append(None)
        
        compteur+=1
        liste_temp=compteur   
        Liste_compteur.append(liste_temp)
        compteur_realise=1
        Compteur_prevu=1

 
if df['TYPE_LOG'].iloc[len(df.index)-1]=='REALISE':
    Lcpt_Realise.append(compteur_realise)
    Lcpt_Prevu.append(None)


elif df['TYPE_LOG'].iloc[len(df.index)-1]=='PREVU':

    Lcpt_Realise.append(None)
    Lcpt_Prevu.append(Compteur_prevu)
 
else:
    Lcpt_Realise.append(None)
    Lcpt_Prevu.append(None)


 
"""    
print('Forme')
print(Liste_forme)        

print('compteur')
print(Liste_compteur)
"""
print('Cpt-Realise')
print(len(Lcpt_Realise))

print('Cpt-Prevu')
print(len(Lcpt_Prevu))

print('compteur')
print(len(Liste_compteur))

df['Cpt-Realise']=Lcpt_Realise
df['Cpt-Prevu']=Lcpt_Prevu

df['Axe_Y']=Liste_compteur
df['Lien_RDV_Acti']=0
df['Delai_RDV_(j)']=np.nan
df['Commentaire']=''
df = df.astype({'Axe_Y':'int','Lien_RDV_Acti':'int','Delai_RDV_(j)':'float','DATE_ACTIVITE':'datetime64[ns]','DATE_CREATION':'datetime64[ns]'})
df = df.astype({'Axe_Y':'int'})


df=df[(df['Axe_Y']<NB_Patients_filtre)] #Filtre les 20 premiers NIP

#print(df)
# Expoert du dataframe au forma excel
#df.to_csv(Chemin_Local_TBD+'test')
#df.sort_values(by = ['NIP', 'DATE_ACTIVITE'], ascending = [True, False], na_position = 'first')

#NECESSAIRE DE RE-INDEXER le DF

df=df.reset_index() 
df.drop('index', axis=1, inplace=True)

df.to_excel("test.xlsx")
print('Fichier test.xlsx créé !')

#print(df)

     #Etape 2 : Afficher le DF en image
"""
df_CS=df[(df['TYPE_ACTIVITE']=='Consultations')]
df_SCINT=df[(df['TYPE_ACTIVITE']=='Scanner')]
df_SCEXT=df[(df['SOURCE']=='DPI_CR')]
df_RDV= df[(df['SOURCE']=='LOG_DPI_RDV')]

plt.scatter(df_RDV['DATE_ACTIVITE'], df_RDV['Axe_Y'], c ="pink",
            marker ='*')
plt.scatter(df_SCINT['DATE_ACTIVITE'], df_SCINT['Axe_Y'], c ="blue",
            marker ='+')
plt.scatter(df_SCEXT['DATE_ACTIVITE'], df_SCEXT['Axe_Y'], c ="red",
            marker ='x')
plt.scatter(df_CS['DATE_ACTIVITE'], df_CS['Axe_Y'], c ="green",
            marker ='o')

plt.xlabel("Date")
plt.ylabel("Patients")
plt.figure (figsize=(100,100))
plt.show()
"""

#Construction d'un tableau de rapprochement RDV -> Activité

#data1 = pd.DataFrame([['N000000', '9000-1-1 0:0:0', '9000-1-1 0:0:0','stat', '9000-1-1 0:0:0','type'],columns=['NIP','Date_creation_RDV', 'Date_projection_RDV','Statut_RDV', 'Date_realisation','Type_Realisation'])

print('-----------------------------------------------')
print('Identification des liens RDV <-> Activité')
print('---------------------')
print('Range')
print(range(df['Axe_Y'].max()))

for y in range(df['Axe_Y'].max()):
    if y>=1:
        
        #Extraction du parcours dans un df provisoire
        df_p=df[(df['Axe_Y']==y)]
        df_RDV=df_p[(df_p['TYPE_LOG']=='PREVU')]        
        
        print('************   PATIENT ' + str(y) + '/' + str(df['Axe_Y'].max()) + '*************')        
               
        #print('Axe_Y = ' + str(y))
        #print(df_RDV)
        #print('len(df_RDV)')
        #print(len(df_RDV.index))
        
        if len(df_RDV.index)!=0:           # si la liste des RDV n'est pas vide alors on analyse les liens en tre rdv et activité
            liste_index_RDV=df_RDV.index.tolist()
            #print('')
            #print('Patient' + ' ' + str(y) + ': ' +  df_p.loc[liste_index_RDV[0],'NIP'])
            #print('NB de RDV = ' + str(len(df_RDV.index)))
            #print('Liste des index RDV Imagerie du parcours  :' + str(liste_index_RDV))
            #print(liste_index_RDV)
            #print('len(df_RDV.index)  =' + str(len(df_RDV.index)))
               
            if len(df_RDV.index)>=1:
                for row in range(len(df_RDV.index)):
                    #print('')
                    #print('ROw = ' + str(row))
                    #print ('range(len(df_RDV.index)')
                    #print (range(len(df_RDV.index)))
                    #print('Cas dfRDV_index >=1')
                    
                    date_activite_rdv=df_RDV['DATE_ACTIVITE'].dt.date.iloc[row]
                    
                    df_Act_scan=df_p[(df['DATE_ACTIVITE']).dt.date==date_activite_rdv]
                    #df_Act_scan=df[(df['DATE_ACTIVITE']).ts.date==date_activite_rdv]
                    #df_Act_scan=df[(pd.to_datetime(df['DATE_ACTIVITE']).dt.date==pd.to_datetime(df_RDV['DATE_ACTIVITE'].dt.date).iloc[row])]
                    
                    df_Act_scan=df_Act_scan[(df_Act_scan['TYPE_ACTIVITE']=='Scanner') | (df_Act_scan['TYPE_ACTIVITE']=='SCANNER EXT')  ]
                    liste_index_scan=df_Act_scan.index.tolist()

                    #print('liste_index des scanners de la bdd activité compatible avec le RDV portant l index ' \
                    #      + str(liste_index_RDV[row]))
                    #print(liste_index_scan)  #liste des scanners de la base d'activité qui s'associe avec le RDV
                    
                    if len(df_Act_scan)!=0:
                        #Cas d'un RDV qui s'associe avec 1 acte de scanner de la BDD
                        #teste si le statut du RDV est bien accueilli pour éviter d'associer à un scanner réalisé fortuitement le meme jour
                        #print(df_p.loc[liste_index_RDV[row],'STATUT_RDV'])
                        if df_p.loc[liste_index_RDV[row],'STATUT_RDV']!='Annul':
                            #print('Patient' + ' ' + str(y) + ': ' +  df_p.loc[liste_index_RDV[0],'NIP'] + '  ' + \
                            #      'J associe le RDV index ' + str(liste_index_RDV[row]) + ' avec l activité de BDD index ' + str(liste_index_scan[0]))
                            
                            df.loc[liste_index_RDV[row],['Lien_RDV_Acti']]=liste_index_scan[0]
                            df.loc[liste_index_scan[0],['Lien_RDV_Acti']]=liste_index_RDV[row]
                            #AJOUTER le CALCUL DU DELAI DE RDV
                            
                            #print(df['DATE_ACTIVITE'].loc[liste_index_RDV[row]])
                            #print(df['DATE_CREATION'].loc[liste_index_RDV[row]])
                            
                            #delai_rdv=df.loc[liste_index_RDV[row],['DATE_ACTIVITE']]-df.loc[liste_index_RDV[row],['DATE_CREATION']]
                            delai_rdv=df['DATE_ACTIVITE'].loc[liste_index_RDV[row]]-df['DATE_CREATION'].loc[liste_index_RDV[row]]
                            
                            #print(delai_rdv)
                            
                            df.loc[liste_index_RDV[row],['Delai_RDV_(j)']]=delai_rdv                           
                            df.loc[liste_index_RDV[row],['Commentaire']]='RDV avec acte BDD associé'
                        #print('Index' + str(df_Act_scan.iloc(row).index))
                    else:
                        #Cas d'un RDV sans activité associée
                       
                       #Extraire les cas où les RDV sont 'annulés' ou non et trouver le lien avec le prochain acte scanner extérieur
                       # ou bien partir d'une routine de test sur les CR_EXt et trouver les RDV antérieurs qui peuvent être associés
                       # en excluant les RDV de scanner déjà associés 
                       
                       #print('Patient' + ' ' + str(y) + ': ' +  df_p.loc[liste_index_RDV[0],'NIP'] + '  ' + \
                       #       'Index du RDV sans activité associée :' + str(liste_index_RDV[row]))
                       df.loc[liste_index_RDV[row],['Lien_RDV_Acti']]=-1    
                       df.loc[liste_index_RDV[row],['Commentaire']]='RDV sans acte associé'     
               
        # Ajouter un test sur l'activité CR (ou BDD) qui n'aurait pas de RDV associé
        
                #lire à l'envers le df_p, 
                #pour chaque "SCANNER EXT", sauf ceux déjàs attributés => OK
                #, chercher le prochain RDV (le premier RDV antérieur)  pour l'associer
                # Si absence de RDV antrieur -> Mentionner un parcours hors processus prise de RDV imagerie.
        df_p=df[(df['Axe_Y']==y)] # Ecrase df-p précédent avec les nouvelles données du df (association des RDV aux actes de la BDD)
        df_scan_ext=df_p[(df_p['TYPE_ACTIVITE']=='SCANNER EXT') & (df_p['Lien_RDV_Acti']==0)]              
        #print('')
        #print('-----------Association des scanners exterieurs à un RDV sans lien d activité dans la BDD -----------')
        
        #print('df_scan_ext')
        #print(df_scan_ext['Lien_RDV_Acti'])
        #print ('len dfscan ext' + str (len(df_scan_ext)))
        
        if len(df_scan_ext)>=1:
            for row in range(len(df_scan_ext)):   
                #♣row_inv=len(df_scan_ext)-1-row
                #print ('row =' + str(row) + '  / Row_inv = ' + str(row_inv))
                
                liste_index_Scan_ext=df_scan_ext.index.tolist()
        
                #print('Index_Scan_ext en cours d analyse = row inv')
                #print(liste_index_Scan_ext[row])
                
                df_Act_RDV=df_p[(df_p['Lien_RDV_Acti']==-1) & (df_p['DATE_ACTIVITE'].dt.date<=pd.to_datetime(df_scan_ext['DATE_ACTIVITE'].dt.date).iloc[row])] #recherche les RDV antérieur au scanner EXT, non attribué
                
                #print('df_Act_RDV')
                #print(df_Act_RDV)

                #print('')
                #print('len(df_Act_RDV.index)')
                #print(len(df_Act_RDV.index))
                
                if len(df_Act_RDV)!=0:
                     #Si nb de RDV = 1, on attribue le RDV 
                     """
                     if len(df_Act_RDV.index)==1:
                       
                        print('nb de RDV = 1, on attribue le RDV')
                        print('Patient' + ' ' + str(y) + ': ' +  df_p.loc[liste_index_Scan_ext[0],'NIP'] + '  ' + \
                                         'Index du Scanner auquel on attribue le RDV index ' + str(df_Act_RDV.index[0]) +  ':' + str(liste_index_Scan_ext[row]))
                        df.loc[liste_index_Scan_ext[row],['Commentaire']]='Scanner Ext potentiellement géré dans le processus RDV imagerie'    
                        df.loc[df_Act_RDV.index[0],['Commentaire']]="RDV pour scanner extérieur"
                        
                        df_p.loc[df_Act_RDV.index[0],['Lien_RDV_Acti']]=liste_index_Scan_ext[row]
                        df.loc[df_Act_RDV.index[0],['Lien_RDV_Acti']]=liste_index_Scan_ext[row]
            
                        df_p.loc[liste_index_Scan_ext[row],['Lien_RDV_Acti']]=df_Act_RDV.index[0]
                        df.loc[liste_index_Scan_ext[row],['Lien_RDV_Acti']]=df_Act_RDV.index[0]
                        
                        delai_rdv=df['DATE_ACTIVITE'].loc[liste_index_Scan_ext[row]]-df['DATE_CREATION'].loc[df_Act_RDV.index[0]]
                        df.loc[df_Act_RDV.index[0],['Delai_RDV_(j)']]=delai_rdv                           
                      
                     elif len(df_Act_RDV.index)>1:
                         print("nb de RDV > 1, on calcule la différence entre la date d'activité du Scan ext et celle du RDV et on attribue le minimum")
                     """   
                         
                     index_RDV_proche=df_Act_RDV['DATE_ACTIVITE'].idxmax()
                     
                     #print('nb de RDV = 1, on attribue le RDV')
                     #print('Patient' + ' ' + str(y) + ': ' +  df_p.loc[liste_index_Scan_ext[0],'NIP'] + '  ' + \
                     #                 'Index du Scanner auquel on attribue le RDV index ' + str(index_RDV_proche) +  ':' + str(liste_index_Scan_ext[row]))
                     
                     df.loc[liste_index_Scan_ext[row],['Commentaire']]='Scanner Ext potentiellement géré dans le processus RDV imagerie'    
                     df.loc[index_RDV_proche,['Commentaire']]="RDV pour scanner extérieur"
                     
                     df_p.loc[index_RDV_proche,['Lien_RDV_Acti']]=liste_index_Scan_ext[row]
                     df.loc[index_RDV_proche,['Lien_RDV_Acti']]=liste_index_Scan_ext[row]
         
                     df_p.loc[liste_index_Scan_ext[row],['Lien_RDV_Acti']]=index_RDV_proche
                     df.loc[liste_index_Scan_ext[row],['Lien_RDV_Acti']]=index_RDV_proche
                     
                     delai_rdv=df['DATE_ACTIVITE'].loc[liste_index_Scan_ext[row]]-df['DATE_CREATION'].loc[index_RDV_proche]
                     df.loc[index_RDV_proche,['Delai_RDV_(j)']]=delai_rdv
                     
                    
                     #Si nb de RDV > 1, on calcule la différence entre la date d'activité du Scan ext et celle du RDV et on attribue le minimum

                else:
                    #print("#Cas d'aucun acte scanner EXTERIEUR associable avec un RDV")
                    #inscrire l'info que l'acte de scanner ext est réalisé hors du processus RDV d'imagerie (2 cas distincts, scanner déjà associé ou non)
                    df.loc[liste_index_Scan_ext[row],['Commentaire']]='Scanner Ext hors process RDV Imagerie'    

print('ecriture du fichier de post-traitement')
print('Période du : ' + str(Date_Borne_inf) + ' au ' + str(Date_Borne_sup) )                  
print('Cut-off nb de patient ' + str(NB_Patients_filtre))

df.to_excel("RDV-Post-tt.xlsx")
print('Fichier RDV-Post-tt.xlsx sauvegardé !')                    

print('**********  Fin du script python ! ************')                    


                    
        
