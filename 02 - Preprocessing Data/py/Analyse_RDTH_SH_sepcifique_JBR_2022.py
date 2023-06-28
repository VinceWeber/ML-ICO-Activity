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
Chemin_Local_TBD='C:\\Users\\v-weber\\Documents\\99-Perso\\Analyses statistiques\\Activité ICO\\Python\\Analyse RDTH'


Char_Delimiter=';'
Nom_fichier_ACCAM='exportCernerACCAM_Test_50_lignes.csv'
Nom_fichier_Acti='Log_Parcours_Patients_ayant_eu_une_activite_RDTH_entre_2020et2021_v2.csv'
Nom_fichier_NCCAM='exportCernerNCCAM2022.csv'  #exportCernerNCCAM2022.csv
#Nom_fichier_ANGAP='exportCernerNCCAM_Test_50_lignes.csv'
#Nom_fichier_NNGAP='exportCernerNNGAP_Test_50_lignes.csv'
#Nom_fichier_AMVT='MVT-ANG-2019-2021_Test_50_lignes.csv'
#Nom_fichier_NMVT='MVT-SH-2019-2021_Test_50_Lignes.csv'
Nom_Fichier_DPI_CR_Ext='CR_Exterieurs.csv'   #CR_Exterieurs.csv
Nom_Fichier_LOG_RDV='Log_RDV_IMA_SH_2021-2022_v3.csv' #Log_RDV_IMA_SH_2021-2022.csv
Nom_Fichier_Localisations='Liste_localisations_ssdoublons.xlsx'


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


#1- Lire fichier NCCAM
df_NCCAM_RD=MFP.F_NCCAM_csv_to_panda_df(Nom_fichier_NCCAM, Chemin_Local_TBD) #lecture du fichier .csv vers un dataframe panda


#2 - Lire fichier des localisations 
Chemin_fich=Chemin_Local_TBD + '\\' + Nom_Fichier_Localisations

df_localisation=pd.read_excel(Chemin_fich,sheet_name='Feuil1')
df_localisation=df_localisation.astype({'NIP':'string','Localisation':'string','Code Loc':'string','Type tumeur':'string','Fonction':'string'})
#3 - Construction DF avec localisations

df_Acti_1=df_NCCAM_RD.join(df_localisation.set_index('NIP'),on='PA_NIP')
df_Acti_1=df_Acti_1.rename(columns={'LI_DATE':'Ddebsej','PA_NIP':'NIP','UFXCODE_LIB':'UFX_Code_Lib'})
df_Acti_1['Poids_Dim_Soins']=1
df_Acti_1=df_Acti_1.drop_duplicates()

df_Acti=df_Acti_1[(df_Acti_1['Localisation']!='')]
#print(df_Acti)
df_Acti.to_excel("test_join.xlsx")
print(df_Acti.index.is_unique)
df_Acti.flags.allows_duplicate_labels = False
print('lecture fichier ok')


#---------------------------------------------------------------------------------
#ANALYSE DES DONNEES
#---------------------------------------------------------------------------------


""" PRINCIPE :
    0- Lister les différents patients -> créer l'Axe Y
    
    1- A partir du dataset global, on extrait tous les actes de radiothérapie irradiation pour identifier le 1er acte de traitement de radiothérapie
        -> df_rdth=df.query('UFXCODE_LIB.str.contains("RADTIOH") and UFXCODE_LIB.str.contains("Traitement")', engine='python')  
        -> définition du J0 RDTH = Date du 1er traitement en RDTH
        -> définition du Jfin RDTH = Date du dernier traitement en RDTH
    
    2- Calcul des poids d'activité 
        - Avant Traitement
            Extraire un df des actes antérieurs à J0 radiothérapie
            Sommer les actes réalisés dans la période
            
        -Pendant Traitement
            Extraire un df des actes réalisés entre J0 rdth et Jfin
            Sommer les actes réalisés dans la période.
            
    3- Préparer un df de restitution
       - Axe Y, NIP, J0 RDTH, Jfin RDTH, Poids avant, TTn Poids pendant TT, Localisation
"""

"""
0 - LISTER LES DIFFERENTS PATIENTS + 
1 - -> définition du J0 RDTH = Date du 1er traitement en RDTH
    -> définition du Jfin RDTH = Date du dernier traitement en RDTH
    
    version 2 :
        Ajouter J0 1ère consultation à l'ICO.
        Ajouter J0 1er Traitement à l'ICO.
        
        Ajouter J0 Chir
        Ajouter Jfin Chir
        
        Ajouter J0 Chimio
        Ajouter Jfin Chimio
"""
print('Analyse des données')

df_Acti=df_Acti.sort_values(by = ['NIP', 'Ddebsej'], ascending = [True, True], na_position = 'first')
df_temp=pd.DataFrame(columns=['NIP','1er_seance_RDTH','1er_CST_RDTH','Dern_seance_RDTH','pds_sej_avt_RDTH','pds_sej_ap_RDTH','Code Loc','Type tumeur','Fonction'])

liste_index=df_Acti.index.tolist()

compteur=1
Liste_compteur=[compteur]
Premier_consult_RDTH=False
Premier_seance_RDTH=False

Temp_date_premiere_seance_RDTH=None
Temp_date_derniere_seance_RDTH=None
Temp_date_premiere_CS_RDTH=None

for row in range(len(df_Acti.index)-1):

    if df_Acti['NIP'].iloc[row]==df_Acti['NIP'].iloc[row+1]:
        liste_temp=compteur
        Liste_compteur.append(liste_temp)
        
        if df_Acti['UFX_Code_Lib'].iloc[row]=='RADIOTHERAPIE IRRADIATION' and Premier_seance_RDTH==False :
            Premier_seance_RDTH=True
            Temp_date_premiere_seance_RDTH=df_Acti['Ddebsej'].iloc[row]         
       
        if df_Acti['UFX_Code_Lib'].iloc[row]=='RADIOTHERAPIE IRRADIATION':
            Temp_date_derniere_seance_RDTH=df_Acti['Ddebsej'].iloc[row]
  
        if df_Acti['UFX_Code_Lib'].iloc[row]=='CONSULTATION DE RADIOTHERAPIE' and Premier_consult_RDTH==False :
              Premier_consult_RDTH=True
              Temp_date_premiere_CS_RDTH=df_Acti['Ddebsej'].iloc[row]
    else:  
        compteur+=1
        code_Loc=df_Acti['Code Loc'].iloc[row]
        Type_Tumeur=df_Acti['Type tumeur'].iloc[row]
        Fonction=df_Acti['Fonction'].iloc[row]
        
        df_temp.loc[len(df_temp.index)]=[df_Acti['NIP'].iloc[row],Temp_date_premiere_seance_RDTH,Temp_date_premiere_CS_RDTH,Temp_date_derniere_seance_RDTH,None,None,code_Loc,Type_Tumeur,Fonction]
        
        Temp_date_premiere_seance_RDTH=None
        Temp_date_derniere_seance_RDTH=None
        Temp_date_premiere_CS_RDTH=None
        
        Premier_seance_RDTH=False
        Premier_consult_RDTH=False
        liste_temp=compteur   
        Liste_compteur.append(liste_temp)

#dernier item

df_temp.loc[len(df_temp.index)]=[df_Acti['NIP'].iloc[row+1],Temp_date_premiere_seance_RDTH,Temp_date_premiere_CS_RDTH,Temp_date_derniere_seance_RDTH,None,None,code_Loc,Type_Tumeur,Fonction]


print('compteur')
print(len(Liste_compteur))


df_Acti['Axe_Y']=Liste_compteur
df_Acti = df_Acti.astype({'Axe_Y':'int'})
#print(df_Acti)
df_Acti.to_excel("Post-TT_RDTH-2022_debug.xlsx")

print('df_temp_dates_repères')
print(df_temp)


"""
2 - Calcul des poids d'activité 
        - Avant Traitement
            Extraire un df des actes antérieurs à J0 radiothérapie
            Sommer les actes réalisés dans la période
            
        -Pendant Traitement
            Extraire un df des actes réalisés entre J0 rdth et Jfin
            Sommer les actes réalisés dans la période.
"""
#Boucle => pour chaque patient de df_temp, extraire le df_acti correspondant aux dates J0 RDTH et RDTH puis calculer le poids TT 


for row in range(len(df_temp.index)):
    
    print('Analyse dates TT du Patient n° ' + str(row) + ' / ' + str(len(df_temp.index)))
    #Extraction du df correspondant au NIP
    
    if df_temp['1er_seance_RDTH'].iloc[row]!=None:
        df_NIP=df_Acti[(df_Acti['Axe_Y']==row+1)]
        df_NIP_av_RDTH=df_NIP[(df_NIP['Ddebsej']<df_temp['1er_seance_RDTH'].iloc[row])]
        
        df_NIP_pdt_RDTH=df_NIP[(df_NIP['Ddebsej']>=df_temp['1er_seance_RDTH'].iloc[row])]
        df_NIP_pdt_RDTH=df_NIP_pdt_RDTH[(df_NIP['Ddebsej']<=df_temp['Dern_seance_RDTH'].iloc[row])]
        df_NIP_pdt_RDTH=df_NIP_pdt_RDTH[(df_NIP['UFX_Code_Lib']=='RADIOTHERAPIE IRRADIATION')]
        
        """
        print('Patient n°' + str(row) )
        print('df_NIP')
        print(df_NIP)
        print('df_Avant_RDTH')
        print(df_NIP_av_RDTH)
        print('df_pdt_RDTH')
        print(df_NIP_pdt_RDTH)
        """
        
        #CALCUL DES POIDS DE SEJOUR
        poids_avt_TT=df_NIP_av_RDTH['Poids_Dim_Soins'].sum()
        
        #Attention IL FAUT FILTRER LES ACTES DE RDTH IRRADIATION SEULE !!!!
        poids_pdt_TT=df_NIP_pdt_RDTH['Poids_Dim_Soins'].sum()
    
        #intégration dans le df 
        df_temp['pds_sej_avt_RDTH'].loc[row]=poids_avt_TT
        df_temp['pds_sej_ap_RDTH'].loc[row]=poids_pdt_TT


print('df_temp_dates_repères + poids des séjours')
print(df_temp)

print('Ecrireture du fichier de sortie')
#df_Acti.to_excel("debug_RDTH.xlsx")
df_temp.to_excel("Post-TT_RDTH-2022.xlsx")
            

print('**********  Fin du script python ! ************')                    


                    
        
