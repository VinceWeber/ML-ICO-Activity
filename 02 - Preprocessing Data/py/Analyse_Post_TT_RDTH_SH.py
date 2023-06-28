# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 08:49:50 2022

@author: v-weber

script python d'analyse du fichier DV_IMG post traité où les parcours patients on t été analysés et les liens entre RDV et examens de scanner interne et externe ont été réalisé.'

"""

#"Import des modules complémentaires"
import math    
import csv  #Lecture / Ecriture des fichiers .csv
import os #Gestion des fichier et chemins - https://python.doctor/page-gestion-fichiers-dossiers-python  
import Module_Fonctions_Principales_Import as MFP # module des fonctions d'import de BDD
import pandas as pd #module panda gestion des dataframes et BDD https://pandas.pydata.org/
import datetime
import matplotlib.pyplot as plt
import numpy as np


Chemin_Local_TBD='C:\\Users\\v-weber\\Documents\\99-Perso\\Analyses statistiques\\Activité ICO\\Python'
Nom_Fichier_RDV_Post_TT='RDV-Post-tt.xlsx'

#ETAPE 1 IMPORT DU FICHIER .CSV

df=MFP.F_RDV_POST_TT_csv_to_panda_df(Nom_Fichier_RDV_Post_TT,Chemin_Local_TBD)

#exclue les consultations de l'analyse
df=df[(df['TYPE_ACTIVITE']!='Consultations')]


#Preparation des données pour affichage
#filtre de la periode à étudier
Date_Borne_inf=datetime.datetime(2021,1,1)
Date_Borne_sup=datetime.datetime(2022,1,1)

df_filtre=df[(df['DATE_ACTIVITE']>Date_Borne_inf) & (df['DATE_ACTIVITE']<Date_Borne_sup)]

Nombre_RDV_local=df_filtre[(df_filtre['Commentaire']=='RDV avec acte BDD associé')]['NIP'].count()
Nombre_RDV_Ext=df_filtre[(df_filtre['Commentaire']=='RDV pour scanner extérieur')]['NIP'].count()
Nombre_SCAN_LOCAL=df_filtre[(df_filtre['TYPE_ACTIVITE']=='Scanner')]['NIP'].count()
Nombre_Scan_Ext_avec_RDV=df_filtre[(df_filtre['Commentaire']=='Scanner Ext potentiellement géré dans le processus RDV imagerie')]['NIP'].count()
Nombre_Scan_Ext_hors_processus_RDV=df_filtre[(df_filtre['Commentaire']=='Scanner Ext hors process RDV Imagerie')]['NIP'].count()

print('Période du : ' + str(Date_Borne_inf) + ' au ' + str(Date_Borne_sup) )
print('Nombre_RDV_local dans la période :' + str(Nombre_RDV_local))
print('Nombre_RDV_Ext dans la période :' + str(Nombre_RDV_Ext))
print('Nombre_Scanner_local dans la période :' + str(Nombre_SCAN_LOCAL))
print('Nombre_Scan_Ext_avec_RDV dans la période :' + str(Nombre_Scan_Ext_avec_RDV))
print('Nombre_Scan_Ext_hors_processus_RDV dans la période :' + str(Nombre_Scan_Ext_hors_processus_RDV))

df_filtre['Délai (sem)']=df_filtre['Delai_RDV_(j)']/7
df_filtre['Délai (sem)']=df_filtre['Délai (sem)'].apply(np.floor)

#☻print(df_filtre)
df_filtre.to_excel("RDV_Filtre.xlsx")

print('Affiche graphique')



#calcul du nombre