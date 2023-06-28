# -*- coding: utf-8 -*-
"""
Created on Tue Aug 30 13:21:27 2022

@author: v-weber
"""

# Module des fonctions pricinpales d'import de l'activité CCAM/NGAP/MVT dans la BDD activité


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

def F_SQL_Liste_tables(cnxn,cursor,pyodbc): # fonction pour lister les tables de la BDD
    try:
        for row in cursor.tables(schema='dbo', tableType='TABLE'):
            print(row.table_name)
        print(cursor.messages)
        print(cursor.rowcount)
    except pyodbc.Error as ex:
           sqlstate = ex.args[1]
           sqlstate = sqlstate.split(".")
           print('F_SQL_Liste_tables - ERROR')
           print(sqlstate)        
    else :
        print('F_SQL_Liste_tables - OK')
    return

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

def F_SQL_Drop_table(NOM_Table):
    requete_sql="""DROP TABLE [dbo].["""+NOM_Table+"""]"""
    return requete_sql

def F_SQL_Creation_table_import_Activite(NOM_table):
    requete_sql = """CREATE TABLE [dbo].["""+NOM_table+"""](
	[idImport-Actes-CCAM] [varchar](50) NULL,
	[NIP_original] [varchar](50) NULL,
	[Ho_Num_Num_sejour] [varchar](50) NULL,
	[Date_début_acte] [datetime] NULL,
	[Date_fin_acte] [datetime] NULL,
	[Date-Debut_Mvt] [datetime] NULL,
	[Heure_Debut_Mvt] [datetime] NULL,
	[Date-Fin_Mvt] [datetime] NULL,
	[Heure_Fin_Mvt] [datetime] NULL,
	[UFX_UFX_Code] [varchar](50) NULL,
	[UFX_Code_Lib] [varchar](50) NULL,
	[Ressource_Med_INX_INX_Code] [varchar](50) NULL,
	[INX_Code_Lib] [varchar](50) NULL,
	[INX_Code_Spe] [varchar](50) NULL,
	[Ref_Actes_NGAP_AC_Code_NGAP] [varchar](50) NULL,
	[AC_Ref] [varchar](50) NULL,
	[AC_Lib] [varchar](250) NULL,
	[Lib_spe] [varchar](50) NULL,
	[Ref_spe] [varchar](50) NULL,
	[Ref_Actes_CCAM_AC_Ref_CCAM] [varchar](50) NULL,
	[CCAM_AC_Lib] [varchar](250) NULL,
	[AC_Acti] [varchar](50) NULL,
	[AC_Asso] [varchar](50) NULL,
	[Lc_Prix] [varchar](50) NULL,
	[mh_ufheber_??] [varchar](50) NULL,
	[mh_ufheber_lib_??] [varchar](50) NULL,
	[Prix_Acte] [varchar](50) NULL,
	[Cout_Acte] [varchar](50) NULL,
	[Statut] [varchar](50) NULL,
	[Equipements_Code_Equipement] [varchar](50) NULL,
	[Site_idSite] [varchar](50) NULL,
	[Source] [varchar](50) NULL,
    [Date_Import] [datetime] NULL,
    [Date_Peremption] [datetime] NULL
) ON [PRIMARY]"""
    return requete_sql
    
def F_SQL_Creation_table_import_test(NOM_table):
    requete_sql = """CREATE TABLE [dbo].["""+NOM_table+"""](
	[idImport-Actes-CCAM] [varchar](50) NULL,
	[NIP_original] [varchar](50) NULL,
    [Date_Import] [datetime] NULL,
    [Date_Peremption] [datetime] NULL
) ON [PRIMARY]"""
    return requete_sql
    
def F_SQL_Creation_table_import_CR(NOM_table):
    requete_sql = """CREATE TABLE [dbo].["""+NOM_table+"""](
	[idImport-CR] [varchar](50) NULL,
	[NIP_original] [varchar](50) NULL,
    [Date_Import] [datetime] NULL,
    [Type_Examen] [varchar](50) NULL,
    [ID_document] [varchar](50) NULL,
    [LIB_TYPE_COUR] [varchar](50) NULL
) ON [PRIMARY]"""

    return requete_sql

def F_SQL_Requete_Activite_scanner_consult ():
    requete_sql = """SELECT [id_acte]
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
  WHERE [Ddebsej]> '2021-01-01' and ( [Activite] like 'Scanner' or [Activite] like 'Consultations' ) and site =2"""
    return requete_sql




def F_SQL_Import_DF_dans_Table_Import_CR(NOM_Table,df,cnxn,cursor,pyodbc):
    from datetime import datetime
    # Insert DataFrame to Table
    Nb_ligne_importees=0
    Nb_Ligne_erreur=0
    #change le format de date pour être exportable en sql
  
    for row in range(len(df.index)):
        try:

            #chaine_date= F_Panda_datatime_to_str_SQL(df,'LI_DATE',row)
            #print(chaine_date)
            
            #print("REQETE_SQL_1 : ")
            #requete1="""INSERT INTO [""" + NOM_Table + ']'+"""([idImport-CR],[NIP_original],[Date_Import],[Type_Examen],[ID_document],[LIB_TYPE_COUR])
            #         VALUES ('"""+ str(row) +"""','"""+ df.loc[row,'PA_NIP']+"""','"""+ str(chaine_date)+"""','""" +str(chaine_date)+"""')"""
            
            
            liste_colonnes_import="""[idImport-CR],[NIP_original],[Date_Import],[Type_Examen],[ID_document],[LIB_TYPE_COUR]"""
            
            requete2="""INSERT INTO [""" + NOM_Table + ']('+liste_colonnes_import + """) VALUES('"""            
            requete2= requete2 + str(row)                                            + """','"""         #ajout de l'idImport à supprimer si autoincrement
            requete2= requete2 + df.loc[row,'NIP']                                + """','"""         #ajout du champ NIP
            requete2= requete2 + F_Panda_datatime_to_str_SQL(df,'DAT_VALEUR',row)  + """','"""         #ajout du champ date de l'examen
            requete2= requete2 + df.loc[row,'LIB_DOC_FIC']                           + """','"""         #ajout du champ Type Examen 
            requete2= requete2 + df.loc[row,'ID_DOC_FIC']                            + """','"""         #ajout du champ ID du document dans DxCare
            requete2= requete2 + df.loc[row,'LIB_TYPE_COUR']                         + """')"""         #ajout du champ Lib type courrier Dxcare
                       
            #chaine= 'Table_Import_CR: ligne:' + str(row) + '/' + str(len(df.index))
            #print(chaine)
            
            cursor.execute(str(requete2))
        except pyodbc.Error as ex:
               sqlstate = ex.args[1]
               sqlstate = sqlstate.split(".")
               #print('F_SQL_Import_DF_dans_Table_Import - ERROR')
               print(sqlstate)       
               Nb_Ligne_erreur= Nb_Ligne_erreur + 1
        else :
            Nb_ligne_importees =  Nb_ligne_importees + 1
            #print('F_SQL_Import_DF_dans_Table_Import - OK')
            
    cnxn.commit()
    chaine_sortie1=str(Nb_ligne_importees) + ' lignes OK' 
    chaine_sortie2=str(Nb_Ligne_erreur) + ' lignes en erreur SQL' 
    print(chaine_sortie1)
    print(chaine_sortie2)
    return 




def F_SQL_Import_DF_dans_Table_Import_CCAM(NOM_Table,df,cnxn,cursor,pyodbc):
    from datetime import datetime
    # Insert DataFrame to Table
    Nb_ligne_importees=0
    Nb_Ligne_erreur=0
    #change le format de date pour être exportable en sql
  
    for row in range(len(df.index)):
        try:

            chaine_date= F_Panda_datatime_to_str_SQL(df,'LI_DATE',row)
            #print(chaine_date)
            
            #print("REQETE_SQL_1 : ")
            requete1="""INSERT INTO [""" + NOM_Table + ']'+"""([idImport-Actes-CCAM],[NIP_original],[Date_Import],[Date_Peremption])
                     VALUES ('"""+ str(row) +"""','"""+ df.loc[row,'PA_NIP']+"""','"""+ str(chaine_date)+"""','""" +str(chaine_date)+"""')"""
            
            liste_colonnes_import="""[idImport-Actes-CCAM],[NIP_original],[Ho_Num_Num_sejour],[Date_début_acte],[Date_fin_acte],[Date-Debut_Mvt],
            [Heure_Debut_Mvt],[Date-Fin_Mvt],[Heure_Fin_Mvt],[UFX_UFX_Code],[UFX_Code_Lib],[Ressource_Med_INX_INX_Code],
        	[INX_Code_Lib],[INX_Code_Spe],[Ref_Actes_NGAP_AC_Code_NGAP],[AC_Ref],[AC_Lib],[Lib_spe],[Ref_spe],
            [Ref_Actes_CCAM_AC_Ref_CCAM],[CCAM_AC_Lib],[AC_Acti],[AC_Asso],[Lc_Prix],[mh_ufheber_??],[mh_ufheber_lib_??],
        	[Prix_Acte],[Cout_Acte],[Statut],[Equipements_Code_Equipement],[Site_idSite],[Source],[Date_Import],
            [Date_Peremption]"""
            
            requete2="""INSERT INTO [""" + NOM_Table + ']('+liste_colonnes_import + """) VALUES('"""            
            requete2= requete2 + str(row)                                        + """','"""         #ajout de l'idImport à supprimer si autoincrement
            requete2= requete2 + df.loc[row,'PA_NIP']                            + """','"""         #ajout du champ NIP
            requete2= requete2 + df.loc[row,'HO_NUM']                            + """','"""         #ajout du champ n° de séjour
            requete2= requete2 + F_Panda_datatime_to_str_SQL(df,'LI_DATE',row)   + """','"""         #ajout du champ date début d'acte 
            requete2= requete2 + F_Panda_datatime_to_str_SQL(df,'LI_DATE',row)   + """','"""         #ajout du champ date fin d'acte 
            requete2= requete2 + "9000-1-1 0:0:0"                                          + """','"""         #ajout du champ date début MVT
            requete2= requete2 + "9000-1-1 0:0:0"                                          + """','"""         #ajout du champ heure debut MVT
            requete2= requete2 + "9000-1-1 0:0:0"                                          + """','"""         #ajout du champ date fin MVT
            requete2= requete2 + "9000-1-1 0:0:0"                                          + """','"""         #ajout du champ heure fin MVT
            requete2= requete2 +  df.loc[row,'UFXCODE']                          + """','"""         #ajout du champ UFX_UFX_Code 
            requete2= requete2 +  df.loc[row,'UFXCODE_LIB']                      + """','"""         #ajout du champ UFX_Code_Lib 
            requete2= requete2 +  df.loc[row,'INXCODE']                          + """','"""         #ajout du champ Ressource_Med_INX_Code 
            requete2= requete2 +  df.loc[row,'INXCODE_NOM']                      + """','"""         #ajout du champ INX Code_Lib 
            requete2= requete2 +  df.loc[row,'INXCODE_SPE']                      + """','"""         #ajout du champ INX_Code_Spe  
            requete2= requete2 + ""                                              + """','"""         #ajout du champ Ref Actes NGAP_COde_NGAP
            requete2= requete2 + ""                                              + """','"""         #ajout du champ AC_Ref
            requete2= requete2 + ""                                              + """','"""         #ajout du champ AC_Lib
            requete2= requete2 + ""                                              + """','"""         #ajout du champ Lib_spe
            requete2= requete2 + ""                                              + """','"""         #ajout du champ Ref_Spe
            requete2= requete2 + df.loc[row,'AC_REF']                            + """','"""         #ajout du champ AC_ref CCAM  
            requete2= requete2 + df.loc[row,'AC_LIB']                            + """','"""         #ajout du champ AC_Lib 
            requete2= requete2 + ""                                              + """','"""         #ajout du champ AC_Acti df.loc[row,'AC_ACTI']    
            requete2= requete2 + ""                                              + """','"""         #ajout du champ AC_Asso df.loc[row,'AC_ASSOC']
            requete2= requete2 + ""                                              + """','"""         #ajout du champ LC_Prix
            requete2= requete2 + ""                                              + """','"""         #ajout du champ mh Uf_herbegement
            requete2= requete2 + ""                                              + """','"""         #ajout du champ mh_Uf_hebergement_lib
            requete2= requete2 + ""                                              + """','"""         #ajout du champ Prix Acte
            requete2= requete2 + ""                                              + """','"""         #ajout du champ Cout Acte
            requete2= requete2 + df.loc[row,'HO_RECODE']                         + """','"""         #ajout du champ Statut (Hospit / Externe) 
            requete2= requete2 + ""                                              + """','"""         #ajout du champ Code Equipement
            requete2= requete2 + '1'                                             + """','"""         #ajout du champ idSite
            requete2= requete2 + "CCAM"                                          + """','"""         #ajout du champ Source
            requete2= requete2 + F_PyDatetime_to_str_SQL(datetime.now())         + """','"""         #ajout du champ Date Import )
            requete2= requete2 + "9000-1-1 0:0:0"                                + """')"""         #ajout du champ Date Peremption
            
            
            #chaine= 'Table_Import_CCAM: ligne:' + str(row) + '/' + str(len(df.index))
            #print(chaine)
            

            #print("REQETE_SQL_2 : ")
            #print(str(requete2))
            #print(requete1)
             
            cursor.execute(str(requete2))
        except pyodbc.Error as ex:
               sqlstate = ex.args[1]
               sqlstate = sqlstate.split(".")
               #print('F_SQL_Import_DF_dans_Table_Import - ERROR')
               print(sqlstate)       
               Nb_Ligne_erreur= Nb_Ligne_erreur + 1
        else :
            Nb_ligne_importees =  Nb_ligne_importees + 1
            #print('F_SQL_Import_DF_dans_Table_Import - OK')
            
    cnxn.commit()
    chaine_sortie1=str(Nb_ligne_importees) + ' lignes OK' 
    chaine_sortie2=str(Nb_Ligne_erreur) + ' lignes en erreur SQL' 
    print(chaine_sortie1)
    print(chaine_sortie2)
    return 

def F_SQL_Import_DF_dans_Table_Import_NGAP(NOM_Table,df,cnxn,cursor,pyodbc):
    print('F_SQL_Import_DF_dans_Table_Import_NGAP : Non fonctionnelle - Code à écrire')
    return

def F_SQL_Import_DF_dans_Table_Import_MVT(NOM_Table,df,cnxn,cursor,pyodbc):
    print('F_SQL_Import_DF_dans_Table_Import_MVT : Non fonctionnelle - Code à écrire')
    return

def F_SQL_Import_DF_test_requete(NOM_Table,df):
    # Insert DataFrame to Table
    for row in range(len(df.index)):
        #print(df.loc[row,'LI_DATE'])
        print("""
                    INSERT INTO """ + NOM_Table + """(idImport-Actes-CCAM, NIP_original, Date_Import,Date_Peremption)
                    VALUES (?,?,?)
                    """,
                    df.loc[row,'PA_NIP'], 
                    df.loc[row,'LI_DATE'],
                    df.loc[row,'LI_DATE']
                    )
    #conn.commit()
    return 

def F_Panda_datatime_to_str_SQL(df,Panda_Colonne,row):
    #Fonction de création d'une string pour requete d'insert SQL, df= table panda contenant un datetime, Panda_Colonne nom de la colonne contenant le datetime
    
    Mois=df.loc[row,Panda_Colonne].month
    joursem=df.loc[row,Panda_Colonne].day
    Annee=df.loc[row,Panda_Colonne].year
    Heure=df.loc[row,Panda_Colonne].hour
    Minutes=df.loc[row,Panda_Colonne].minute
    Secondes=df.loc[row,Panda_Colonne].second
    
    chaine_date= str(Annee) + "-" + str(joursem) + "-" + str(Mois)+ " " + str(Heure)+ ":" + str(Minutes)+ ":" + str(Secondes)

    return chaine_date

def F_PyDatetime_to_str_SQL(madate):
    
    Mois=madate.month
    joursem=madate.day
    Annee=madate.year
    Heure=madate.hour
    Minutes=madate.minute
    Secondes=madate.second
    
    chaine_date= str(Annee) + "-" + str(joursem) + "-" + str(Mois)+ " " + str(Heure)+ ":" + str(Minutes)+ ":" + str(Secondes)
    
    return chaine_date


def F_ACCAM_csv_to_panda_df (Nom_fichier_ACCAM, Chemin_Local_TBD):
    import pandas as pd
    #ETAPE 1 - Fichier CCAM Angers
    #print('-----------------------------')
        #Creation table d'import provisoire
    
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
    
    #ajout de la colonne 'SOURCE' 
    df['SOURCE']='CCAM'
    
    #affiche la longueur du fichier .csv
    chaine_txt= "Nombre de lignes du fichier : " + Nom_fichier_ACCAM + " est de " + str(len(df.index))
    print(chaine_txt)
 
    return df

def F_NCCAM_csv_to_panda_df (Nom_fichier_NCCAM, Chemin_Local_TBD):
    import pandas as pd
    #Fichier CCAM NANTES
    #print('-----------------------------')
        #Creation table d'import provisoire
    
    #Import du fichier .csv dans un dataframe
    Chemin_fich=Chemin_Local_TBD + '\\' + Nom_fichier_NCCAM
    print(Chemin_fich)

    dataNCCAM=pd.read_csv(Chemin_fich,encoding='ISO-8859-1',sep=';') #lecteur du fichier .csv
    df=pd.DataFrame(dataNCCAM) #Intégration des données dans un dataframe panda
    
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
    df['PA_NIP']=df.apply(lambda x: 'N' + x['PA_NIP'],axis=1)
    df['HO_NUM']=df.apply(lambda x: 'N' + x['HO_NUM'],axis=1)
    df['INXCODE']=df.apply(lambda x: x['INXCODE'].replace(" ", ""),axis=1)
    df['HO_RECODE']=df.apply(lambda x: x['HO_RECODE'].replace(" ", ""),axis=1)

    #print(df)
    #df.loc[row,'AC_ASSOC'] -> Si NULL -> "" , SI Not NULL = même valeur    
    df['SOURCE']='CCAM'    

    #affiche la longueur du fichier .csv
    chaine_txt= "Nombre de lignes du fichier : " + Nom_fichier_NCCAM + " est de " + str(len(df.index))
    print(chaine_txt)
 
    return df

def F_Acti_csv_to_panda_df (Nom_fichier_Acti, Chemin_Local_TBD):
    import pandas as pd
    #ETAPE 1 - Fichier CCAM Angers
    #print('-----------------------------')
        #Creation table d'import provisoire
    
    #Import du fichier .csv dans un dataframe
    Chemin_fich=Chemin_Local_TBD + '\\' + Nom_fichier_Acti
    print(Chemin_fich)

    dataNCCAM=pd.read_csv(Chemin_fich,encoding='ISO-8859-1',sep=';') #lecteur du fichier .csv
    df=pd.DataFrame(dataNCCAM) #Intégration des données dans un dataframe panda

    #Definition des types de données du fichier.csv
    #pd.to_datetime(df['LI_DATE'], dayfirst=true)
    #df['LI_DATE']=pd.to_datetime(df['LI_DATE'],infer_datetime_format=False)
    df['Ddebsej'] = pd.to_datetime(df['Ddebsej'], dayfirst=True)
    
    df = df.astype({'NIP':'string'})
    #print(df)
    
    #print('-> Liste des entête du fichier CCAM.csv')
    #print(df.dtypes)
    
    #ZONE OU L'ON PEUT AJOUTER DES TRANSFORMATIONS DE DONNEES ex : filtrage de la date de l'acte, suppression des espaces 
    #print('-> modification du df')
    #df['PA_NIP']=df.apply(lambda x: 'N' + x['PA_NIP'],axis=1)
    #df['HO_NUM']=df.apply(lambda x: 'N' + x['HO_NUM'],axis=1)
    #df['INXCODE']=df.apply(lambda x: x['INXCODE'].replace(" ", ""),axis=1)
    #df['HO_RECODE']=df.apply(lambda x: x['HO_RECODE'].replace(" ", ""),axis=1)

    #ajout de la colonne 'SOURCE' 
    df['SOURCE']='BDD_Acti'
    print(df)
    #affiche la longueur du fichier .csv
    chaine_txt= "Nombre de lignes du fichier : " + Nom_fichier_Acti + " est de " + str(len(df.index))
    print(chaine_txt)
 
    return df

def F_Acti_RDTH_csv_to_panda_df (Nom_fichier_Acti, Chemin_Local_TBD):
    import pandas as pd
    #ETAPE 1 - Fichier CCAM Angers
    #print('-----------------------------')
        #Creation table d'import provisoire
    
    #Import du fichier .csv dans un dataframe
    Chemin_fich=Chemin_Local_TBD + '\\' + Nom_fichier_Acti
    print(Chemin_fich)

    dataNCCAM=pd.read_csv(Chemin_fich,sep=';') #lecteur du fichier .csv #encoding='ISO-8859-1',
    df=pd.DataFrame(dataNCCAM) #Intégration des données dans un dataframe panda

    #Definition des types de données du fichier.csv
    #pd.to_datetime(df['LI_DATE'], dayfirst=true)
    #df['LI_DATE']=pd.to_datetime(df['LI_DATE'],infer_datetime_format=False)
    df['Ddebsej'] = pd.to_datetime(df['Ddebsej'], dayfirst=False)
    df['Dfinsej'] = pd.to_datetime(df['Dfinsej'], dayfirst=False)
    df['J0_V2'] = pd.to_datetime(df['J0_V2'], dayfirst=True)
    df['J0_V1'] = pd.to_datetime(df['J0_V1'], dayfirst=True)
    df['J0_V3'] = pd.to_datetime(df['J0_V3'], dayfirst=True)
    df['J0_V4'] = pd.to_datetime(df['J0_V4'], dayfirst=True)
    
    df = df.astype({'NIP':'string','Num_Sejour':'string','Poids_Dim_Soins':'float64','Poids_Dim_SOS':'float64','Séquence_Parcours':'string', \
                    'RessourceMedcode_refx':'string','INX_Code_Lib_refx':'string','INX_Code_Spe_refx':'string','INX_Code_Spe_refx':'string', \
                    'UFX_Code':'string','UFX_Code_Lib':'string','Service':'string','Service':'string','Activite':'string','Info complémentaire':'string', \
                    'Phase Parcours':'string','Dimension Parcours':'string','Localisation':'string','Code Loc':'string','Type tumeur':'string','Fonction':'string'})
    #df.rename(columns = {'ï»¿id_acte ':'ID_Acte'}, inplace = True)
    #print(df)

    
    
    
    print('-> Liste des entête du fichier CCAM.csv')
    print(df.dtypes)
    
    
    
    #ZONE OU L'ON PEUT AJOUTER DES TRANSFORMATIONS DE DONNEES ex : filtrage de la date de l'acte, suppression des espaces 
    #print('-> modification du df')
    #df['PA_NIP']=df.apply(lambda x: 'N' + x['PA_NIP'],axis=1)
    #df['HO_NUM']=df.apply(lambda x: 'N' + x['HO_NUM'],axis=1)
    #df['INXCODE']=df.apply(lambda x: x['INXCODE'].replace(" ", ""),axis=1)
    #df['HO_RECODE']=df.apply(lambda x: x['HO_RECODE'].replace(" ", ""),axis=1)

    #ajout de la colonne 'SOURCE' 
    df['SOURCE']='BDD_Acti'
    print(df)
    #affiche la longueur du fichier .csv
    chaine_txt= "Nombre de lignes du fichier : " + Nom_fichier_Acti + " est de " + str(len(df.index))
    print(chaine_txt)
 
    return df



def F_CR_Ext_csv_to_panda_df (Nom_fichier_CR, Chemin_Local_TBD):
    import pandas as pd
    #Fichier CR DXCARE 
    #print('-----------------------------')
        #Creation table d'import provisoire
    
    #Import du fichier .csv dans un dataframe
    Chemin_fich=Chemin_Local_TBD + '\\' + Nom_fichier_CR
    print(Chemin_fich)

    data=pd.read_csv(Chemin_fich,encoding='ISO-8859-1',sep=';') #lecteur du fichier .csv
    df=pd.DataFrame(data) #Intégration des données dans un dataframe panda
    
    #Definition des types de données du fichier.csv
    #pd.to_datetime(df['LI_DATE'], dayfirst=true)
    #df['LI_DATE']=pd.to_datetime(df['LI_DATE'],infer_datetime_format=False)
    df['DAT_VALEUR'] = pd.to_datetime(df['DAT_VALEUR'],yearfirst=True)

    df = df.astype({'LIB_TYPE_COUR':'string','ID_DOC_FIC':'string','LIB_DOC_FIC':'string','NIP':'string' })
    #print(df)

    #print('-> Liste des entête du fichier CCAM.csv')
    #print(df.dtypes)

    #ZONE OU L'ON PEUT AJOUTER DES TRANSFORMATIONS DE DONNEES ex : filtrage de la date de l'acte, suppression des espaces 
    #print('-> modification du df')
    """
    df['PA_NIP']=df.apply(lambda x: 'N' + x['PA_NIP'],axis=1)
    df['HO_NUM']=df.apply(lambda x: 'N' + x['HO_NUM'],axis=1)
    df['INXCODE']=df.apply(lambda x: x['INXCODE'].replace(" ", ""),axis=1)
    df['HO_RECODE']=df.apply(lambda x: x['HO_RECODE'].replace(" ", ""),axis=1)
    """

    #print(df)
    #df.loc[row,'AC_ASSOC'] -> Si NULL -> "" , SI Not NULL = même valeur    
    df['SOURCE']='DPI_CR'
    
    
    #affiche la longueur du fichier .csv
    chaine_txt= "Nombre de lignes du fichier : " + Nom_fichier_CR + " est de " + str(len(df.index))
    print(chaine_txt)
 
    return df

def F_LOG_RDV_csv_to_panda_df (Nom_fichier_CR, Chemin_Local_TBD):
    import pandas as pd
    #Fichier CR DXCARE 
    #print('-----------------------------')
        #Creation table d'import provisoire
    
    #Import du fichier .csv dans un dataframe
    Chemin_fich=Chemin_Local_TBD + '\\' + Nom_fichier_CR
    print(Chemin_fich)

    data=pd.read_csv(Chemin_fich,encoding='ISO-8859-1',sep=';') #lecteur du fichier .csv
    df=pd.DataFrame(data) #Intégration des données dans un dataframe panda
    #print(df)
    
    #Nettoyage des données, suppression des RDV "-"
    df2=df[(df['TYPEACTE']!='-')]
    df=df2
    
    #Definition des types de données du fichier.csv
    #pd.to_datetime(df['LI_DATE'], dayfirst=true)
    #df['LI_DATE']=pd.to_datetime(df['LI_DATE'],infer_datetime_format=False)
    df['DATE_CREATION'] = pd.to_datetime(df2['DATE_CREATION'],dayfirst=True)
    


    #TRAITEMENT DU CHAMP HORAIRE NECESSAIRE POUR LE CONVERTIR EN DATETIME
 
    df = df.astype({'TYPEACTE':'string','HORAIRE':'string','LIBELLE':'string','LIB_STATUT':'string','LIB_NATURE':'string','NIP':'string','COMMENTAIRE':'string' })
    #print(df)

    #TRAITEMENT DU CHAMP HORAIRE NECESSAIRE POUR LE CONVERTIR EN DATETIME

    #df['HORAIRE']=df.apply(lambda x: x['HORAIRE'].replace("-", "203001010000"),axis=1)
    df['HORAIRE']=df.apply(lambda x: x['HORAIRE'].replace("-", str(x['DATE_CREATION'])),axis=1)
    df['HORAIRE'] = pd.to_datetime(df['HORAIRE'],yearfirst=True)                                       

    #print('Dataframe avec conversion en datetime ')
    #print(df['HORAIRE'])
    #print(df['HORAIRE'].dtypes)


    #print('-> Liste des entête du fichier CCAM.csv')
    #print(df.dtypes)

    #ZONE OU L'ON PEUT AJOUTER DES TRANSFORMATIONS DE DONNEES ex : filtrage de la date de l'acte, suppression des espaces 
    #print('-> modification du df')
    """
    df['PA_NIP']=df.apply(lambda x: 'N' + x['PA_NIP'],axis=1)
    df['HO_NUM']=df.apply(lambda x: 'N' + x['HO_NUM'],axis=1)
    df['INXCODE']=df.apply(lambda x: x['INXCODE'].replace(" ", ""),axis=1)
    df['HO_RECODE']=df.apply(lambda x: x['HO_RECODE'].replace(" ", ""),axis=1)
    """

    #print(df)
    #df.loc[row,'AC_ASSOC'] -> Si NULL -> "" , SI Not NULL = même valeur    
    df['SOURCE']='LOG_DPI_RDV'
    
    
    #affiche la longueur du fichier .csv
    chaine_txt= "Nombre de lignes du fichier : " + Nom_fichier_CR + " est de " + str(len(df.index))
    print(chaine_txt)
 
    return df

def F_RDV_POST_TT_csv_to_panda_df (Nom_fichier_ACCAM, Chemin_Local_TBD):
    import pandas as pd
    #ETAPE 1 - Fichier post TT Angers
    #print('-----------------------------')
        #Creation table d'import provisoire
    
    #Import du fichier .csv dans un dataframe
    Chemin_fich=Chemin_Local_TBD + '\\' + Nom_fichier_ACCAM
    print(Chemin_fich)

    dataACCAM=pd.read_excel(Chemin_fich,sheet_name='Sheet1') #lecteur du fichier .xlsx
    df=pd.DataFrame(dataACCAM) #Intégration des données dans un dataframe panda
    
    #Definition des types de données du fichier.csv
    #pd.to_datetime(df['LI_DATE'], dayfirst=true)
    #df['LI_DATE']=pd.to_datetime(df['LI_DATE'],infer_datetime_format=False)
    df['DATE_ACTIVITE'] = pd.to_datetime(df['DATE_ACTIVITE'], dayfirst=False)
    df['DATE_CREATION'] = pd.to_datetime(df['DATE_CREATION'], dayfirst=False)
    
    
    df = df.astype({'NIP':'string','TYPE_ACTIVITE':'string','STATUT_RDV':'string','SOURCE':'string', \
                   'LIB_NATURE':'string','TYPE_LOG':'string','Commentaire':'string'})
    #print(df)

    #print('-> Liste des entête du fichier CCAM.csv')
    #print(df.dtypes)
    
    #ZONE OU L'ON PEUT AJOUTER DES TRANSFORMATIONS DE DONNEES ex : filtrage de la date de l'acte, suppression des espaces 
    #print('-> modification du df')
    #df['xxxx']=df.apply(lambda x: 'yyyy' + x['xxxx'],axis=1)
    

    #print(df)
    #df.loc[row,'AC_ASSOC'] -> Si NULL -> "" , SI Not NULL = même valeur    
    
    #ajout de la colonne 'SOURCE' 
    #df['SOURCE']='CCAM'
    
    #affiche la longueur du fichier .csv
    chaine_txt= "Nombre de lignes du fichier : " + Nom_fichier_ACCAM + " est de " + str(len(df.index))
    print(chaine_txt)
 
    return df





def P_df_to_SQL (Nom_table,df,cnxn,cursor,pyodbc,Type_Import):
        
    #suppression de la précédente table d'import
    requete_sql=F_SQL_Drop_table(Nom_table)
    F_SQL_execute(cnxn,cursor,requete_sql,pyodbc)

    if Type_Import=='CR':
    
        """ Requete insertion du fichier CSV sur le serveur SQL"""
        #Creation de la table de d'import 

        requete_sql=F_SQL_Creation_table_import_CR(Nom_table)
        F_SQL_execute(cnxn,cursor,requete_sql,pyodbc)
    
        #Import des données du .csv dans une table d'import sur le serveur sql
        F_SQL_Import_DF_dans_Table_Import_CR(Nom_table,df,cnxn,cursor,pyodbc)
    
    elif Type_Import=='CCAM':
        
        """ Requete insertion du fichier CSV sur le serveur SQL"""
        #Creation de la table de d'import (création )

        requete_sql=F_SQL_Creation_table_import_Activite(Nom_table)
        F_SQL_execute(cnxn,cursor,requete_sql,pyodbc)

        #Import des données du .csv dans une table d'import sur le serveur sql
        F_SQL_Import_DF_dans_Table_Import_CCAM(Nom_table,df,cnxn,cursor,pyodbc)
        
    elif Type_Import=='NGAP':
    
         """ Requete insertion du fichier CSV sur le serveur SQL"""
         #Creation de la table de d'import (création )
    
         requete_sql=F_SQL_Creation_table_import_Activite(Nom_table)
         F_SQL_execute(cnxn,cursor,requete_sql,pyodbc)
    
         #Import des données du .csv dans une table d'import sur le serveur sql
         F_SQL_Import_DF_dans_Table_Import_NGAP(Nom_table,df,cnxn,cursor,pyodbc)
    
    elif Type_Import=='MVT':
         """ Requete insertion du fichier CSV sur le serveur SQL"""
         #Creation de la table de d'import (création )
    
         requete_sql=F_SQL_Creation_table_import_Activite(Nom_table)
         F_SQL_execute(cnxn,cursor,requete_sql,pyodbc)
    
         #Import des données du .csv dans une table d'import sur le serveur sql
         F_SQL_Import_DF_dans_Table_Import_MVT(Nom_table,df,cnxn,cursor,pyodbc)
    else:
        print('P_df_to_SQL: Type _Import Inconnu')
    
    return

def F_Forme_Graphique(df,row):
    if df['SOURCE'].iloc[row]=='DPI_CR':
        Forme='x'
        #print('ligne' +str(row) + 'DPI_CR')
    elif df['SOURCE'].iloc[row]=='LOG_DPI_RDV':
        Forme='*'
        #print('ligne' + str(row) + 'LOG_DPI')
    elif df['SOURCE'].iloc[row]=='CCAM' and df['TYPE_ACTIVITE'].iloc[row+1]=='SCANNER':
        Forme='+'
        #print('ligne' + str(row) + 'CCAM/SCANNER')
    elif df['SOURCE'].iloc[row]=='CCAM' and df['TYPE_ACTIVITE'].iloc[row+1].find("CONSULT")!=-1:
        Forme='s'
        #print('ligne' + str(row) + 'CCAM/CONSULT')
    else:
        Forme='H'
        #print('ligne' + str(row) + 'AUTRE')
    return Forme


