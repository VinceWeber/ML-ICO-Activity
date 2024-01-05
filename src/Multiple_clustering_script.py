#python script to perform multiple clusterings and store informations in mlflow and database

from datetime import datetime
import time
#import pyodbc
import sqlalchemy
#import sqlalchemy as msql
#import Connexion_bdd as Cx_bdd
import numpy as np
import pandas as pd
import Requetes_SQL as Req_SQL
import matplotlib.pyplot as plt
import seaborn as sns
#import dtw as dtw
#import tsfresh #TimeSeries Transformation library
import my_custom_func_TS_Clust_1 as Mcftsc
import my_custom_func_Clustering as McfC
import my_custom_func_Carepath_plotting as Mcfcp
import my_custom_func_config as Mcfconf
import my_custom_func_batch_follow as Mcfbf
import mlflow

from sklearn import feature_selection
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.model_selection import cross_validate, cross_val_score, train_test_split, KFold, RepeatedKFold, RepeatedStratifiedKFold
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor, DecisionTreeClassifier, export_graphviz
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn import metrics, tree

#SET THE RANDOM SET FOR SKLEARN
#np.random.seed(0)


#Project Classes
#import FSQL_Classes as FSQLC
import Sql_Alchemy_Classes as AlSQL
#import Parcours_Classes as PC

import os
current_directory = os.getcwd()

#Open config.csv file as a dict
file_path = current_directory + "\\07-Batch_configuration\\export_config.csv"

config = pd.read_csv(file_path, encoding='ISO-8859-1')
#Add a function to chekc csv file
print(Mcfbf.myprint('Import csv batch file succeed', 1, 1))


total_index = len(config)


#Delete all existing result tables from database ************TO BE COMPLETED*******************
myRequete = 'EXECUTE dbo.Delete_TmpTables '
AlSQL.AlSQL_Execute(AlSQL.engine,myRequete,'No')

myRequete = 'EXECUTE dbo.Delete_MlflowTables '
AlSQL.AlSQL_Execute(AlSQL.engine,myRequete,'No')



for index,row in config.iterrows():
    #Initialize previous parameters variables
    if index!=0:
        Ac_config_old=config.iloc[index-1]
        First_run=False
    else:
        First_run=True

    Ac_config = config.iloc[index]
    print(Mcfbf.myprint('START ANALYSIS ', index, total_index))

    #MLFLOW SETUP
    Experiment_name=Ac_config['Experiment']
    Experiment_tag1=Ac_config['Experiment_tag_1']
    Experiment_tag2=Ac_config['Experiment_tag_2']
    Mcfconf.my_custom_func_MLFLOWconfig(Experiment_name,Experiment_tag1,Experiment_tag2)

    #IMPORT DES DONNEES
    DSprefix=Ac_config['DS_Prefix']
    myouputpath=Ac_config['myouputpath']
    #GET THE PARAMETERS
    Create_dataset_parameters=Mcfconf.get_Create_dataset_parameters(Ac_config)
    
    if First_run==False:
        old_dataset_parameters=Mcfconf.get_Create_dataset_parameters(Ac_config_old)
        if old_dataset_parameters!=Create_dataset_parameters:
            same_dataset=False
        else:
            same_dataset=True
    else:
        same_dataset=False

    mlflow.log_params(Create_dataset_parameters)
    print(Mcfbf.myprint('Import données OK', index, total_index))

    #PREPARE THE DATABASE
    print(Mcfbf.myprint('START TO PREPARE THE DATABASE & STORE THE RESULT IN MLFLOW ', index, total_index))
    start_time = time.time()

    if same_dataset and First_run==False:
        print(Mcfbf.myprint('SKIP PREPARE DATASET STEP Parameters are identical', index, total_index))
    else :
        
        Caracteristiques_Dataset=Mcftsc.Create_dataset(Create_dataset_parameters,DSprefix)

    #STORE THE RESULT IN MLFLOW
    df = pd.DataFrame.from_dict(Caracteristiques_Dataset)
    # Convert specified columns to dictionary
    mydict = df.iloc[:, 6:].to_dict(orient='list')
    # Filter dictionary to contain only float values and not lists
    for key, value in mydict.items():
        if len(value) == 1 :
            mydict[key] = value[0]

    mlflow.log_metrics(mydict)
    print(Mcfbf.myprint('PREPARE THE DATABASE & STORE THE RESULT IN MLFLOW OK ', index, total_index))
    elapsed_time = time.time() - start_time
    mlflow.log_metrics({'Time_STEP_DB_seconds' : elapsed_time }) 


    if Ac_config['T_Actes_Total']:
        #Recuperer une table acte pour affichage parcours complet
        Requete=Ac_config['T_Requete']
        
        if same_dataset and Requete==Ac_config_old['T_Requete'] and First_run==False:
            print(Mcfbf.myprint('SKIP parcours complet', index, total_index))
        else:
            df_Actes_graph=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,'No')
        
        Mcftsc.plot_carepath(df_Actes_graph,myouputpath+ Ac_config['T_Filename'],mlflow,'Plots')
        #mlflow.log_artifact(myouputpath+ Ac_config['T_Filename'], Ac_config['T_MlflowName'])
        print(Mcfbf.myprint('STORE table acte pour affichage parcours complet OK', index, total_index))

    if Ac_config['P_Specific_Plot']:
        #Recuperer une table acte pour affichage parcours radiotherapie
        Requete=Ac_config['P_Requete']

        if same_dataset and Requete==Ac_config_old['P_Requete'] and First_run==False:
            print(Mcfbf.myprint('SKIP parcours radiotherapie', index, total_index))
        else:
            df_Actes_graph0=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,'No')

        Mcftsc.plot_carepath(df_Actes_graph0,myouputpath+ Ac_config['P_Filename'],mlflow,'Plots')
        #Mcftsc.plot_carepath(df_Actes_graph0,myouputpath+ Ac_config['P_Filename'],mlflow,Ac_config['P_MlflowName'])
        #mlflow.log_artifact(myouputpath+ Ac_config['P_Filename'], Ac_config['P_MlflowName'])
        print(Mcfbf.myprint('STORE table acte pour affichage parcours radiotherapie OK', index, total_index))


    #DEFINTIION DU DICT DE CONFIG D'AGGLOMERATION
    Aggreg_parameters, Aggprefix=Mcfconf.get_Aggreg_param(Ac_config)
    
    #Define and save the aggregation parameters
    Parameters_list=[Aggreg_parameters]
    for param in Parameters_list:
        mlflow.log_params(param)
    print(Mcfbf.myprint('STORE aggregation parameters OK', index, total_index))


    print(Mcfbf.myprint('START TO Get the aggregation table ', index, total_index))
    start_time = time.time()

    #Get the aggregation table
    Aggreg_Patients=Mcftsc.get_Aggreg_Dataset2(Parameters_list,Aggprefix)
    print(Mcfbf.myprint('Get the aggregation table OK', index, total_index))
    elapsed_time = time.time() - start_time
    mlflow.log_metrics({'Time_STEP_Aggregation_seconds ' : elapsed_time }) 


    #Save the aggregation table !!!!!!  A DEPLACER A LA FIN DU NOTEBOOK POUR INTEGRER LES RESULTATS DE CLUSTERING ?
    Aggreg_Patients['df'].to_csv(myouputpath + Ac_config['filename'])
    #mlflow.log_artifact(myouputpath + Ac_config['filename'], Ac_config['mlflowname'])
    mlflow.log_artifact(myouputpath + Ac_config['filename'], 'Dataset_csv')
    print(Mcfbf.myprint('Save the aggregation table OK', index, total_index))

    if Ac_config['T_F_Cluster']:

        print(Mcfbf.myprint('START TimeWindow clustering ', index, total_index))
        start_time = time.time()

        #### PREPARATION CLUSTERING DE FENETRE TEMPORELLE
        My_List_NIP=Aggreg_Patients['df']['NIP']
        #DDA_Clust=McfC.prepare_clust_DDA(Create_dataset_parameters,DSprefix,My_List_NIP)
        DDT_Clust=McfC.prepare_clust_DDA_DDT(Create_dataset_parameters,DSprefix,My_List_NIP)
        Time_Clust=DDT_Clust
        #Clustering parameters
        Time_Clust_parameters=Mcfconf.set_Time_clust_parameters(Ac_config)
        #FIRST CLUSTERING (sub clust)
        Aggreg_Time_clust=McfC.cluster(Aggreg_Patients,Time_Clust,False,mlflow,Time_Clust_parameters )
        print(Mcfbf.myprint('First clustering (Sub clust) OK', index, total_index))
        elapsed_time = time.time() - start_time
        mlflow.log_metrics({'Time-TimeWindow_clustering_seconds' : elapsed_time }) 

        #SAVE CLUSTERING TO BDD
        #Mydf=Aggreg_Time_clust['df_dist'][['NIP',Time_Clust_parameters['clust_name']]]
        Mydf=Aggreg_Time_clust['df_dist']
        Mcfcp.Save_only_Cluster_to_Database(Mydf,Time_Clust_parameters, myouputpath, Time_Clust_parameters['Table_name'] )
        print(Mcfbf.myprint('Save First clustering to BDD (Principal clust) OK', index, total_index))

    if Ac_config['T_Dist_Cluster']:
        
        print(Mcfbf.myprint('START Parcours clustering ', index, total_index))
        start_time = time.time()
        
        ##CLUSTERING DE PARCOURS - PREPARATION
        dtw_param=Mcfconf.get_dtw_param(Ac_config)
        #CALCUL DE LA MATRICE DE DISTANCE
        dist_matrix=Mcftsc.GetDistanceMatrix(Aggreg_Patients, Aggreg_parameters,Aggprefix,dtw_param)
        print(Mcfbf.myprint('Calcul Matrice de distance OK', index, total_index))

        #EXPORT DE LA MATRICE DE DISTANCE
        #dist_matrix.tofile(myouputpath + "Matrice_distance.dat")
        np.savetxt(myouputpath + "distance_matrix.csv",dist_matrix,delimiter=",")
        mlflow.log_artifact(myouputpath + "distance_matrix.csv", "Matrice de distance inter-Parcours")
        print(Mcfbf.myprint('Save the Matrice de distance OK', index, total_index))

        #Clustering parameters
        Parcours_Clust_parameters=Mcfconf.set_parcours_clust_parameters(Ac_config)
        #SECOND CLUSTERING (principal clust)
        Aggreg_Parcours_clust=McfC.cluster(Aggreg_Patients,dist_matrix,False,mlflow,Parcours_Clust_parameters )
        print(Mcfbf.myprint('Second clustering (Principal clust) OK', index, total_index))
        elapsed_time = time.time() - start_time
        mlflow.log_metrics({'Time-Parcours_clustering_seconds' : elapsed_time })

        #SAVE CLUSTERING TO BDD
        #Mydf=Aggreg_Parcours_clust['df_dist'][['NIP',Parcours_Clust_parameters['clust_name']]]
        non_integer_columns = [col for col in Aggreg_Parcours_clust['df_dist'].columns if not isinstance(col, int)]
        Mydf=Aggreg_Parcours_clust['df_dist'][non_integer_columns]
        Mcfcp.Save_only_Cluster_to_Database(Mydf,Parcours_Clust_parameters, myouputpath, Parcours_Clust_parameters['Table_name'] )
        print(Mcfbf.myprint('Save Second clustering to BDD (Principal clust) OK', index, total_index))

        #PLOT AND SAVE THE TS CURVES
        Timesteps=int(Aggreg_parameters[Aggprefix + 'Stop_at_item'])-int(Aggreg_parameters[Aggprefix + 'Start_at_item'])
        clust_name=Parcours_Clust_parameters['clust_name']
        Parcours_nb_clusters={
            'nb_cluster' : Aggreg_Parcours_clust['Nb_clusters'],
            'Column_name' : Parcours_Clust_parameters['clust_name'],
        }
        Mcftsc.plot_TS_clusters(Aggreg_Parcours_clust,Timesteps,myouputpath+ 'TS_curves.png',Parcours_nb_clusters,mlflow,'Plots')
        #Mcftsc.plot_TS_clusters(Aggreg_Parcours_clust,Timesteps,myouputpath+ 'TS_curves.png',Parcours_nb_clusters,mlflow,"TS_Curves_Clustering")
        print(Mcfbf.myprint('TS Curves - Plotting and Saving OK', index, total_index))

    if Ac_config['CPP_Plot']:

        CPP_Param=Mcfconf.set_CPP_Plot_parameters(Ac_config)

        #PLOT CARTEPATHES
        #My_order=Ac_config['CPP_Order']
        #Table_name=Ac_config['CPP_Save_Tble_Name']
        #Requete=Ac_config['CPP_Requete']
        Filter_df_col=CPP_Param['CPP_Filter_df_col']
        Filter_df_value=CPP_Param['CPP_Filter_df_value']


        # **************** ADD HERE WHAT TO DO IF Clust 1 only, or Clust1+Clust2 set in CPP Parameters 
        # 1 LEVEL (PARCOURS CLUSTERING)
        if CPP_Param['primary_clust_name']!='NO_VALUE':
            #Get df_parcours from database
            myRequete="""SELECT * FROM [ICO_Activite].[dbo].[""" + CPP_Param['primary_clust_TableName'] +"""]"""
            df_Parcours_clust=AlSQL.AlSQL_Requete(AlSQL.engine,myRequete,'No')
            nameToBeSaved=CPP_Param['primary_clust_TableName'] + '_CPP'

            Requete="""SELECT Table_Acte.[NIP]  ,
            Table_Cluster.""" + CPP_Param['primary_clust_name'] + """ as Clust  ,
            Table_Cluster.X_abscisse     ,
            Table_Acte.[J_Parcours_V1]      ,
            Table_Acte.[J_Parcours_V3]     ,
            Table_Acte.[Service]      ,
            Table_Acte.[Activite]      ,
            Table_Acte.[Phase]     ,
            Table_Acte.[Dimension]      ,
            Table_Acte.[Type_seq]  

            FROM [ICO_Activite].[dbo].[Tmp_Carac_Actes] as Table_Acte ,
            [ICO_Activite].[dbo].[""" + nameToBeSaved + """] as Table_Cluster

            WHERE Table_Cluster.NIP = Table_Acte.NIP  

            ORDER BY Clust asc ,Table_Acte.[J_Parcours_V1] desc, Table_Acte.[NIP]"""


            Mcfcp.Prepare_Save_Plot_one_clust(df_Actes_graph,df_Parcours_clust,CPP_Param['primary_clust_name'], CPP_Param['CPP_order'],myouputpath,nameToBeSaved,Requete,Filter_df_col,Filter_df_value,mlflow, nameToBeSaved)
            print(Mcfbf.myprint('Primary Clust CPP - Plotting and Saving OK', index, total_index))
        

        # 1 LEVEL (TIME CLUSTERING)
        if CPP_Param['sub_clust_name']!='NO_VALUE':
            #Get df_parcours from database
            myRequete="""SELECT * FROM [ICO_Activite].[dbo].[""" + CPP_Param['sub_clust_TableName'] +"""]"""
            df_Parcours_clust=AlSQL.AlSQL_Requete(AlSQL.engine,myRequete,'No')
            nameToBeSaved=CPP_Param['sub_clust_TableName'] + '_CPP'

            Requete="""SELECT Table_Acte.[NIP]  ,
            Table_Cluster.""" + CPP_Param['sub_clust_name'] + """ as Clust  ,
            Table_Cluster.X_abscisse     ,
            Table_Acte.[J_Parcours_V1]      ,
            Table_Acte.[J_Parcours_V3]     ,
            Table_Acte.[Service]      ,
            Table_Acte.[Activite]      ,
            Table_Acte.[Phase]     ,
            Table_Acte.[Dimension]      ,
            Table_Acte.[Type_seq]  

            FROM [ICO_Activite].[dbo].[Tmp_Carac_Actes] as Table_Acte ,
            [ICO_Activite].[dbo].[""" + nameToBeSaved + """] as Table_Cluster

            WHERE Table_Cluster.NIP = Table_Acte.NIP  

            ORDER BY Clust asc ,Table_Acte.[J_Parcours_V1] desc, Table_Acte.[NIP]"""

            Mcfcp.Prepare_Save_Plot_one_clust(df_Actes_graph,df_Parcours_clust,CPP_Param['sub_clust_name'], CPP_Param['CPP_order'],myouputpath,nameToBeSaved,Requete,Filter_df_col,Filter_df_value,mlflow, nameToBeSaved)
            print(Mcfbf.myprint('Subclust CPP - Plotting and Saving OK', index, total_index))

        
        # 2 LEVELS (TIME + PARCOURS CLUSTERING)
        if CPP_Param['sub_clust_name']!='NO_VALUE' and CPP_Param['primary_clust_name']!='NO_VALUE':
            #Get df_parcours from database
            myRequete="""SELECT * FROM [ICO_Activite].[dbo].[""" + CPP_Param['primary_clust_TableName'] + """]"""
            cluster1_Table=AlSQL.AlSQL_Requete(AlSQL.engine,myRequete,'No')
            myRequete="""SELECT * FROM [ICO_Activite].[dbo].[""" + CPP_Param['sub_clust_TableName'] + """]"""
            cluster2_Table=AlSQL.AlSQL_Requete(AlSQL.engine,myRequete,'No')
            nameToBeSaved=CPP_Param['CPP_Table_Name'] + CPP_Param['primary_clust_name'] +"-" + CPP_Param['sub_clust_name'] + '_CPP'


            Requete="""SELECT Table_Acte.[NIP]  ,
            Table_Cluster.""" + CPP_Param['primary_clust_name'] + """ as Clust1  ,
            Table_Cluster.""" + CPP_Param['sub_clust_name'] + """ as Clust2  ,
            Table_Cluster.X_abscisse     ,
            Table_Acte.[J_Parcours_V1]      ,
            Table_Acte.[J_Parcours_V3]     ,
            Table_Acte.[Service]      ,
            Table_Acte.[Activite]      ,
            Table_Acte.[Phase]     ,
            Table_Acte.[Dimension]      ,
            Table_Acte.[Type_seq]  

            FROM [ICO_Activite].[dbo].[Tmp_Carac_Actes] as Table_Acte ,
            [ICO_Activite].[dbo].[""" + CPP_Param['CPP_Table_Name'] + """] as Table_Cluster

            WHERE Table_Cluster.NIP = Table_Acte.NIP  

            ORDER BY Table_Acte.[J_Parcours_V1] desc, Table_Acte.[NIP]"""
            
            Mcfcp.Prepare_Save_Plot_twice_clustered(df_Actes_graph,cluster1_Table,cluster2_Table,CPP_Param,Requete,myouputpath,mlflow,nameToBeSaved)
            print(Mcfbf.myprint('Primary And Subclust CPP - Plotting and Saving OK', index, total_index))

    if Ac_config['FPP_Plot']:
        FPP_Param=Mcfconf.set_FPP_Plot_parameters(Ac_config)

        if FPP_Param['clust1TableName']!='NO_VALUE':

            nameToBeSaved=FPP_Param['FPP_Name']

            tables_list =[]
            cluster_names= []

            if FPP_Param['clust1TableName']!='NO_VALUE':
                #Get Parcours Clusterings
                myRequete="""SELECT * FROM [ICO_Activite].[dbo].[""" + FPP_Param['clust1TableName'] + """]"""
                cluster1_Table=AlSQL.AlSQL_Requete(AlSQL.engine,myRequete,'No')
                tables_list.append(cluster1_Table)
                cluster_names.append(FPP_Param['clust1Name'])

            if FPP_Param['clust2TableName']!='NO_VALUE':
                myRequete="""SELECT * FROM [ICO_Activite].[dbo].[""" + FPP_Param['clust2TableName'] + """]"""
                cluster2_Table=AlSQL.AlSQL_Requete(AlSQL.engine,myRequete,'No')
                tables_list.append(cluster2_Table)
                cluster_names.append(FPP_Param['clust2Name'])

            if FPP_Param['clust3TableName']!='NO_VALUE':    
                myRequete="""SELECT * FROM [ICO_Activite].[dbo].[""" + FPP_Param['clust3TableName'] + """]"""
                cluster3_Table=AlSQL.AlSQL_Requete(AlSQL.engine,myRequete,'No')
                tables_list.append(cluster3_Table)
                cluster_names.append(FPP_Param['clust3Name'])

            # Call the function to merge the tables
            result_table = Mcfcp.FPP_merge_tables(tables_list, 'NIP', cluster_names)

            #Sauvegarder dans la BDD la nouvelle table - Cluster
            Table_Cluster=FPP_Param['FPP_Table_Name']
            Requete = 'EXECUTE dbo.Delete_Table_if_exists ' + Table_Cluster
            with AlSQL.engine.begin() as conn:
                        conn.execute(sqlalchemy.text(Requete))
            #my_df[['NIP',principal_clust_name]].to_sql(Table_Cluster,AlSQL.engine)
            result_table.to_sql(Table_Cluster,AlSQL.engine)

            Requete="""SELECT Table_Acte.[NIP]  ,
                Table_Cluster.""" + 'Concat_' + FPP_Param['clust1Name'] + """ as Clust  ,

                Table_Cluster.X_abscisse     ,
                Table_Acte.[J_Parcours_V1]      ,
                Table_Acte.[J_Parcours_V3]     ,
                Table_Acte.[Service]      ,
                Table_Acte.[Activite]      ,
                Table_Acte.[Phase]     ,
                Table_Acte.[Dimension]      ,
                Table_Acte.[Type_seq]  

                FROM [ICO_Activite].[dbo].[Tmp_Carac_Actes] as Table_Acte ,
                [ICO_Activite].[dbo].[""" + FPP_Param['FPP_Table_Name'] + """] as Table_Cluster

                WHERE Table_Cluster.NIP = Table_Acte.NIP  

                ORDER BY Table_Acte.[J_Parcours_V1] desc, Table_Acte.[NIP]"""

            Filter_df_col=FPP_Param['FPP_Filter_df_col']
            Filter_df_value=FPP_Param['FPP_Filter_df_value']
            clustname='Concat_' + FPP_Param['clust1Name']

            Mcfcp.Prepare_Save_Plot_one_clust(df_Actes_graph,result_table,clustname,FPP_Param['FPP_order'],myouputpath,FPP_Param['FPP_Table_Name'],Requete,Filter_df_col,Filter_df_value,mlflow,nameToBeSaved)
            print(Mcfbf.myprint('FPP Plotting Done ! - Plotting and Saving OK', index, total_index))

    # CLOSE THE MLFLOW
    mlflow.end_run()

print('************   End of process - Thank you ! *****************')