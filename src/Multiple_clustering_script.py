#python script to perform multiple clusterings and store informations in mlflow and database

from datetime import datetime

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



#Open config.csv file as a dict
file_path = 'C:/Users/vince/Documents/DSTI/DSTI_Projects/ML ICO Activity/src/config_py.csv'  
config = pd.read_csv(file_path)

row_index = 0
Ac_config = config.iloc[row_index]

#MLFLOW SETUP
Experiment_name=Ac_config['Experiment']
Experiment_tag1=Ac_config['Experiment_tag_1']
Experiment_tag2=Ac_config['Experiment_tag_2']
Mcfconf.my_custom_func_MLFLOWconfig(Experiment_name,Experiment_tag1,Experiment_tag2)

#IMPORT DES DONNEES
DSprefix=Ac_config['DS_Prefix']
Create_dataset_parameters={DSprefix + 'My_NIP_filter_1rst_date': Ac_config['My_NIP_filter_1rst_date'],
                            DSprefix + 'My_NIP_filter_2nd_date_delta_in_days': int(Ac_config['My_NIP_filter_2nd_date_delta_in_days']),
                            DSprefix + 'Site': str(Ac_config['Site']),
                            DSprefix + 'Start_Window_time': Ac_config['Start_Window_time'],
                            DSprefix + 'End_Window_time': Ac_config['End_Window_time'],
                            }
myouputpath=Ac_config['myouputpath']

mlflow.log_params(Create_dataset_parameters)

print('Import données OK')


#PREPARE THE DATABASE
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
print('PREPARE THE DATABASE & STORE THE RESULT IN MLFLOW OK')


#Recuperer une table acte pour affichage parcours complet
Requete=Ac_config['T_Requete']
df_Actes_graph=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,'No')
Mcftsc.plot_carepath(df_Actes_graph,myouputpath+ Ac_config['T_Filename'])
mlflow.log_artifact(myouputpath+ Ac_config['T_Filename'], Ac_config['T_MlflowName'])
print('STORE table acte pour affichage parcours complet OK')


#Recuperer une table acte pour affichage parcours radiotherapie
Requete=Ac_config['P_Requete']
df_Actes_graph0=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,'No')
Mcftsc.plot_carepath(df_Actes_graph0,myouputpath+ Ac_config['P_Filename'])
mlflow.log_artifact(myouputpath+ Ac_config['P_Filename'], Ac_config['P_MlflowName'])
print('STORE table acte pour affichage parcours radiotherapie OK')



#DEFINTIION DU DICT DE CONFIG D'AGGLOMERATION
Aggreg_parameters, Aggprefix=Mcfconf.get_Aggreg_param(Ac_config)
#Define and save the aggregation parameters
Parameters_list=[Aggreg_parameters]
for param in Parameters_list:
    mlflow.log_params(param)
print('STORE aggregation parameters OK')


#Get the aggregation table
Aggreg_Patients=Mcftsc.get_Aggreg_Dataset2(Parameters_list,Aggprefix)
print('Get the aggregation table OK')
#Save the aggregation table !!!!!!  A DEPLACER A LA FIN DU NOTEBOOK POUR INTEGRER LES RESULTATS DE CLUSTERING
Aggreg_Patients['df'].to_csv(myouputpath + Ac_config['filename'])
mlflow.log_artifact(myouputpath + Ac_config['filename'], Ac_config['mlflowname'])
print('Save the aggregation table OK')



#### PREPARATION CLUSTERING DE FENETRE TEMPORELLE
My_List_NIP=Aggreg_Patients['df']['NIP']
#DDA_Clust=McfC.prepare_clust_DDA(Create_dataset_parameters,DSprefix,My_List_NIP)
DDT_Clust=McfC.prepare_clust_DDA_DDT(Create_dataset_parameters,DSprefix,My_List_NIP)
Time_Clust=DDT_Clust
#Clustering parameters
Time_Clust_parameters=Mcfconf.set_Time_clust_parameters(Ac_config)
#FIRST CLUSTERING (sub clust)
Aggreg_Time_clust=McfC.cluster(Aggreg_Patients,Time_Clust,False,mlflow,Time_Clust_parameters )
print('First clustering (Sub clust) OK')
#SAVE CLUSTERING TO BDD
Mydf=Aggreg_Time_clust['df_dist'][['NIP',Time_Clust_parameters['clust_name']]]
Mcfcp.Save_only_Cluster_to_Database(Mydf,Time_Clust_parameters, myouputpath, 'Tmp_' + Time_Clust_parameters['clust_name'] )
print('Save First clustering to BDD (Principal clust) OK')



##CLUSTERING DE PARCOURS - PREPARATION
dtw_param=Mcfconf.get_dtw_param(Ac_config)
#CALCUL DE LA MATRICE DE DISTANCE
dist_matrix=Mcftsc.GetDistanceMatrix(Aggreg_Patients, Aggreg_parameters,Aggprefix,dtw_param)
print('Calcul Matrice de distance OK')
#EXPORT DE LA MATRICE DE DISTANCE
#dist_matrix.tofile(myouputpath + "Matrice_distance.dat")
np.savetxt(myouputpath + "distance_matrix.csv",dist_matrix,delimiter=",")
mlflow.log_artifact(myouputpath + "distance_matrix.csv", "Matrice de distance inter-Parcours")
print('Save the Matrice de distance OK')
#Clustering parameters
Parcours_Clust_parameters=Mcfconf.set_parcours_clust_parameters(Ac_config)
#SECOND CLUSTERING (principal clust)
Aggreg_Parcours_clust=McfC.cluster(Aggreg_Patients,dist_matrix,False,mlflow,Parcours_Clust_parameters )
print('Second clustering (Principal clust) OK')
#SAVE CLUSTERING TO BDD
Mydf=Aggreg_Parcours_clust['df_dist'][['NIP',Parcours_Clust_parameters['clust_name']]]
Mcfcp.Save_only_Cluster_to_Database(Mydf,Parcours_Clust_parameters, myouputpath, 'Tmp_' + Parcours_Clust_parameters['clust_name'] )
print('Save Second clustering to BDD (Principal clust) OK')

#PLOT AND SAVE THE TS CURVES
Timesteps=int(Aggreg_parameters[Aggprefix + 'Stop_at_item'])-int(Aggreg_parameters[Aggprefix + 'Start_at_item'])
clust_name=Parcours_Clust_parameters['clust_name']
Parcours_nb_clusters={
    'nb_cluster' : Aggreg_Parcours_clust['Nb_clusters'],
    'Column_name' : Parcours_Clust_parameters['clust_name'],
}
Mcftsc.plot_TS_clusters(Aggreg_Parcours_clust,Timesteps,myouputpath+ 'TS_curves.png',Parcours_nb_clusters)
mlflow.log_artifact(myouputpath+ 'TS_curves.png', "TS_Curves_Clustering")
print('TS Curves - Plotting and Saving OK')


#PLOT CARTEPATHES
My_order=Ac_config['CPP_Order']
Table_name=Ac_config['CPP_Save_Tble_Name']
Requete=Ac_config['CPP_Requete']
Filter_df_col=Ac_config['CPP_Filter_df_col']
Filter_df_value=Ac_config['CPP_Filter_df_value']

#PREPARE THE DATASET TO BE PLOTED
Parcours_DF=Mcfcp.Prepare_Plot_carepath_clustered_2levels(df_Actes_graph,Aggreg_Parcours_clust,Parcours_Clust_parameters,Aggreg_Time_clust,Time_Clust_parameters, My_order)
Abcisses_DF, Plot_dict =Mcfcp.Compute_abcisses(Parcours_DF,Parcours_Clust_parameters,Time_Clust_parameters)
print('CPP - Computing Abcisses OK')
#SAVE THE CLUSTERING + PLOTTING VALUES TO THE DATABASE
Mcfcp.Save_Cluster_and_Carepath_to_Database(Abcisses_DF,Parcours_Clust_parameters,Time_Clust_parameters,myouputpath,Table_name)
print('CPP - Saving Abcisses to BDD OK')
#GET A DATASET OF ACTES
df_Actes_graph2=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,'No')
#FILTER THE DATASET IN ORDER NOT TO SHOW ALL ACTES
filtered_df = df_Actes_graph2[df_Actes_graph2[Filter_df_col] == Filter_df_value]
nip_no_treatment_info = df_Actes_graph2[~df_Actes_graph2['NIP'].isin(filtered_df['NIP'])]
nip_no_treatment_info = nip_no_treatment_info[['NIP', 'Clust', 'X_abscisse']].drop_duplicates()
final_df = pd.concat([filtered_df, nip_no_treatment_info], ignore_index=True)
final_df_sorted = final_df.sort_values(by='X_abscisse')
#PLOT AND SAVE IN MLFLOW
Mcfcp.plot_df_actes(final_df_sorted,Aggreg_Parcours_clust,Parcours_Clust_parameters,Aggreg_Time_clust,Time_Clust_parameters, Plot_dict, mlflow, myouputpath)
print('CPP - PLOTING AND SAVE TO MLFLOW OK')

# CLOSE THE MLFLOW
mlflow.end_run()

print('************   End of process - Thank you ! *****************')