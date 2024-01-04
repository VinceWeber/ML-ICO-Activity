#MODULE DEDICATED FOR PLOTTING CAREPATHES
#V.WEBER 12/2023


def Prepare_Plot_carepath_clustered_2levels(df_actes,Aggreg_principal_clust,Principal_clust_param,Aggreg_sub_clust,Sub_Clust_param, Trace_Order):
    #Prepare Plot_carepath_clustered_2levels
    #input Variables :
    # principal_clust_name
    # sub_clust_name
    # Trace order

    #Input dataset :
    # df_Actes_graph (liste d'actes avec les colonnes NIP, J_Parcours_V1, J_parcours_V3)
    # Aggreg_patients_clustered
    
    # output : Dataset sorted by principal cluster, subcluster, trace order (ready for abcisse computing)

    import pandas as pd

    #Definitions:
    principal_clust_name=Principal_clust_param['clust_name']
    sub_clust_name=Sub_Clust_param['clust_name']

    # For One Clust Name (we can add clust and sub clust)
    #principal clust
    df_Parcours_plot=pd.merge(df_actes[['NIP','J_Parcours_V1','J_Parcours_V3']],Aggreg_principal_clust['df'][['NIP',principal_clust_name]], on='NIP',how='left')
    #sub clust
    df_Parcours_plot=pd.merge(df_Parcours_plot,Aggreg_sub_clust['df'][['NIP',sub_clust_name]], on='NIP',how='left')
    #merge on 'right' will add the average NIP lines (1 AVG nip for 1 cluster), these individuals will get J_Parcours_V1 and V3=0

    agg_functions_max = { 
        'J_Parcours_V1': 'max',
        'J_Parcours_V3': 'max',
        principal_clust_name: 'max',
        sub_clust_name: 'max'
    }
    agg_functions_min = { 
        'J_Parcours_V1': 'min',
        'J_Parcours_V3': 'min',
    }
    df_Parcours_plot_max = df_Parcours_plot.groupby('NIP').agg(agg_functions_max)
    df_Parcours_plot_max = df_Parcours_plot_max.rename(columns={'J_Parcours_V1': 'Max_J_V1','J_Parcours_V3': 'Max_J_V3'})

    df_Parcours_plot_min = df_Parcours_plot[['NIP','J_Parcours_V1','J_Parcours_V3']].groupby('NIP').agg(agg_functions_min)
    df_Parcours_plot_min = df_Parcours_plot_min.rename(columns={'J_Parcours_V1': 'Min_J_V1','J_Parcours_V3': 'Min_J_V3'})
    df_Parcours_plot1 = pd.concat([df_Parcours_plot_min, df_Parcours_plot_max], axis=1)

    df_Parcours_plot1['NIP']=df_Parcours_plot1.index
    df_Parcours_plot1.reset_index(drop=True, inplace=True)
    df_Parcours_plot1.sort_values([principal_clust_name,sub_clust_name, Trace_Order], ascending=[True,True, False], inplace=True)

    return df_Parcours_plot1

def Prepare_Plot_carepath_twice_clustered(df_actes,Principal_clust,Sub_clust,CPP_two_Clust_param):
    #Prepare Plot_carepath_clustered_2levels
    #input Variables :
    # principal_clust_name
    # sub_clust_name
    # Trace order

    #Input dataset :
    # df_Actes_graph (liste d'actes avec les colonnes NIP, J_Parcours_V1, J_parcours_V3) ***** J_Parcours_V1 ou V3 has to be a parameter! ****
    # Aggreg_patients_clustered
    
    # output : Dataset sorted by principal cluster, subcluster, trace order (ready for abcisse computing)

    import pandas as pd

    #Definitions:
    principal_clust_name=CPP_two_Clust_param['primary_clust_name']
    sub_clust_name=CPP_two_Clust_param['sub_clust_name']
    Trace_Order=CPP_two_Clust_param['CPP_order']

    # For One Clust Name (we can add clust and sub clust)
    #principal clust
    df_Parcours_plot=pd.merge(df_actes[['NIP','J_Parcours_V1','J_Parcours_V3']],Principal_clust[['NIP',principal_clust_name]], on='NIP',how='left')
    #sub clust
    df_Parcours_plot=pd.merge(df_Parcours_plot,Sub_clust[['NIP',sub_clust_name]], on='NIP',how='left')
    #merge on 'right' will add the average NIP lines (1 AVG nip for 1 cluster), these individuals will get J_Parcours_V1 and V3=0

    agg_functions_max = { 
        'J_Parcours_V1': 'max',
        'J_Parcours_V3': 'max',
        principal_clust_name: 'max',
        sub_clust_name: 'max'
    }
    agg_functions_min = { 
        'J_Parcours_V1': 'min',
        'J_Parcours_V3': 'min',
    }
    df_Parcours_plot_max = df_Parcours_plot.groupby('NIP').agg(agg_functions_max)
    df_Parcours_plot_max = df_Parcours_plot_max.rename(columns={'J_Parcours_V1': 'Max_J_V1','J_Parcours_V3': 'Max_J_V3'})

    df_Parcours_plot_min = df_Parcours_plot[['NIP','J_Parcours_V1','J_Parcours_V3']].groupby('NIP').agg(agg_functions_min)
    df_Parcours_plot_min = df_Parcours_plot_min.rename(columns={'J_Parcours_V1': 'Min_J_V1','J_Parcours_V3': 'Min_J_V3'})
    df_Parcours_plot1 = pd.concat([df_Parcours_plot_min, df_Parcours_plot_max], axis=1)

    df_Parcours_plot1['NIP']=df_Parcours_plot1.index
    df_Parcours_plot1.sort_values([principal_clust_name,sub_clust_name, Trace_Order], ascending=[True,True, False], inplace=True)
    df_Parcours_plot1.reset_index(drop=True, inplace=True)
    

    return df_Parcours_plot1


def Prepare_Plot_carepath_one_cluster(df_actes,Table_NIP_Clust,clustName, Trace_Order):
    #Prepare Plot_carepath_clustered_1level
    #input Variables :
    # principal_clust_name
    # Trace order

    #Input dataset :
    # df_Actes_graph (liste d'actes avec les colonnes NIP, J_Parcours_V1, J_parcours_V3) ***** J_Parcours_V1 ou V3 has to be a parameter! ****
    # Aggreg_patients_clustered
    
    # output : Dataset sorted by principal cluster, trace order (ready for abcisse computing)

    import pandas as pd

    #Definitions:
    principal_clust_name=clustName

    # For One Clust Name (we can add clust and sub clust)
    #principal clust
    df_Parcours_plot=pd.merge(df_actes[['NIP','J_Parcours_V1','J_Parcours_V3']],Table_NIP_Clust[['NIP',principal_clust_name]], on='NIP',how='left')

    agg_functions_max = { 
        'J_Parcours_V1': 'max',
        'J_Parcours_V3': 'max',
        principal_clust_name: 'max',
    }
    agg_functions_min = { 
        'J_Parcours_V1': 'min',
        'J_Parcours_V3': 'min',
    }
    df_Parcours_plot_max = df_Parcours_plot.groupby('NIP').agg(agg_functions_max)
    df_Parcours_plot_max = df_Parcours_plot_max.rename(columns={'J_Parcours_V1': 'Max_J_V1','J_Parcours_V3': 'Max_J_V3'})

    df_Parcours_plot_min = df_Parcours_plot[['NIP','J_Parcours_V1','J_Parcours_V3']].groupby('NIP').agg(agg_functions_min)
    df_Parcours_plot_min = df_Parcours_plot_min.rename(columns={'J_Parcours_V1': 'Min_J_V1','J_Parcours_V3': 'Min_J_V3'})
    df_Parcours_plot1 = pd.concat([df_Parcours_plot_min, df_Parcours_plot_max], axis=1)

    df_Parcours_plot1['NIP']=df_Parcours_plot1.index
    df_Parcours_plot1.sort_values([principal_clust_name, Trace_Order], ascending=[True, False], inplace=True)
    df_Parcours_plot1.reset_index(drop=True, inplace=True)

    return df_Parcours_plot1



def Compute_abcisses(Parcours_Dataset,Principal_clust_param,Sub_Clust_param ):
    #Abcisse computing 
    #Input : 
    # Parcours plot : dataset sorted by principal cluster, subcluster, trace order (ready for abcisse computing)
    # Principal cluster name
    # Sub Clust name

    #Ouput :
    # Dataset Parcours plot enlarged with a new column 'abcisse'


    # definition
    my_df=Parcours_Dataset
    principal_clust_name=Principal_clust_param['clust_name']
    sub_clust_name=Sub_Clust_param['clust_name']

    # Creation de l'abscisee du graph

    Nb_NIP=len(my_df)

    #First STEP
    old_NIP=''
    old_Cluster=-1
    old_Cluster2=-1
    x_values=[]
    xx_values=[]
    x=0
    NIP_Step = 100/Nb_NIP

    for index,row in my_df.iterrows():
        
        if old_Cluster!=row[principal_clust_name]:
            x += NIP_Step
            x_values.append(x)  #crée une nouvelle ligne verticale de Cluster N1
            my_df.at[index,'X_abscisse']=x
        elif old_Cluster2!=row[sub_clust_name]:
            x += NIP_Step
            xx_values.append(x) #crée une nouvelle ligne verticale de Cluster N2
            my_df.at[index,'X_abscisse']=x
        else:    
            x += NIP_Step
            my_df.at[index,'X_abscisse']=x
        
        my_df.at[index,'X_abscisse']=x
        old_NIP=row['NIP']
        old_Cluster=row[principal_clust_name]
        old_Cluster2=row[sub_clust_name]
    
        plot_dict={
             'x_values': x_values,
             'xx_values':xx_values
        }

    return my_df, plot_dict


def Compute_abcisses_One_cluster(Parcours_Dataset,Clust_name):
    #Abcisse computing 
    #Input : 
    # Parcours plot : dataset sorted by principal cluster, subcluster, trace order (ready for abcisse computing)
    # Principal cluster name

    #Ouput :
    # Dataset Parcours plot enlarged with a new column 'abcisse'

    # definition
    my_df=Parcours_Dataset
    principal_clust_name=Clust_name

    # Creation de l'abscisee du graph
    #Nb_NIP=len(my_df)
    nbNip=len(my_df['NIP'].unique())
    Nb_Cluster=len(my_df[Clust_name].unique())
    nipStep = 100/nbNip

    #For each cluster get the index of the first NIP
    x_values=[]
    for cluster in range(Nb_Cluster):
        clustMinIndex=my_df[my_df[principal_clust_name]==cluster].index.min()
        x_values.append(clustMinIndex * nipStep)

    my_df['X_abscisse']=my_df.index*nipStep
    
    plot_dict={
             'x_values': x_values
        }

    return my_df, plot_dict

def Compute_abcisses_two_clusters(Parcours_Dataset,CPP_two_Clust_param):
    #Abcisse computing 
    #Input : 
    # Parcours plot : dataset sorted by principal cluster, subcluster, trace order (ready for abcisse computing)
    # Principal cluster name
    # Sub Clust name

    #Ouput :
    # Dataset Parcours plot enlarged with a new column 'abcisse'

    # definition
    my_df=Parcours_Dataset
    principal_clust_name=CPP_two_Clust_param['primary_clust_name']
    subclust_name=CPP_two_Clust_param['sub_clust_name']

    # Creation de l'abscisee du graph
    #Nb_NIP=len(my_df)
    nbNip=len(my_df['NIP'].unique())
    nipStep = 100/nbNip

    Nb_Pincipal_Cluster=len(my_df[principal_clust_name].unique())
    Nb_Sub_Cluster=len(my_df[subclust_name].unique())

    #For each cluster get the index of the first NIP
    x_values=[]
    xx_values=[]

    for P_cluster in range(Nb_Pincipal_Cluster):
        clustMinIndex=my_df[my_df[principal_clust_name]==P_cluster ].index.min()
        x_values.append(clustMinIndex * nipStep)

    for P_cluster in range(Nb_Pincipal_Cluster):
        for S_Cluster in range(Nb_Sub_Cluster):
            clustMinIndex=my_df[(my_df[principal_clust_name] == P_cluster) & (my_df[subclust_name] == S_Cluster)].index.min()
            xx_values.append(clustMinIndex * nipStep)

    my_df['X_abscisse']=my_df.index*nipStep
    
    plot_dict={
             'x_values': x_values,
             'xx_values': xx_values
        }
    
    return my_df, plot_dict


def Save_only_Cluster_to_Database(Mydf,Clust_param, myouputpath,table_name ):
    #Input : 
    # Principal cluster name
    # Sub Clust name 
    # mydf : Dataset with colums : NIP, Cluster name

    import Sql_Alchemy_Classes as AlSQL
    import sqlalchemy
    import mlflow
    
    #defintion
    my_df=Mydf
    principal_clust_name=Clust_param['clust_name']

    #Sauvegarder dans la BDD l'association NIP - Cluster
    Table_Cluster=table_name 
    Requete = 'EXECUTE dbo.Delete_Table_if_exists ' + Table_Cluster
    with AlSQL.engine.begin() as conn:
                conn.execute(sqlalchemy.text(Requete))
    #my_df[['NIP',principal_clust_name]].to_sql(Table_Cluster,AlSQL.engine)
    my_df.to_sql(Table_Cluster,AlSQL.engine)
    
    my_df[['NIP',principal_clust_name]].to_csv(myouputpath + principal_clust_name + '_clust.csv')
    mlflow.log_artifact(myouputpath + principal_clust_name + '_clust.csv', 'Dataset_csv')
    #mlflow.log_artifact(myouputpath + principal_clust_name + '_clust.csv', principal_clust_name + "-Cluster Ouput")

    return


def Save_Cluster_and_Carepath_to_Database_twice_clustered(Abcisses_DF,principalClustName,subClustName, myouputpath,tableName ):
    #Input : 
    # Principal cluster name
    # Sub Clust name 
    # mydf : Dataset with colums : NIP, Principal cluster name, Sub Clust name , X_abcisse

    import Sql_Alchemy_Classes as AlSQL
    import sqlalchemy
    import mlflow
    
    #defintion
    my_df=Abcisses_DF

    #Sauvegarder dans la BDD l'association NIP - Cluster
    Table_Cluster=tableName 
    Requete = 'EXECUTE dbo.Delete_Table_if_exists ' + Table_Cluster
    with AlSQL.engine.begin() as conn:
                conn.execute(sqlalchemy.text(Requete))
    my_df[['NIP',principalClustName,subClustName,'X_abscisse']].to_sql(Table_Cluster,AlSQL.engine)

    my_df[['NIP',principalClustName,subClustName,'X_abscisse']].to_csv(myouputpath + principalClustName + '-' + subClustName +'_abscisses.csv')
    mlflow.log_artifact(myouputpath + principalClustName + '-' + subClustName +'_abscisses.csv', 'Dataset_csv')
    #mlflow.log_artifact(myouputpath + principal_clust_name + '-' + sub_clust_name +'_abscisses.csv', "Cluster Ouput")

    return

def Save_Cluster_and_Carepath_to_Database_one_clust(Abcisses_DF,clustName, myouputpath,Table_Cluster ):
    #Input : 
    # Principal cluster name
    # Sub Clust name 
    # mydf : Dataset with colums : NIP, Principal cluster name, Sub Clust name , X_abcisse

    import Sql_Alchemy_Classes as AlSQL
    import sqlalchemy
    import mlflow
    
    #defintion
    my_df=Abcisses_DF
    principal_clust_name=clustName

    #Sauvegarder dans la BDD l'association NIP - Cluster
    Requete = 'EXECUTE dbo.Delete_Table_if_exists ' + Table_Cluster
    with AlSQL.engine.begin() as conn:
                conn.execute(sqlalchemy.text(Requete))
    my_df[['NIP',principal_clust_name,'X_abscisse']].to_sql(Table_Cluster,AlSQL.engine)

    my_df[['NIP',principal_clust_name,'X_abscisse']].to_csv(myouputpath + principal_clust_name + '_abscisses.csv')
    mlflow.log_artifact(myouputpath + principal_clust_name +'_abscisses.csv', 'Dataset_csv')
    #mlflow.log_artifact(myouputpath + principal_clust_name +'_abscisses.csv', "Cluster Ouput")

    return



def plot_df_actes(Df,Aggreg_principal_clust,Principal_clust_param,Aggreg_sub_clust,Sub_Clust_param, Plot_dict,mlflow, myouputpath):
    #Recuperer une table acte avec les clusters
    #Requete="""SELECT Table_Acte.[NIP]
    #    ,Table_Cluster.***Nom du cluster***
    #    ,Table_Cluster.X_abscisse
    #    ,Table_Acte.[J_Parcours_V1]
    #    ,Table_Acte.[J_Parcours_V3]
    #    ,Table_Acte.[Service]
    #    ,Table_Acte.[Activite]
    #    ,Table_Acte.[Phase]
    #    ,Table_Acte.[Dimension]
    #    ,Table_Acte.[Type_seq]
    #FROM [ICO_Activite].[dbo].[Tmp_Carac_Actes] as Table_Acte
    #    , [ICO_Activite].[dbo].[***NOM de la table de clustering***] as Table_Cluster
    #
    #WHERE Table_Cluster.NIP = Table_Acte.NIP
    #        AND Table_Acte.[Phase]='Traitement'
    #ORDER BY Table_Cluster.Cluster asc ,Table_Acte.[J_Parcours_V1] desc, Table_Acte.[NIP]
    #"""
    #df_Actes_graph2=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,'No')


    #Function to plot a Dataset from df_Actes
    #Input_DAta: 
    # df_actes  a data frame with a specific shape get from an SQL query (In comment below)
    # plot_dict : a dict containing x and xx values (position of the vertical red and green lines)

    import matplotlib.pyplot as plt
    import seaborn as sns

    df_actes=Df
    x_values=Plot_dict['x_values']
    xx_values=Plot_dict['xx_values']
    NB_NIP=len(df_actes['NIP'].unique())

    Cluster_name=Principal_clust_param['clust_name']
    n_clusters2=Aggreg_principal_clust['Nb_clusters']
    NIP_Step=100/NB_NIP

    #PLOT THE DATA
    fig, axs = plt.subplots(1, 1, figsize=(15, 8))
    axs.set_title('Carepathes')
    scatter=sns.scatterplot(data=df_actes, x=df_actes.X_abscisse, y='J_Parcours_V1',markers='Activite', hue='Service')

    # Changing X and Y axis labels
    scatter.set_xlabel('%')  # Change X axis label
    scatter.set_ylabel('J_Parcours_V1')  # Change Y axis label

    # Ajoutez la ligne verticale
    for x_value in x_values:
        axs.axvline(x=x_value, color='red', linestyle='--') #, label=f'Vertical Line at x={x_value}')

    # Ajoutez la ligne verticale
    for xx_value in xx_values:
        axs.axvline(x=xx_value, color='green', linestyle='-.') #, label=f'Vertical Line at x={x_value}')

    #Ajouter le n° de cluster + sa taille*
    pop_clust=Aggreg_principal_clust['df'][Cluster_name].value_counts().sort_index()
    x_text=[0]*n_clusters2

    for n_clus in range(n_clusters2):    
        if n_clus==0 :
            x_text[n_clus]=(NIP_Step * pop_clust[n_clus] )/ 2
        else:
            x_text[n_clus]= x_text[n_clus-1] + NIP_Step/2*(pop_clust[n_clus-1] + pop_clust[n_clus]) 
        y_text=(-200)
        axs.text(x_text[n_clus], y_text, 'Group-' + str(n_clus) + " - " + str(pop_clust[n_clus]) +"p", color='red', rotation=60)
        

    # Vous pouvez personnaliser la couleur, le style de ligne, et ajouter une légende
    axs.legend()

    #Enregistre le graph dans mlflow.
    plt.savefig(myouputpath + 'Parcours_clustered_shape.png')
    mlflow.log_artifact(myouputpath + 'Parcours_clustered_shape.png', 'Plots')
    #mlflow.log_artifact(myouputpath + 'Parcours_clustered_shape.png', "Carepath_Clustered")

    #plt.show()
    plt.close()
    #axs.flat[1].set_title('ratings_count boxplot')
    #sns.boxplot(data=df, x='ratings_count', ax=axs[1])

    return 

def plot_df_actes_twice_clustered(Df,Aggreg_principal_clust,Cluster1_name,Cluster2_name, Plot_dict,mlflow, myouputpath,mlflowname):
    #Recuperer une table acte avec les clusters
    #Requete="""SELECT Table_Acte.[NIP]
    #    ,Table_Cluster.***Nom du cluster***
    #    ,Table_Cluster.X_abscisse
    #    ,Table_Acte.[J_Parcours_V1]
    #    ,Table_Acte.[J_Parcours_V3]
    #    ,Table_Acte.[Service]
    #    ,Table_Acte.[Activite]
    #    ,Table_Acte.[Phase]
    #    ,Table_Acte.[Dimension]
    #    ,Table_Acte.[Type_seq]
    #FROM [ICO_Activite].[dbo].[Tmp_Carac_Actes] as Table_Acte
    #    , [ICO_Activite].[dbo].[***NOM de la table de clustering***] as Table_Cluster
    #
    #WHERE Table_Cluster.NIP = Table_Acte.NIP
    #        AND Table_Acte.[Phase]='Traitement'
    #ORDER BY Table_Cluster.Cluster asc ,Table_Acte.[J_Parcours_V1] desc, Table_Acte.[NIP]
    #"""
    #df_Actes_graph2=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,'No')


    #Function to plot a Dataset from df_Actes
    #Input_DAta: 
    # df_actes  a data frame with a specific shape get from an SQL query (In comment below)
    # plot_dict : a dict containing x and xx values (position of the vertical red and green lines)

    import matplotlib.pyplot as plt
    import seaborn as sns

    df_actes=Df
    x_values=Plot_dict['x_values']
    xx_values=Plot_dict['xx_values']

    NB_NIP=len(df_actes['NIP'].unique())

    #Cluster_name=Principal_clust_param['clust_name']
    n_clusters1=Aggreg_principal_clust[Cluster1_name].max()
    NIP_Step=100/NB_NIP

    #PLOT THE DATA
    fig, axs = plt.subplots(1, 1, figsize=(15, 8))
    axs.set_title('Carepathes')
    scatter=sns.scatterplot(data=df_actes, x=df_actes.X_abscisse, y='J_Parcours_V1',markers='Activite', hue='Service')

    # Changing X and Y axis labels
    scatter.set_xlabel('%')  # Change X axis label
    scatter.set_ylabel('J_Parcours_V1')  # Change Y axis label

    # Ajoutez la ligne verticale
    for x_value in x_values:
        axs.axvline(x=x_value, color='red', linestyle='--') #, label=f'Vertical Line at x={x_value}')

    # Ajoutez la ligne verticale
    for xx_value in xx_values:
        axs.axvline(x=xx_value, color='green', linestyle='-.') #, label=f'Vertical Line at x={x_value}')

    #Ajouter le n° de cluster + sa taille*
    pop_clust=Aggreg_principal_clust[Cluster1_name].value_counts().sort_index()
    x_text=[0]*(n_clusters1+1)

    for n_clus in range(n_clusters1+1):    
        if n_clus==0 :
            x_text[n_clus]=(NIP_Step * pop_clust[n_clus] )/ 2
        else:
            x_text[n_clus]= x_text[n_clus-1] + NIP_Step/2*(pop_clust[n_clus-1] + pop_clust[n_clus]) 
        y_text=(-200)
        axs.text(x_text[n_clus], y_text, 'Group-' + str(n_clus) + " - " + str(pop_clust[n_clus]) +"p", color='red', rotation=60)
        

    # Vous pouvez personnaliser la couleur, le style de ligne, et ajouter une légende
    axs.legend()

    #Enregistre le graph dans mlflow.
    plt.savefig(myouputpath + mlflowname + '.png')
    mlflow.log_artifact(myouputpath + mlflowname + '.png', 'Plots')
    #mlflow.log_artifact(myouputpath + 'Parcours_clustered_shape.png', "Carepath_Clustered")

    #plt.show()
    plt.close()
    #axs.flat[1].set_title('ratings_count boxplot')
    #sns.boxplot(data=df, x='ratings_count', ax=axs[1])

    return 


def plot_df_actes_one_clust(Df,Aggreg_principal_clust,Cluster_name, Plot_dict,mlflow, myouputpath,mlflowname):
    #Recuperer une table acte avec les clusters
    #Requete="""SELECT Table_Acte.[NIP]
    #    ,Table_Cluster.***Nom du cluster***
    #    ,Table_Cluster.X_abscisse
    #    ,Table_Acte.[J_Parcours_V1]
    #    ,Table_Acte.[J_Parcours_V3]
    #    ,Table_Acte.[Service]
    #    ,Table_Acte.[Activite]
    #    ,Table_Acte.[Phase]
    #    ,Table_Acte.[Dimension]
    #    ,Table_Acte.[Type_seq]
    #FROM [ICO_Activite].[dbo].[Tmp_Carac_Actes] as Table_Acte
    #    , [ICO_Activite].[dbo].[***NOM de la table de clustering***] as Table_Cluster
    #
    #WHERE Table_Cluster.NIP = Table_Acte.NIP
    #        AND Table_Acte.[Phase]='Traitement'
    #ORDER BY Table_Cluster.Cluster asc ,Table_Acte.[J_Parcours_V1] desc, Table_Acte.[NIP]
    #"""
    #df_Actes_graph2=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,'No')


    #Function to plot a Dataset from df_Actes
    #Input_DAta: 
    # df_actes  a data frame with a specific shape get from an SQL query (In comment below)
    # plot_dict : a dict containing x and xx values (position of the vertical red and green lines)

    import matplotlib.pyplot as plt
    import seaborn as sns

    df_actes=Df
    x_values=Plot_dict['x_values']
    NB_NIP=len(df_actes['NIP'].unique())

    #Cluster_name=Principal_clust_param['clust_name']
    n_clusters2=Aggreg_principal_clust[Cluster_name].max()
    NIP_Step=100/NB_NIP

    #PLOT THE DATA
    fig, axs = plt.subplots(1, 1, figsize=(15, 8))
    axs.set_title('Carepathes')
    scatter=sns.scatterplot(data=df_actes, x=df_actes.X_abscisse, y='J_Parcours_V1',markers='Activite', hue='Service')

    # Changing X and Y axis labels
    scatter.set_xlabel('%')  # Change X axis label
    scatter.set_ylabel('J_Parcours_V1')  # Change Y axis label

    # Ajoutez la ligne verticale
    for x_value in x_values:
        axs.axvline(x=x_value, color='red', linestyle='--') #, label=f'Vertical Line at x={x_value}')

    #Ajouter le n° de cluster + sa taille*
    pop_clust=Aggreg_principal_clust[Cluster_name].value_counts().sort_index()
    x_text=[0]*(n_clusters2+1)

    for n_clus in range(n_clusters2+1):    
        if n_clus==0 :
            x_text[n_clus]=(NIP_Step * pop_clust[n_clus] )/ 2
        else:
            x_text[n_clus]= x_text[n_clus-1] + NIP_Step/2*(pop_clust[n_clus-1] + pop_clust[n_clus]) 
        y_text=(-200)
        axs.text(x_text[n_clus], y_text, 'Group-' + str(n_clus) + " - " + str(pop_clust[n_clus]) +"p", color='red', rotation=60)
        

    # Vous pouvez personnaliser la couleur, le style de ligne, et ajouter une légende
    axs.legend()

    #Enregistre le graph dans mlflow.
    plt.savefig(myouputpath + mlflowname + '.png')
    mlflow.log_artifact(myouputpath + mlflowname + '.png', 'Plots')
    #mlflow.log_artifact(myouputpath + 'Parcours_clustered_shape.png', "Carepath_Clustered")

    #plt.show()
    plt.close()
    #axs.flat[1].set_title('ratings_count boxplot')
    #sns.boxplot(data=df, x='ratings_count', ax=axs[1])

    return 


def Prepare_Save_Plot_one_clust(df_Actes_graph,df_Parcours_clust,clustName, My_order,myouputpath,Table_name,Requete,Filter_df_col,Filter_df_value,mlflow,mlflowname):
    import Sql_Alchemy_Classes as AlSQL
    import my_custom_func_batch_follow as Mcfbf
    import pandas as pd

    #PREPARE THE DATASET TO BE PLOTED
    Parcours_DF=Prepare_Plot_carepath_one_cluster(df_Actes_graph,df_Parcours_clust,clustName, My_order)
    Abcisses_DF, Plot_dict =Compute_abcisses_One_cluster(Parcours_DF,clustName)
    print('CPP - Computing Abcisses OK')

    #SAVE THE CLUSTERING + PLOTTING VALUES TO THE DATABASE
    Save_Cluster_and_Carepath_to_Database_one_clust(Abcisses_DF,clustName,myouputpath,Table_name)
    print(Mcfbf.myprint('CPP - Saving Abcisses to BDD OK', 1, 1))

    #GET A DATASET OF ACTES
    df_Actes_graph2=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,'No')
    #FILTER THE DATASET IN ORDER NOT TO SHOW ALL ACTES
    filtered_df = df_Actes_graph2[df_Actes_graph2[Filter_df_col] == Filter_df_value]
    nip_no_treatment_info = df_Actes_graph2[~df_Actes_graph2['NIP'].isin(filtered_df['NIP'])]
    nip_no_treatment_info = nip_no_treatment_info[['NIP', 'Clust', 'X_abscisse']].drop_duplicates()
    final_df = pd.concat([filtered_df, nip_no_treatment_info], ignore_index=True)
    final_df_sorted = final_df.sort_values(by='X_abscisse')
    #PLOT AND SAVE IN MLFLOW

    plot_df_actes_one_clust(final_df_sorted,Parcours_DF,clustName,Plot_dict,mlflow,myouputpath,mlflowname)
    #Mcfcp.plot_df_actes_one_clust(final_df_sorted,Aggreg_Parcours_clust,Parcours_Clust_parameters,Aggreg_Time_clust,Time_Clust_parameters, Plot_dict, mlflow, myouputpath)
    print(Mcfbf.myprint('CPP - PLOTING AND SAVE TO MLFLOW OK', 1, 1))
    
    return


def Prepare_Save_Plot_twice_clustered(df_Actes_graph,cluster1_Table,cluster2_Table,CPP_Param,Requete,myouputpath,mlflow,mlflowname):
    import Sql_Alchemy_Classes as AlSQL
    import my_custom_func_batch_follow as Mcfbf
    import pandas as pd

    Parcours_DF=Prepare_Plot_carepath_twice_clustered(df_Actes_graph,cluster1_Table,cluster2_Table,CPP_Param)
    Abcisses_DF, Plot_dict =Compute_abcisses_two_clusters(Parcours_DF,CPP_Param)
    print('CPP - Computing Abcisses OK')

    #SAVE THE CLUSTERING + PLOTTING VALUES TO THE DATABASE
    
    Save_Cluster_and_Carepath_to_Database_twice_clustered(Abcisses_DF,CPP_Param['primary_clust_name'],CPP_Param['sub_clust_name'],myouputpath,CPP_Param['CPP_Table_Name'])
    print(Mcfbf.myprint('CPP - Saving Abcisses to BDD OK', 1, 1))
    
    #FILTER THE DATASET IN ORDER NOT TO SHOW ALL ACTES
    df_Actes_graph2=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,'No')
    filtered_df = df_Actes_graph2[df_Actes_graph2[CPP_Param['CPP_Filter_df_col']] == CPP_Param['CPP_Filter_df_value']]
    nip_no_treatment_info = df_Actes_graph2[~df_Actes_graph2['NIP'].isin(filtered_df['NIP'])]
    nip_no_treatment_info = nip_no_treatment_info[['NIP', 'X_abscisse']].drop_duplicates()
    #nip_no_treatment_info = nip_no_treatment_info[['NIP', 'Clust1','Clust2', 'X_abscisse']].drop_duplicates()
    final_df = pd.concat([filtered_df, nip_no_treatment_info], ignore_index=True)
    final_df_sorted = final_df.sort_values(by='X_abscisse')

    #PLOT AND SAVE IN MLFLOW
    plot_df_actes_twice_clustered(final_df_sorted,Parcours_DF,CPP_Param['primary_clust_name'],CPP_Param['sub_clust_name'],Plot_dict,mlflow,myouputpath,mlflowname)
    #Mcfcp.plot_df_actes_one_clust(final_df_sorted,Aggreg_Parcours_clust,Parcours_Clust_parameters,Aggreg_Time_clust,Time_Clust_parameters, Plot_dict, mlflow, myouputpath)
    print(Mcfbf.myprint('CPP - PLOTING AND SAVE TO MLFLOW OK', 1, 1))

    return