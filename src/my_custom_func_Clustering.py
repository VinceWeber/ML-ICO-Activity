def optimal_nb_cluster(inertia,threshold,max_clusters):
    #design to return eht optimal nb of cluster
    #by reading the inertia value and stop when the threshold as been reached.

    optimal_nb_cluster = None  # Initialiser à None pour indiquer qu'aucune valeur optimale n'est encore trouvée
    for i in range(1, max_clusters):  # Commencer à 1 car la différence est calculée entre i et i-1
        if abs(inertia[i] - inertia[i - 1]) / inertia[0] < threshold:
            optimal_nb_cluster = i
            break  # Sortir de la boucle lorsque la condition est satisfaite*
    return optimal_nb_cluster


def Automatic_nb_cluster(X_scaled,Method, max_clusters,threshold,ouput=None,mlflow=None,mlflow_output=None):
    # Design to perform multiple clustering and return a nb of clusters :
    # a matrix (Xscaled)
    # a method (KMeans, Agglomerative, GMM, or....)
    # a maxi number of cluster 
    # a threshold to stop the loop of finding the right nb of cluster
    # output : Boolean show or not show the plot during the execution
    # mlfow : mlflow module (if=None , not loaded), no log of the parameters
    # mlflowoutput : dictionnary with the parameters to be stored in the study    

    import matplotlib.pyplot as plt
    
    #APPLY CLUSTERING
    if Method=="KMeans" :
        from sklearn.cluster import KMeans
        # Liste pour stocker les valeurs de l'inertie
        inertia = []

        for n_clusters in range(1, max_clusters + 1):
            kmeans = KMeans(n_clusters=n_clusters)
            kmeans.fit(X_scaled)
            inertia.append(kmeans.inertia_)

    elif Method=="AgglomerativeClustering":
        print('No method implemented !!')

    nb_clusters = optimal_nb_cluster(inertia,threshold,max_clusters)

    curve_filename=mlflow_output['curve_filename']
    curve_mlflowname=mlflow_output['curve_mlflowname']


    # Tracer le graphique de l'inertie en fonction du nombre de clusters
    plt.figure(figsize=(10, 6))
    plt.plot(range(1, max_clusters + 1), inertia, marker='o')
    plt.xlabel('Nombre de clusters')
    plt.ylabel('Inertie')
    plt.title('Méthode du coude pour déterminer le nombre optimal de clusters')
    
    if mlflow!=None:
        plt.savefig(curve_filename)
        #MLFLOW LOG
        #Curve of nb of cluster
        mlflow.log_artifact(curve_filename, curve_mlflowname)
        
    #if ouput:
        #plt.show()
    
    return nb_clusters


def Do_Clustering(X_scaled,Method,nb_clusters,ouput=None,mlflow=None,mlflow_output=None):
    # Design to perform a clustering from :
    # a matrix (Xscaled)
    # a method (KMeans, Agglomerative, GMM, or....)
    # a number of cluster (if n_cluster=None, get an automatic choice of nb_cluster)
    # output : Boolean show or not show the plot during the execution
    # mlfow : mlflow module (if=None , not loaded), no log of the parameters
    # mlflowoutput : dictionnary with the parameters to be stored in the study

    import matplotlib.pyplot as plt
    import pandas as pd
    
    #Initialisation de la variable de sortie df_tobemerged_to_aggreg
    col_name= mlflow_output['clust_name']
    #data= {col_name : []}
    #df_tobemerged_to_aggreg = pd.DataFrame(data)

    #APPLY CLUSTERING
    if Method=="KMeans" :
        from sklearn.cluster import KMeans
        clust = KMeans(n_clusters=nb_clusters)
    elif Method=="AgglomerativeClustering":
        from sklearn.cluster import AgglomerativeClustering
        clust = AgglomerativeClustering(n_clusters=nb_clusters, linkage='ward')

    #PREDICT THE CLUSTER ON THE DATASET
    labels = clust.fit_predict(X_scaled)

                    #IN CASE OF MULTIDIMENSIONNAL ANALYSYS (n lines for One NIP)
                    #Labels_duplicated=[]

                    #ADAPT THE LABEL SIZE IF MULTIDIMENSIONNAL TS CLUSTERING
                    #for i in range(len(labels)):
                    #    for k in range(int(Aggreg_Patients['Nb_dim'])):
                    #        Labels_duplicated.append(labels[i])
                        
                    #Enregistre la liste de cluster en vue de merger avec le dataset initial
                    #df_tobemerged_to_aggreg[col_name]=pd.DataFrame(Labels_duplicated)

    #Create a dataframe describing the clustering
    df_dist_matrix = pd.DataFrame(X_scaled)
    df_dist_matrix[col_name] = labels
    df_dist_matrix[col_name + '_NIP'] = 1
    df_dist_matrix[col_name + '_Mean_Indiv'] = False

    for cluster in range(nb_clusters):
        # Extract a part of the dist matrix
        cluster_subset = df_dist_matrix[df_dist_matrix[col_name] == cluster]

        # Compute the mean distance for each row in the cluster and store it in the 'Mean_dist' column
        df_dist_matrix.loc[df_dist_matrix[col_name] == cluster, col_name + '_Mean_dist'] = cluster_subset.drop([col_name, col_name + '_Mean_Indiv'], axis=1).mean(axis=1)

        # Calculate the mean of the distances within the cluster
        mean_distance = df_dist_matrix[df_dist_matrix[col_name] == cluster][col_name + '_Mean_dist'].mean()

        # Find the index of the row closest to the mean distance
        mean_row_mean_index = df_dist_matrix[df_dist_matrix[col_name] == cluster][col_name + '_Mean_dist'].sub(mean_distance).abs().idxmin()

        # Update 'Mean_Indiv' to True for the row with the minimum mean distance
        df_dist_matrix.at[mean_row_mean_index, col_name + '_Mean_Indiv'] = True


    # Creating the Clustering_summary DataFrame
    # Group by 'Cluster' and calculate count of 'NIP' and variance of 'Mean_dist'
    summary_data = df_dist_matrix.groupby(col_name).agg({col_name + '_NIP': 'count', col_name + '_Mean_dist': 'var'}).reset_index()
    summary_data.columns = [col_name, col_name + '_NIP_Count', col_name + '_Mean_dist_Variance']
    
    #remove 'col_name + '_NIP' column from df_dist_matrix
    df_dist_matrix.drop(columns=[col_name + '_NIP'],inplace=True)

    # Creating the Clustering_summary DataFrame
    Clustering_summary = summary_data.copy()

    #labels   # Example cluster labels, replace this with your actual cluster labels
    # Assuming dist_matrix is your 50x50 matrix
    # Perform PCA
    from sklearn.decomposition import PCA
    pca = PCA(n_components=2)  # Reduce to 2 dimensions
    pca_result = pca.fit_transform(X_scaled)

    indices_to_highlight=df_dist_matrix.loc[df_dist_matrix[col_name + '_Mean_Indiv'] == True].index

    # Plotting the PCA result with cluster colors
    plt.figure(figsize=(8, 6))
    plt.scatter(pca_result[:, 0], pca_result[:, 1], c=labels, cmap='viridis')
    plt.scatter(pca_result[indices_to_highlight, 0], 
                pca_result[indices_to_highlight, 1], 
                c='red', label='Highlighted Points')

    plt.title('PCA Result: dist_matrix matrix reduced to 2 dimensions')
    plt.xlabel('Principal Component 1')
    plt.ylabel('Principal Component 2')
    plt.colorbar(label='Cluster')
    #plt.savefig(myouputpath + 'Clust_2_shape.png')
    
    PCA_filename=mlflow_output['PCA_filename']
    PCA_mlflowname=mlflow_output['PCA_mlflowname']

    if mlflow!=None:
        plt.savefig(PCA_filename)
        #MLFLOW LOG
        #Curve of nb of cluster
        mlflow.log_artifact(PCA_filename, PCA_mlflowname)
        
    #if ouput:
    #    plt.show()

    return df_dist_matrix, Clustering_summary

def my_clust_func(X_scaled,Method,n_clusters,max_clusters,threshold,ouput,mlflow,mlflow_output,List_NIP):
    # Design to perform a clustering from :
    # a matrix (Xscaled)
    # a method (KMeans, Agglomerative, GMM, or....)
    # a number of cluster (if n_cluster=None, get an automatic choice of nb_cluster)
    # If Automatic choice of cluster: 
    #       a maximum of cluster 
    #       a threshold to stop the loop of finding the right nb of cluster
    # output : Boolean show or not show the plot during the execution
    # mlfow : mlflow module (if=None , not loaded), no log of the parameters
    # mlflowoutput : dictionnary with the parameters to be stored in the study
    # List NIP : A list of NIP in the same order with Xscaled (1line of Xcaled is linked with the 1st line of ListNIP) 
    
    import pandas as pd

    Cluster_summary_filename=mlflow_output['Summary_filename']
    Cluster_summary_mlflowname=mlflow_output['Summary_mlflowname']

    NIP_Carac_filename=mlflow_output['NIP_Carac_filename']
    NIP_Carac_mlflowname=mlflow_output['NIP_Carac_mlflowname']

    col_name= mlflow_output['clust_name']

    if n_clusters==None:
        n_clusters=Automatic_nb_cluster(X_scaled,Method, max_clusters,threshold,ouput,mlflow,mlflow_output)
        mlflow.log_metrics({'Optimal_nb_' + col_name : n_clusters})

    #nb of clusters
    mlflow.log_params({'NB_ ' + col_name : n_clusters})

    df_dist,Cl_sum=Do_Clustering(X_scaled,Method,n_clusters,ouput,mlflow,mlflow_output)
    NIP_Carac=pd.concat([List_NIP,df_dist],axis=1)
    NIP_Carac=NIP_Carac.loc[NIP_Carac[col_name + '_Mean_Indiv'] == True, ['NIP', col_name]]

    if mlflow!=None:
        Cl_sum.to_csv(Cluster_summary_filename)
        mlflow.log_artifact(Cluster_summary_filename,Cluster_summary_mlflowname)
        
        NIP_Carac.to_csv(NIP_Carac_filename)
        mlflow.log_artifact(NIP_Carac_filename,NIP_Carac_mlflowname)

    #add the NIP to 'df_dist' and set it as index
    df_out=pd.concat([df_dist,List_NIP], axis=1)
    
    ouput_dict={'ncluster' : n_clusters,
                'df_dist' : df_out}

    return ouput_dict

def prepare_clust_DDA(Create_dataset_parameters,DSprefix, List_NIP):
    #Function qui généère un dataset 'DDA clust' en vue d'une clusterisation

    #import variables : create dataset_parameters, list_NIP provenant de la fonction d'aggregation SQL
    #ouput variable : DDA clust (3 colonnes : NIP / DDA Date dernier acte / DPA Date Premier acte)
    
    # AJOUTER DES VARIABLES D'INTERET DU PATIENT
    #Date de la dernière activitée ?

    import pandas as pd
    from datetime import datetime
    import Sql_Alchemy_Classes as AlSQL

    date_format = '%m-%d-%Y %H:%M:%S'

    DPA_ref=datetime.strptime(Create_dataset_parameters[DSprefix + 'Start_Window_time'], date_format)
    DDA_ref=datetime.strptime(Create_dataset_parameters[DSprefix + 'End_Window_time'], date_format)

    DPA_ref = pd.to_datetime(DPA_ref)
    DPA_ref = pd.to_datetime(DPA_ref)


    #Calcul des distances entre les bornes de fenêtre temporelle et premier et dernier acte
    Requete="""SELECT [NIP]
        ,MIN([DD_A]) DPA_NIP
        ,MAX([DF_A]) DDA_NIP
    FROM [ICO_Activite].[dbo].[Tmp_Carac_Actes]
    GROUP BY NIP
    """
    CP_Bounds_NIP=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,'No')

    CP_Bounds_NIP['DPA_NIP'] = pd.to_datetime(CP_Bounds_NIP['DPA_NIP'])
    CP_Bounds_NIP['DDA_NIP'] = pd.to_datetime(CP_Bounds_NIP['DDA_NIP'])

    CP_Bounds_NIP.DPA_NIP=(CP_Bounds_NIP['DPA_NIP'] - DPA_ref).dt.days
    CP_Bounds_NIP.DDA_NIP=( DDA_ref - CP_Bounds_NIP['DDA_NIP']).dt.days

    #integration au dataset Aggregpatient ?
    Temp_df = pd.merge(List_NIP,CP_Bounds_NIP, on='NIP')

    #Création d'une matrice concaténée entre dist_matrix et CP_bounds
    agg_func = { 
        'DPA_NIP': 'min',
        'DDA_NIP': 'min'
    }
    #print(Aggreg_Patients2)
    DDA_Clust=Temp_df[['NIP','DPA_NIP','DDA_NIP']].groupby('NIP').agg(agg_func)
    print(DDA_Clust[['DPA_NIP','DDA_NIP']])
    
    return DDA_Clust




def df_avg_individual(My_Aggreg, My_clust_result, mlflow_param):
    import pandas as pd
    import numpy as np
    
    Clust_name=mlflow_param['clust_name']
    Mydf0=pd.merge(My_Aggreg,My_clust_result['df_dist'][['NIP',Clust_name]], on='NIP' )

    My_avg_NIP_df=pd.DataFrame()

    for clust in range(My_clust_result['ncluster']):
        Mydf=Mydf0.loc[Mydf0[Clust_name]==clust, :]

        #Lister les types de colonnes du dataframe
        column_types = Mydf.dtypes

        # Séparer les colonnes numériques et textuelles
        numeric_columns = [col for col, dtype in column_types.items() if np.issubdtype(dtype, np.number)]
        text_columns = [col for col, dtype in column_types.items() if dtype == 'object']
        bool_columns = [col for col, dtype in column_types.items() if dtype == bool]

        # Appliquer différentes fonctions d'agrégation aux colonnes
        aggregations = {col: 'mean' for col in numeric_columns}
        aggregations.update({col: 'max' for col in text_columns})
        aggregations.update({col: 'any' for col in bool_columns})

        # Appliquer les agrégations à chaque colonne
        result = Mydf.agg(aggregations)
        result['NIP']='Avg_' + Clust_name + "_" + str(clust)

        My_avg_NIP_df = My_avg_NIP_df.append(result, ignore_index=True)

    return My_avg_NIP_df

def clustering_result_to_df(My_Aggreg_dict_df,My_Clust_result,My_Clust_parameter ):
    #Function to store result of the clustering

    #Input :
    #My_Aggreg_df : a dict of aggregation, composed of : nb_of dim and 'df' : a dataset with columns NIP, (timestep) x columns of time aggregation , FT, FV (filters from aggreg)
    #My_Clust_result : a dict of the result of a clustering given by the function my_clust_func
    #My_Clust_parameter : a dict of the parameters of the clustering

    #output: 
    # dataset = My_Aggreg_df + My_Clust_result

    import pandas as pd

    My_Clust_result['Avg_Indiv']=df_avg_individual(My_Aggreg_dict_df['df'], My_Clust_result, My_Clust_parameter)
    Aggreg_Patients_TC=My_Aggreg_dict_df
    clust_name=My_Clust_parameter['clust_name']
    subset_cols = My_Clust_result['df_dist'][['NIP',clust_name,clust_name +'_Mean_Indiv']]
    my_merged_df = pd.merge(My_Aggreg_dict_df['df'], subset_cols, on='NIP')
    Parcours_Aggreg=pd.concat([my_merged_df,My_Clust_result['Avg_Indiv']], axis=0)

    Aggreg_Patients_TC['df']=Parcours_Aggreg

    return Aggreg_Patients_TC

def cluster(My_Aggreg_dict_df,Data_to_be_clustered,ouput,mlflow,My_Clust_parameter):
    #perform a clustering and update the initial dataset (dict) with the result

    #Input = 
    # Data_to_be_clustered
    # Method of clustering
    # Maximum nb of cluster
    # Threshold
    # output : yes/no
    # mflow
    # My_Clust_parameter
    # List NIP 
    
    from sklearn.preprocessing import StandardScaler

    List_NIP=My_Aggreg_dict_df['df']['NIP']
    Myaggreg_copy=My_Aggreg_dict_df.copy()
    
    Method=My_Clust_parameter['Method']
    Nb_clust=My_Clust_parameter['Nb_clusters']
    Max_clusters=My_Clust_parameter['max_nb_clusters']
    threshold=My_Clust_parameter['threshold']

    # Standardiser les données
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(Data_to_be_clustered)

    #Perform the clustering with calling my_clust_func
    TC_Result=my_clust_func(X_scaled,Method,Nb_clust, Max_clusters,threshold,ouput,mlflow,My_Clust_parameter,List_NIP)
    
    out_dict=clustering_result_to_df(Myaggreg_copy,TC_Result,My_Clust_parameter )
    out_dict['Nb_clusters']=TC_Result['ncluster']
    out_dict['df_dist']=TC_Result['df_dist']

    return out_dict