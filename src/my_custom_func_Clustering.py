def optimal_nb_cluster(inertia,threshold,max_clusters):
    optimal_nb_cluster = None  # Initialiser à None pour indiquer qu'aucune valeur optimale n'est encore trouvée
    for i in range(1, max_clusters):  # Commencer à 1 car la différence est calculée entre i et i-1
        if abs(inertia[i] - inertia[i - 1]) / inertia[0] < threshold:
            optimal_nb_cluster = i
            break  # Sortir de la boucle lorsque la condition est satisfaite*
    return optimal_nb_cluster


def Automatic_nb_cluster(X_scaled,Method, max_clusters,threshold,ouput=None,mlflow=None,mlflow_output=None):
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

    ouput_dict={'ncluster' : n_clusters,
                'df_dist' : df_dist}

    return ouput_dict