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
    PCA_filename=mlflow_output['PCA_filename']
    PCA_mlflowname=mlflow_output['PCA_mlflowname']

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
        #PLot of the clustering 
        mlflow.log_artifact(PCA_filename, PCA_mlflowname)
        
    if ouput!=None:
        plt.show()
    
    return nb_clusters