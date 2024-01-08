# IMPORT SECTION



# FUNCTION DECLARATION 

#INPUT PARAMETERS
    # DATAFRAME WITH SPECIFIC SHAPE 
#OUTPUT PARAMETERS
    # GRAPHICAL OUTPUT 
    # WARNINGS/ERRORS 



import pandas as pd
from sklearn.preprocessing import LabelEncoder, OneHotEncoder

##### STEP 01
#ONE HOT ENCODING 'MY_SERVICE'
my_service_column = df_time_seq['My_Service']
label_encoder = LabelEncoder()
my_service_encoded = label_encoder.fit_transform(my_service_column)

onehot_encoder = OneHotEncoder(sparse=False)
my_service_onehot = onehot_encoder.fit_transform(my_service_encoded.reshape(-1, 1))

# Créez un DataFrame à partir de la matrice One-Hot
my_service_df = pd.DataFrame(my_service_onehot, columns=[f'My_Service_{label}' for label in label_encoder.classes_])

# Concaténez le DataFrame One-Hot avec le DataFrame d'origine
df_time_seq = pd.concat([df_time_seq, my_service_df], axis=1)

# Supprimez la colonne d'origine 'My_Service' si nécessaire
df_time_seq.drop('My_Service', axis=1, inplace=True)


##### STEP 02
#ONE HOT ENCODING 'Type_seq'
my_service_column = df_time_seq['Type_seq']
label_encoder = LabelEncoder()
my_service_encoded = label_encoder.fit_transform(my_service_column)

onehot_encoder = OneHotEncoder(sparse=False)
my_service_onehot = onehot_encoder.fit_transform(my_service_encoded.reshape(-1, 1))

# Créez un DataFrame à partir de la matrice One-Hot
my_service_df = pd.DataFrame(my_service_onehot, columns=[f'Type_seq_{label}' for label in label_encoder.classes_])

# Concaténez le DataFrame One-Hot avec le DataFrame d'origine
df_time_seq = pd.concat([df_time_seq, my_service_df], axis=1)

# Supprimez la colonne d'origine 'My_Service' si nécessaire
df_time_seq.drop('Type_seq', axis=1, inplace=True)

# Affichez le DataFrame résultant
df_time_seq.drop([	'Nb_sejours',	'Poids_Sej', 'UFX_CL', 'Activite'], axis=1, inplace=True)

df_time_seq



##### STEP 03 

#To be tested on Actes / Sejours / Sequences Time series
#This libaray doesn't work with categorical values -> Modify Service + Activité in OneHotEncoding (0/1) to be passed into this library !

df_time_feature=tsfresh.extract_features(df_time_seq,column_id='NIP',column_sort='JP_V1',column_kind=None, column_value=None)



##### STEP 04 
#CHECKING IF SOME ROWS OR COLMUNS CONTAINS Nan or inf values + Supress them

rows_with_infinity = df_time_feature.index[df_time_feature.isin([np.inf, -np.inf]).any(1)]
columns_with_infinity = df_time_feature.columns[df_time_feature.isin([np.inf, -np.inf]).any()]

rows_with_Nan = df_time_feature.index[df_time_feature.isna().any(1)]
columns_with_Nan = df_time_feature.columns[df_time_feature.isna().any()]

#print("Colonnes avec des valeurs infinies :")
#print(columns_with_infinity)
#print("Colonnes avec des valeurs Nan :")
#print(columns_with_Nan)
print("Nb de lignes avec des valeurs infinies : " + str(len(rows_with_infinity)))
print("Nb de colonnes avec des valeurs infinies : " + str(len(columns_with_infinity)))

print("Nb de lignes avec des valeurs NaN : " + str(len(rows_with_Nan)))
print("Nb de colonnes avec des valeurs  NaN " + str(len(columns_with_Nan)))

# Combine columns with infinity and NaN
columns_to_remove = list(set(columns_with_infinity) | set(columns_with_Nan))

# Remove the identified columns from the DataFrame
df_time_feature = df_time_feature.drop(columns=columns_with_Nan)


## ADD OUPUT OF WARNINGS HERE !!!!

#df_time_feature.loc[rows_with_infinity,:]
#df = df_time_feature.drop(rows_with_infinity)


##### STEP 05 
#Try a clustering on this table to create different categories of patients based on their Radiotherapie Treatment.

# Standardiser les données
scaler = StandardScaler()
X_scaled = scaler.fit_transform(df_time_feature)


##### STEP 06
#APPLY CLUSTERING AGGLOMERATIVE

from sklearn.cluster import AgglomerativeClustering

n_clusters = 20  # Choose the number of clusters you want
agglomerative = AgglomerativeClustering(n_clusters=n_clusters)
labels = agglomerative.fit_predict(X_scaled)

df_time_feature['Cluster'] = labels
unique_clusters = df_time_feature['Cluster'].nunique()

# The 'Cluster' column in df_time_feature now contains the cluster labels assigned by agglomerative clustering.
# The variable unique_clusters will give you the number of unique clusters found.


#### STEP 07
#PREPARE OUTPUT DATAS

# Calculer le nombre de NIP dans chaque cluster
df_X_values = df_time_feature1['Cluster'].value_counts()
Nb_NIP=df_time_feature1['NIP'].nunique()

# Ajouter une colonne "X_abscisse" au DataFrame Parcours_Encoded_Total pour stocker les abscisses calculées
df_time_feature1['X_abscisse'] = None

df_time_feature1.sort_values(by=['Cluster','NIP'],inplace=True)
df_time_feature1



#### STEP 08
#COMPUTE ABCISSES VALUE FOR THE GRAPHIC

#First STEP
old_NIP=''
old_Cluster=-1
x_value=[]
x=0
NIP_Step = 100/Nb_NIP

for index,row in df_time_feature1.iterrows():

    if old_Cluster!=row['Cluster']:
        x += NIP_Step
        x_value.append(x)
        df_time_feature1.at[index,'X_abscisse']=x

    elif old_NIP!=row['NIP']:
        x += NIP_Step
        df_time_feature1.at[index,'X_abscisse']=x
    
    df_time_feature1.at[index,'X_abscisse']=x
    old_NIP=row['NIP']
    old_Cluster=row['Cluster']

# Afficher le DataFrame avec les abscisses calculées
print(df_time_feature1[['NIP','Cluster', 'X_abscisse']])


#### STEP 09
#Sauvegarder dans la BDD l'association NIP - Cluster
Table_Cluster='Tmp_NIP_Cluster' 
Requete = 'EXECUTE dbo.Delete_Table_if_exists ' + Table_Cluster
with AlSQL.engine.begin() as conn:
            conn.execute(sqlalchemy.text(Requete))

df_time_feature1[['NIP','Cluster','X_abscisse']].to_sql(Table_Cluster,AlSQL.engine)

#### STEP 10
#Recuperer une table acte avec les clusters pour l'affichage
Requete="""SELECT Table_Acte.[NIP]
	  ,Table_Cluster.Cluster
	  ,Table_Cluster.X_abscisse
      ,Table_Acte.[J_Parcours_V1]
      ,Table_Acte.[J_Parcours_V3]
      ,Table_Acte.[Service]
      ,Table_Acte.[Activite]
      ,Table_Acte.[Phase]
      ,Table_Acte.[Dimension]
      ,Table_Acte.[Type_seq]
  FROM [ICO_Activite].[dbo].[Tmp_Carac_Actes] as Table_Acte
	 , [ICO_Activite].[dbo].[Tmp_NIP_Cluster] as Table_Cluster
  
  WHERE Table_Cluster.NIP = Table_Acte.NIP
        AND Table_Acte.[Phase]='Traitement'
        --AND Table_Acte.[Service]='Imagerie'
"""
df_Actes_graph=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,'No')

#### STEP 11
#CREATION D'UNE TABLE QUI RESUME LE RESULTAT DU CLUSTERING EN TERME DE POPULATION ET % DU DATASET

#Recuperer une table récapitulative des actes / Population de NIP avec les clusters
Requete="""SELECT 
	MIN(Table_NB_actes_cluster.Cluster) as Cluster
	,SUM(Table_NB_actes_cluster.Nb_Actes) as Nb_Actes
	,COUNT(Table_NB_actes_cluster.NIP) as Nb_NIP
FROM 
			(SELECT 
				   COUNT(Table_Acte.[NIP]) as Nb_Actes
				   ,Table_Cluster.[NIP] as NIP
				  ,MAX(Table_Cluster.Cluster) as Cluster
				  --,Table_Cluster.X_abscisse
				  --,Table_Acte.[J_Parcours_V1]
				  --,Table_Acte.[J_Parcours_V3]
				  --,Table_Acte.[Service]
				  --,Table_Acte.[Activite]
				  --,Table_Acte.[Phase]
				  --,Table_Acte.[Dimension]
				  --,Table_Acte.[Type_seq]
			  FROM [ICO_Activite].[dbo].[Tmp_Carac_Actes] as Table_Acte
				 , [ICO_Activite].[dbo].[Tmp_NIP_Cluster] as Table_Cluster
  
			  WHERE Table_Cluster.NIP = Table_Acte.NIP
					--AND Table_Acte.[Type_seq]='TRAIT'
                              
			  GROUP BY Table_Cluster.NIP
			  ) as Table_NB_actes_cluster
  GROUP BY Cluster
  ORDER BY Cluster
"""
df_Cluster=AlSQL.AlSQL_Requete(AlSQL.engine,Requete,'No')
df_Cluster

#### STEP 12
#CREATION DU GRAPHIQUE

fig, axs = plt.subplots(1, 1, figsize=(15, 6))
data_graph=df_Actes_graph
axs.set_title('Carepathes')
sns.scatterplot(data=df_Actes_graph, x=df_Actes_graph.X_abscisse, y='J_Parcours_V1',markers='Activite', hue='Service')

# Ajoutez la ligne horizontale
for x_value in x_value:
    axs.axvline(x=x_value, color='red', linestyle='--') #, label=f'Vertical Line at x={x_value}')

# Vous pouvez personnaliser la couleur, le style de ligne, et ajouter une légende
axs.legend()
plt.show()

#axs.flat[1].set_title('ratings_count boxplot')
#sns.boxplot(data=df, x='ratings_count', ax=axs[1])




